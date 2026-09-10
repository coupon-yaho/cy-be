// 대상 목록 페이징이 뒤로 갈수록 싸지는지 — 커서가 안쪽에 있는지를 잽니다.
package com.kafkick.storage.db.verification;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.FindingType;
import com.kafkick.core.verification.ResidualCursor;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.VerificationFindingRepository;
import com.kafkick.core.verification.VerificationRun;
import com.kafkick.storage.db.RepositoryTest;

/**
 * <b>커서가 파생 테이블 <i>안쪽</i>에 있는지를 동작으로 잡는다.</b>
 *
 * <p>이 질의는 {@code GROUP BY} 가 집합을 다 만든 뒤에 자르므로 <b>{@code LIMIT} 이
 * 시간을 거의 안 줄인다</b>(12만 키 형상에서 100건 220ms · 전량 270ms). 그래서 커서를
 * 바깥 {@code WHERE} 에 붙이면 페이지마다 전체 비용을 다시 낸다 — 90페이지면 20초다.
 *
 * <p><b>그런데 결과는 똑같다.</b> 안쪽이든 바깥이든 같은 줄이 같은 순서로 나오므로
 * 정확성 시험은 하나도 안 깨진다. <b>조용히 느려지는 변경</b>이고, 그것을 잡는 자리가
 * 여기다.
 *
 * <p>재는 축은 <b>스토리지 엔진 읽기 호출</b>({@code Handler_read_*})이다.
 * {@code EXPLAIN} 의 {@code rows} 는 추정치고, 벽시계는 러너를 탄다.
 * 형제 {@code ResidualQueryCostProbe} 가 같은 기준을 쓴다.
 *
 * <h2>트랜잭션 안에서 잰다</h2>
 *
 * <p>{@code Handler_read_*} 는 <b>세션 상태</b>다. 트랜잭션이 없으면 스프링이 커넥션을
 * 스레드에 안 묶어 호출마다 풀이 다른 세션을 줄 수 있고, 그러면 전후 차이가 뜻을 잃는다.
 * {@code @DataJpaTest} 의 기본 트랜잭션이 그것을 묶어 준다 — 그 이유는 형제 프로브
 * javadoc 에 길게 적혀 있다.
 */
@RepositoryTest
@Import({VerificationFindingJdbcAdapter.class, VerificationRunJdbcAdapter.class})
class ResidualTargetCostProbe {

    /** 규칙 6종 × 이 수 × 두 실행. 형제 프로브와 같은 상한이다. */
    private static final int PER_RULE = 10_000;

    private static final int PAGE = 1_000;

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 8, 15, 14, 0);

    @Autowired VerificationFindingJdbcAdapter findings;
    @Autowired VerificationRunJdbcAdapter runs;
    @Autowired JdbcClient jdbcClient;

    /**
     * <b>뒤 페이지가 앞 페이지보다 눈에 띄게 싸야 한다.</b>
     *
     * <p>커서를 바깥으로 옮기면 두 값이 <b>같아진다</b> — 집합을 매번 통째로 만들기
     * 때문이다. 그래서 비율로 건다. 절대값은 안 건다(러너·버퍼풀에 따라 흔들린다).
     *
     * <p>실측으로 시간이 237ms → 19ms 였다. 여유를 크게 두고 <b>절반</b>으로 잡는다 —
     * 잡으려는 것은 "바깥으로 옮겼다"(비율 1)이지 몇 퍼센트가 아니다.
     */
    @Test
    @DisplayName("커서가 뒤로 갈수록 읽는 행이 줄어든다 — 커서가 파생 테이블 안쪽에 있다")
    void laterPagesReadFewerRowsBecauseTheCursorIsPushedIntoTheDerivedTable() {
        long before = newRun(1);
        long after = newRun(2);
        seed(before, after);

        long first = readsFor(before, after, null);
        // 마지막 유형의 끝 — 남은 대상이 거의 없는 자리다.
        ResidualCursor late = new ResidualCursor(lastType(), "￿");
        long tail = readsFor(before, after, late);

        assertThat(tail)
                .as("첫 페이지 %d · 끝 커서 %d — 값이 비슷하면 커서가 파생 테이블 "
                        + "**바깥**에 있다는 뜻이고, 그러면 페이지마다 집합을 다시 만든다",
                        first, tail)
                .isLessThan(first / 2);

        System.out.printf("[CY-954] 첫 페이지 읽기 호출 %d · 끝 커서 %d%n", first, tail);
    }

    /**
     * <b>상한 값 자체를 못 박는다.</b>
     *
     * <p>{@code <=} 로만 걸면 상한을 <b>120,000 으로 올려도</b> 이 시험도, API 시험
     * ({@code limit=100000} 거부), 어댑터 시험({@code 상한 + 1} 거부)도 전부 통과한다 —
     * 그 구간에 <b>"절대 내보내면 안 된다" 던 전량이 통째로 들어 있다.</b>
     *
     * <p>1,000 은 바이트에서 나온 값이다(행당 70~89B → 한 응답 70~89KB).
     * 근거는 {@link VerificationFindingRepository#MAX_TARGET_PAGE} 에 표로 있다.
     * 바꾸려면 그 표를 다시 재고 이 수를 함께 고쳐라.
     */
    @Test
    @DisplayName("페이지 상한이 1,000 이다 — 바꾸려면 바이트를 다시 재라")
    void thePageCapIsTheOneTheBytesJustify() {
        assertThat(VerificationFindingRepository.MAX_TARGET_PAGE)
                .as("올리면 한 응답이 커진다. 120,000 이면 10.7MB 다")
                .isEqualTo(1_000);
        assertThat(PAGE).isEqualTo(VerificationFindingRepository.MAX_TARGET_PAGE);
    }

    private long readsFor(long before, long after, ResidualCursor cursor) {
        findings.residualTargets(before, after, null, cursor, PAGE);   // 워밍업
        long start = handlerReads();
        findings.residualTargets(before, after, null, cursor, PAGE);
        return handlerReads() - start;
    }

    private long handlerReads() {
        return jdbcClient.sql("SHOW SESSION STATUS WHERE Variable_name IN"
                        + " ('Handler_read_next','Handler_read_key','Handler_read_first',"
                        + "  'Handler_read_rnd_next')")
                .query((rs, i) -> rs.getLong("Value")).list()
                .stream().mapToLong(Long::longValue).sum();
    }

    private static FindingType lastType() {
        List<FindingType> sorted = java.util.Arrays.stream(FindingType.values())
                .sorted(java.util.Comparator.comparing(Enum::name))
                .toList();
        return sorted.get(sorted.size() - 1);
    }

    /** 절반은 양쪽(잔여), 절반은 뒤 실행에만(신규). 형제 프로브와 같은 방식으로 심는다. */
    private void seed(long beforeRun, long afterRun) {
        for (FindingType type : FindingType.values()) {
            insert(beforeRun, type, 0, PER_RULE, 0);
            insert(afterRun, type, 0, PER_RULE / 2, 0);
            insert(afterRun, type, PER_RULE / 2, PER_RULE, PER_RULE);
        }
    }

    private void insert(long runId, FindingType type, int from, int to, int offset) {
        for (int chunk = from; chunk < to; chunk += 500) {
            int end = Math.min(chunk + 500, to);
            StringBuilder sql = new StringBuilder(
                    "INSERT INTO verification_findings"
                            + " (run_id, finding_type, target_key, expected, actual) VALUES ");
            for (int i = chunk; i < end; i++) {
                sql.append(i > chunk ? "," : "")
                        .append("(").append(runId).append(",'").append(type.name())
                        .append("','").append(type.name()).append(':').append(offset + i)
                        .append("','기대','실제')");
            }
            jdbcClient.sql(sql.toString()).update();
        }
    }

    private long newRun(int attempt) {
        return runs.save(VerificationRun.start(
                AS_OF, null, ScopeType.FULL, DatasetType.CLEAN, attempt, AS_OF)).id();
    }
}
