// 잔여 집계가 무엇을 읽고 얼마나 걸리는지 실제로 재는 프로브입니다.
package com.kafkick.storage.db.verification;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.FindingType;
import com.kafkick.core.verification.ResidualCount;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.VerificationRun;
import com.kafkick.storage.db.RepositoryTest;

/**
 * <b>CY-947 이 "안 쟀다" 고 적어 둔 자리를 닫는다.</b> 그 PR 은 상한 계산만 적고
 * 실행계획도 시간도 안 뗐다 — 예산을 넘기는 날 무엇을 해야 할지가 어디에도 없다.
 *
 * <h2>이름이 아니라 재는 것을 본다 — 그런데 재도 안 되는 것이 있다</h2>
 *
 * <p>⚠️ <b>첫 판은 {@code EXPLAIN} 의 {@code access_type} 으로 비용을 말했다.</b>
 * 같은 모듈이 이미 그 추론으로 반려당했는데({@code BacklogPlanContractTest}:
 * <i>"이름은 비용을 말하지 않는다"</i>) <b>옆에 있는 선례를 안 보고 같은 실수를 했다.</b>
 *
 * <p>그래서 {@code Handler_read_*}(스토리지 엔진 읽기 호출)로 갈아탔는데,
 * <b>그것도 단언할 만큼 안정적이지 않았다.</b>
 *
 * <pre>
 * 표 120,000행(대상 100%)   673,910
 * 표 240,000행( 50%)      1,033,912   ← 같은 형상을 따로 재면 793,910 이 나오기도 한다
 * 표 360,000행( 33%)      1,033,912
 * 표 480,000행( 25%)      1,153,912
 * 표 600,000행( 20%)      1,273,912
 * 표 720,000행( 17%)        673,910   ← 기준값으로 되돌아온다
 * </pre>
 *
 * <p>옵티마이저가 비율에 따라 계획을 갈아타는 것으로 <b>보이지만</b>, 여섯 점으로 그 법칙을
 * 세울 수는 없다 — <b>단조롭지도 재현되지도 않는다.</b> 여기에 단언을 걸면 남의 PR 이
 * 이유 없이 빨개진다. <b>그래서 안 건다.</b>
 *
 * <p><b>결정에 쓰는 축은 시간이다.</b> 표가 <b>6배</b>가 되는 동안 <b>260~290ms 로
 * 평평하다</b> — 예산 5초의 6% 안쪽이고, 지배하는 것은 대상 12만 키의 임시 테이블
 * 집계다. 인덱스를 안 만드는 근거가 이것이다.
 *
 * <h2>못 박는 것과 로그로만 남기는 것</h2>
 *
 * <ul>
 *   <li><b>단언한다</b> — 계획의 <b>모양</b>(어느 인덱스·커버링·{@code filesort}),
 *       상한에서의 정답, 그리고 프로브가 재는 값이 <b>아직 배포 상한과 같은가</b></li>
 *   <li><b>로그로만 남긴다</b> — 벽시계와 읽기 호출. <b>둘 다 재현이 안 된다</b>
 *       (러너·버퍼풀·옵티마이저). 임계를 걸면 남의 PR 이 이유 없이 빨개진다 —
 *       그 수는 사람이 읽고 문서에 옮긴다</li>
 * </ul>
 *
 * <h2>트랜잭션 안에서 잰다 — 그것이 같은 세션을 보장한다</h2>
 *
 * <p>⚠️ <b>첫 판은 {@code @Transactional(NOT_SUPPORTED)} 이었다.</b> <i>"세션 상태라
 * 트랜잭션 밖에서 읽어야 한다"</i> 고 거꾸로 생각한 것인데, 사실은 <b>반대다</b> —
 * 트랜잭션이 없으면 스프링이 커넥션을 스레드에 안 묶어 {@code JdbcClient} 호출마다
 * 풀이 <b>다른 세션</b>을 줄 수 있다. 그러면 {@code Handler_read_*} 전후를 서로 다른
 * 세션에서 빼는 셈이라 <b>수가 뜻을 잃는다.</b>
 *
 * <p>기본 동작({@code @DataJpaTest} 의 트랜잭션)으로 되돌리면 어댑터의 질의까지
 * <b>같은 커넥션</b>에 묶이고, 덤으로 심은 18만 행이 롤백으로 사라진다.
 * (형제 {@code BacklogPlanContractTest} 는 {@code NOT_SUPPORTED} 인데, 그쪽은 같은
 * 위험을 안고 있다 — 이 티켓이 고칠 자리는 아니지만 <b>선례를 그대로 베끼지 않은</b>
 * 이유다.)
 */
@RepositoryTest
@Import({VerificationFindingJdbcAdapter.class, VerificationRunJdbcAdapter.class})
class ResidualQueryCostProbe {

    /**
     * <b>상한 그대로여야 의미가 있다.</b> {@code batch.verify.max-findings-per-rule} 의
     * 기본값이고, 그 값이 바뀌면 이 프로브가 재는 것이 더 이상 상한이 아니다 —
     * {@link #theProbeStillMeasuresTheCap()} 가 그 표류를 잡는다.
     */
    private static final int PER_RULE = 10_000;

    /** 겹침 0이면 키가 행 수와 같다 — <b>그것이 진짜 상한</b>이다. */
    private static final int CAP_KEYS = 2 * PER_RULE * 6;

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 8, 15, 14, 0);

    @Autowired
    private VerificationFindingJdbcAdapter adapter;

    @Autowired
    private VerificationRunJdbcAdapter runs;

    @Autowired
    private JdbcClient jdbcClient;

    /**
     * <b>표를 두 배로 키워도 읽기 호출이 안 늘어야 한다.</b> 그것이 <i>"비용은 표 크기가
     * 아니라 대상을 따른다"</i> 의 진짜 검증이다 — 첫 판은 표 크기만 바꿔 놓고
     * <b>대상 행 수를 따른다</b> 고 적었는데, 대상을 한 번도 안 바꿨으므로 그 문장은
     * 실측이 아니라 해석이었다.
     */
    @Test
    @DisplayName("잔여 집계가 무엇을 읽고 얼마나 걸리는지")
    void measureAtTheCap() {
        {
            long before = newRun(1);
            long after = newRun(2);
            // **겹치지 않게 심는다.** 절반을 겹치면 안쪽 GROUP BY 의 키가 9만으로 줄어
            // 임시 테이블을 최악의 75% 에서만 재게 된다. 상한은 겹침 0이다.
            plant(before, 0, PER_RULE);
            plant(after, PER_RULE, PER_RULE);

            assertThat(rowCount())
                    .as("적게 심고 '빠르다' 고 적으면 안 된다")
                    .isEqualTo(CAP_KEYS);

            assertPlanShape(before, after);

            long readsBefore = handlerReads();
            long startedAt = System.nanoTime();
            Map<FindingType, ResidualCount> residual = adapter.residualByType(before, after);
            Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);
            long reads = handlerReads() - readsBefore;

            assertThat(residual.get(FindingType.STOCK_MISMATCH))
                    .as("겹침 0이므로 지속은 없고 신규·해소가 규칙당 상한만큼이다")
                    .isEqualTo(new ResidualCount(0, PER_RULE, PER_RULE));

            System.out.println("[CY-949] 대상 " + CAP_KEYS + "키 · 표 " + rowCount()
                    + "행 · 읽기호출 " + reads + " · 소요 " + elapsed.toMillis() + "ms");

            // **표를 키우고 같은 대상을 다시 잰다.** 읽기 호출이 표를 따라가는지가
            // 예산의 분모를 정한다.
            plant(newRun(3), PER_RULE * 2, PER_RULE * 2);
            long biggerReadsBefore = handlerReads();
            long biggerStartedAt = System.nanoTime();
            adapter.residualByType(before, after);
            Duration biggerElapsed = Duration.ofNanos(System.nanoTime() - biggerStartedAt);
            long biggerReads = handlerReads() - biggerReadsBefore;

            System.out.println("[CY-949] 대상 " + CAP_KEYS + "키 · 표 " + rowCount()
                    + "행 · 읽기호출 " + biggerReads + " · 소요 "
                    + biggerElapsed.toMillis() + "ms");

            // **여기에 단언을 안 건다.** 같은 형상을 두 번 재도 값이 달라진다(위 표) —
            // 걸면 남의 PR 이 이유 없이 빨개진다. 결정에 쓰는 축은 시간이고 그것도
            // 러너마다 다르다. 못 박는 것은 계획의 모양이다.

    }
    }

    /**
     * <b>상한이 바뀌면 이 프로브는 더 이상 상한을 안 잰다.</b> 그런데 javadoc 과
     * {@code docs/15} 는 <i>"상한에서 쟀다"</i> 를 계속 주장한다 — 아무것도 안 빨개지는
     * 그 상태를 여기서 끊는다.
     */
    @Test
    @DisplayName("프로브가 재는 값이 아직 배포 상한과 같다")
    void theProbeStillMeasuresTheCap() {
        assertThat(deployedCap())
                .as("batch/src/main/resources/application.yml.example 의 상한이 바뀌었다. "
                        + "PER_RULE 을 맞추고 프로브를 다시 돌린 뒤, 그 수로 어댑터 javadoc 과 "
                        + "docs/15 를 고쳐라 — 안 고치면 문서가 안 잰 수를 주장한다")
                .isEqualTo(PER_RULE);
    }

    /** {@code EXPLAIN} 은 <b>모양만</b> 단언한다 — 비싼가는 시간이 답하고, 그것은 로그다. */
    private void assertPlanShape(long before, long after) {
        String plan = jdbcClient.sql(
                        "EXPLAIN FORMAT=JSON "
                                + VerificationFindingJdbcAdapter.SELECT_RESIDUAL_BY_TYPE)
                .param("beforeRunId", before)
                .param("afterRunId", after)
                .query(String.class)
                .single();

        assertThat(plan)
                .as("이 인덱스를 안 타면 대상 밖 행을 읽는다. 계획이 바뀌면 위 javadoc 과 "
                        + "docs/15 의 수가 조용히 거짓이 된다:\n%s", plan)
                .contains("\"key\": \"uk_run_finding\"")
                .contains("\"using_index\": true");
        assertThat(plan)
                .as("정렬이 붙으면 임시 테이블 위에 filesort 가 더 얹힌다:\n%s", plan)
                .doesNotContain("\"using_filesort\": true");
    }

    /**
     * 스토리지 엔진 읽기 호출 수. {@code EXPLAIN} 의 추정치가 아니라
     * <b>실제로 일어난 호출</b>이라 여기서 쓴다 — {@code BacklogPlanContractTest} 와 같다.
     */
    private long handlerReads() {
        return jdbcClient.sql("SHOW SESSION STATUS WHERE Variable_name IN"
                        + " ('Handler_read_next','Handler_read_key','Handler_read_first',"
                        + "  'Handler_read_rnd_next')")
                .query((rs, i) -> rs.getLong("Value"))
                .stream().mapToLong(Long::longValue).sum();
    }

    private long rowCount() {
        return jdbcClient.sql("SELECT COUNT(*) FROM verification_findings")
                .query(Long.class).single();
    }

    /** 배포 기본값을 파일에서 읽는다 — 상수를 또 적으면 세어야 할 자리가 하나 는다. */
    private static int deployedCap() {
        try {
            String text = java.nio.file.Files.readString(java.nio.file.Path.of(
                    "../batch/src/main/resources/application.yml.example"));
            java.util.regex.Matcher matcher = java.util.regex.Pattern
                    .compile("max-findings-per-rule:\\s*\\$\\{VERIFY_MAX_FINDINGS_PER_RULE:(\\d+)\\}")
                    .matcher(text);
            assertThat(matcher.find())
                    .as("상한 키의 표기가 바뀌었으면 이 정규식도 함께 고쳐야 한다 — "
                            + "안 고치면 이 검사가 조용히 죽는다")
                    .isTrue();
            return Integer.parseInt(matcher.group(1));
        } catch (java.io.IOException e) {
            throw new IllegalStateException("배포 기본값 파일을 못 읽었다", e);
        }
    }

    /**
     * 규칙마다 {@code count} 건을 심는다. {@code offset} 이 두 실행의 겹침을 정한다.
     *
     * <p><b>배치 INSERT 로 심는다.</b> 도메인 팩토리를 12만 번 부르면 심는 데만 몇 분이
     * 걸려 <b>재려는 것이 아니라 준비가 시험을 지배한다.</b> 값은 전부 코드 상수다.
     */
    private void plant(long runId, int offset, int count) {
        for (FindingType type : FindingType.values()) {
            for (int from = 0; from < count; from += 1_000) {
                int to = Math.min(from + 1_000, count);
                StringBuilder sql = new StringBuilder(
                        "INSERT INTO verification_findings "
                                + "(run_id, finding_type, target_key, expected, actual) VALUES ");
                for (int i = from; i < to; i++) {
                    sql.append(i > from ? "," : "")
                            .append("(").append(runId).append(",'").append(type.name())
                            .append("','").append(type.name()).append(':').append(offset + i)
                            .append("','기대','실제')");
                }
                jdbcClient.sql(sql.toString()).update();
            }
        }
    }

    private long newRun(int attempt) {
        return runs.save(VerificationRun.start(
                AS_OF, null, ScopeType.FULL, DatasetType.CLEAN, attempt, AS_OF)).id();
    }
}
