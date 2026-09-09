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
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

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
 * <h2>접근 방식 이름이 아니라 읽은 행 수를 본다</h2>
 *
 * <p>⚠️ <b>첫 판은 {@code EXPLAIN} 의 {@code access_type} 으로 비용을 말했다.</b>
 * {@code index} 가 나오면 <i>"전체 인덱스 스캔"</i>, {@code range} 면 <i>"두 구간"</i> 으로
 * 읽고 그 위에 결론을 세웠는데, <b>같은 모듈이 이미 그 추론으로 반려당했다</b> —
 * {@code BacklogPlanContractTest} 가 <i>"이름은 비용을 말하지 않는다"</i> 를 적고
 * {@code Handler_read_*} 로 갈아탔다. <b>옆에 있는 선례를 안 보고 같은 실수를 했다.</b>
 *
 * <p>그래서 여기도 <b>스토리지 엔진 읽기 호출 수</b>를 센다. {@code EXPLAIN} 은
 * <b>모양</b>(어느 인덱스인가·커버링인가·정렬이 붙는가)만 단언하고, <b>비싼가</b> 는
 * 호출 수와 시간이 답한다. {@code EXPLAIN} 의 {@code rows} 는 추정치라 안 쓴다.
 *
 * <h2>못 박는 것과 로그로만 남기는 것</h2>
 *
 * <ul>
 *   <li><b>단언한다</b> — 어느 인덱스를 타는가, 커버링인가, {@code filesort} 가 붙는가,
 *       그리고 <b>읽기 호출이 표 크기를 안 따르는가</b></li>
 *   <li><b>로그로만 남긴다</b> — 벽시계. 러너마다 달라 임계를 걸면 남의 PR 이 이유 없이
 *       빨개진다. 그 수는 사람이 문서에 옮긴다</li>
 * </ul>
 *
 * <p>{@code @Transactional(NOT_SUPPORTED)} 이다 — {@code Handler_read_*} 는 세션 상태라
 * 같은 커넥션에서 읽어야 하고, 형제도 같은 이유로 같은 모양을 쓴다.
 * <b>그래서 심은 행을 손으로 지운다.</b>
 */
@RepositoryTest
@Import({VerificationFindingJdbcAdapter.class, VerificationRunJdbcAdapter.class})
@Transactional(propagation = Propagation.NOT_SUPPORTED)
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
        try {
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

            // **읽기 호출은 표를 따라 는다 — 그것은 사실이고 막을 것이 아니다.**
            // 막을 것은 그것이 **행당 한 번을 넘는 것**이다. 커버링이 깨지거나 조인이
            // 붙으면 추가분이 배수로 뛴다 — 그때는 표가 커질수록 예산이 무너진다.
            long addedRows = CAP_KEYS;
            assertThat(biggerReads - reads)
                    .as("표에 %d행을 더했는데 읽기 호출이 그보다 훨씬 많이 늘었다. "
                            + "커버링이 깨졌거나 대상 밖 행을 여러 번 읽는다", addedRows)
                    .isLessThan((long) (addedRows * 1.1));

        } finally {
            jdbcClient.sql("DELETE FROM verification_findings").update();
            jdbcClient.sql("DELETE FROM verification_runs").update();
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

    /** {@code EXPLAIN} 은 <b>모양만</b> 단언한다 — 비싼가는 읽기 호출과 시간이 답한다. */
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
