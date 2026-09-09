// 잔여 집계가 5초 예산 안에서 도는지 실제로 재는 프로브입니다.
package com.kafkick.storage.db.verification;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
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
 * <b>CY-947 이 "안 쟀다" 고 적어 둔 자리를 닫는다.</b> 그 PR 은 상한 계산
 * (규칙당 10,000 × 규칙 6 × 실행 2 = <b>12만 키</b>)만 적고 실행계획도 시간도 안 뗐다 —
 * 그러면 예산을 넘기는 날 무엇을 해야 할지가 어디에도 없다.
 *
 * <p><b>이것은 회귀 시험이 아니라 프로브다.</b> 공유 컨테이너의 성능은 CI 러너와
 * 개발 기기에서 다르므로 <b>시간에 임계를 걸지 않는다</b> — 걸면 남의 PR 이 이유 없이
 * 빨개진다. 여기서 못 박는 것은 <b>실행계획의 모양</b>이고, 시간은 로그로 남겨 사람이
 * 문서에 옮긴다.
 *
 * <p>{@code EXPLAIN} 의 {@code rows} 는 <b>근거로 안 쓴다</b> — 추정치다. 세는 것은
 * {@code COUNT} 로 세고, 느린지는 시간으로 잰다.
 *
 * <p><b>이 프로브가 CI 에 더하는 비용도 쟀다 — 약 11초</b>(그중 질의는 3초 미만이고
 * 나머지는 18만 행 심기다). 남의 PR 이 그만큼 느려지는 것이라 <b>값을 알고 두는 것</b>과
 * 모르고 두는 것은 다르다. 표를 더 키우면 {@code range} 갈래까지 한 번에 재지지만
 * 심는 데만 8분이 걸려 <b>일부러 안 한다</b> — 그 수는 손으로 한 번 재서 위에 적었다.
 */
@RepositoryTest
@Import({VerificationFindingJdbcAdapter.class, VerificationRunJdbcAdapter.class})
class ResidualQueryCostProbe {

    /**
     * <b>상한 그대로다.</b> {@code batch.verify.max-findings-per-rule} 기본 10,000 에
     * 규칙 여섯 — 한 실행이 6만이고 두 실행이면 12만이다.
     */
    private static final int PER_RULE = 10_000;

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 8, 15, 14, 0);

    @Autowired
    private VerificationFindingJdbcAdapter adapter;

    @Autowired
    private VerificationRunJdbcAdapter runs;

    @Autowired
    private JdbcClient jdbcClient;

    @Test
    @DisplayName("상한 12만 키에서 잔여 집계의 실행계획과 소요를 잰다")
    void measureAtTheCap() {
        long before = newRun(1);
        long after = newRun(2);

        // **절반은 겹치게 심는다.** 전부 겹치거나 전부 안 겹치면 한쪽 갈래만 타서
        // 실제 부하와 다르다 — 지속·신규·해소가 다 나오는 모양이어야 한다.
        plant(before, 0, PER_RULE);
        plant(after, PER_RULE / 2, PER_RULE);

        // **남의 실행을 함께 심는다.** 두 실행만 있으면 표 전체가 곧 대상이라, 옵티마이저가
        // 무엇을 고르든 같은 값이 나온다. 운영 표는 실행이 계속 쌓이므로 그쪽이 실제 형상이다.
        //
        // ⚠️ **접근 방식은 비율에 달렸다(실측).** 대상이 표의 67%(120k/180k)면
        //    access_type=index(전체 커버링 스캔), 29%(120k/420k)면 range 다 — 둘 다
        //    uk_run_finding 이고 둘 다 using_index=true 이며 소요도 275ms / 268ms 로 같다.
        //    **비용은 표 크기가 아니라 대상 행 수를 따른다.**
        //    여기서는 앞엣것 형상으로 둔다 — 뒤엣것은 심는 데만 8분이 걸려 CI 를 그만큼
        //    늘린다. 그 수는 손으로 한 번 재서 docs/15 에 적었다.
        long stranger = newRun(3);
        plant(stranger, 0, PER_RULE);

        long total = jdbcClient.sql("SELECT COUNT(*) FROM verification_findings")
                .query(Long.class).single();
        assertThat(total)
                .as("상한대로 심었는지 먼저 확인한다 — 적게 심고 '빠르다' 고 적으면 안 된다")
                .isEqualTo(3L * PER_RULE * FindingType.values().length);

        String plan = jdbcClient.sql(explainOf(before, after)).query(String.class).single();

        long startedAt = System.nanoTime();
        Map<FindingType, ResidualCount> residual = adapter.residualByType(before, after);
        Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

        assertThat(residual).hasSize(FindingType.values().length);
        assertThat(residual.get(FindingType.STOCK_MISMATCH))
                .as("절반이 겹치게 심었으니 세 갈래가 다 나와야 한다 — 안 나오면 부하가 아니다")
                .isEqualTo(new ResidualCount(PER_RULE / 2, PER_RULE / 2, PER_RULE / 2));

        System.out.println("[CY-949] 표 전체 " + total + "행 · 대상 "
                + (2L * PER_RULE * FindingType.values().length) + "행 · 소요 "
                + elapsed.toMillis() + "ms");
        System.out.println("[CY-949] EXPLAIN\n" + plan);

        assertThat(elapsed)
                .as("시간에 임계를 안 걸지만, 예산(5초)의 열 배를 넘으면 그것은 성능이 "
                        + "아니라 설계 문제다. 그때는 이 프로브가 아니라 티켓이 필요하다")
                .isLessThan(Duration.ofSeconds(50));
    }

    /** {@code EXPLAIN} 은 파라미터 바인딩을 안 쓰고 리터럴로 넣는다 — 계획만 본다. */
    private static String explainOf(long before, long after) {
        return """
                EXPLAIN FORMAT=JSON
                SELECT MIN(k.finding_type) AS finding_type,
                       SUM(k.sides = 2)                        AS persisted,
                       SUM(k.sides = 1 AND k.has_after = 1)    AS introduced,
                       SUM(k.sides = 1 AND k.has_after = 0)    AS resolved
                  FROM (SELECT MIN(finding_type)         AS finding_type,
                               COUNT(*)                  AS sides,
                               MAX(run_id = %d)          AS has_after
                          FROM verification_findings
                         WHERE run_id IN (%d, %d)
                         GROUP BY CAST(finding_type AS BINARY),
                                  CAST(target_key AS BINARY)) k
                 GROUP BY CAST(k.finding_type AS BINARY)
                """.formatted(after, before, after);
    }

    /**
     * 규칙마다 {@code count} 건을 심는다. {@code offset} 이 두 실행의 겹침을 정한다 —
     * {@code offset = count / 2} 면 절반이 겹치고 나머지가 신규·해소로 갈린다.
     *
     * <p><b>배치 INSERT 로 심는다.</b> 도메인 팩토리를 12만 번 부르면 심는 데만 몇 분이
     * 걸려 <b>재려는 것이 아니라 준비가 시험을 지배한다.</b>
     */
    private void plant(long runId, int offset, int count) {
        for (FindingType type : FindingType.values()) {
            List<Object[]> rows = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                rows.add(new Object[] {runId, type.name(), type.name() + ":" + (offset + i)});
            }
            for (int from = 0; from < rows.size(); from += 1_000) {
                StringBuilder sql = new StringBuilder(
                        "INSERT INTO verification_findings "
                                + "(run_id, finding_type, target_key, expected, actual) VALUES ");
                int to = Math.min(from + 1_000, rows.size());
                for (int i = from; i < to; i++) {
                    sql.append(i > from ? "," : "")
                            .append("(").append(rows.get(i)[0]).append(",'")
                            .append(rows.get(i)[1]).append("','")
                            .append(rows.get(i)[2]).append("','기대','실제')");
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
