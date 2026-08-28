package com.kafkick.batch.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * 대상 조회를 <b>실제 MySQL 에 태워</b> 고정한다. 문자열 단언으로는 못 잡는다 — 회차 하나에
 * 비 FINALIZED run 이 둘일 때 어느 행이 이기는지는 SQL 의 의미이지 SQL 의 모양이 아니다.
 *
 * <p>{@code benchmark_runs.coupon_id} 에는 유일 제약이 없다. {@code uk_run_running} 은
 * RUNNING 만 하나로 묶으므로 LOAD_STOPPED·OBSERVED 인 run 은 몇 개든 남고, <b>한 회차를 두고
 * WARMUP run 을 돌린 뒤 MAIN run 을 돌리면</b> 그 회차에 run 두 행이 공존한다. 그때 뒤늦은
 * WARMUP(V1) 행이 이기면 진행 중인 MAIN(V2) 의 PENDING 이 N_A 로 덮여 사라진다.
 */
@SpringBootTest(properties = "spring.flyway.enabled=true")
@Import(MySqlContainerConfig.class)
class PendingIssuedGaugeTargetSqlTest {

    @Autowired
    @Qualifier("obs")
    private JdbcTemplate observationJdbcTemplate;

    @Autowired
    private DataSource dataSource;

    @Test
    @DisplayName("한 회차에 비 FINALIZED run 이 둘이어도 최신 run 한 행만 나온다")
    void oneRowPerRoundEvenWhenSeveralRunsAreStillOpen() {
        JdbcTemplate app = new JdbcTemplate(dataSource);
        app.update("DELETE FROM benchmark_runs");
        insert(app, 1L, "WARM-1", "V1", "LOAD_STOPPED", 4242L);
        insert(app, 2L, "MAIN-1", "V2", "RUNNING", 4242L);

        List<Map<String, Object>> rows =
            observationJdbcTemplate.queryForList(PendingIssuedGaugeCollector.OBSERVABLE_RUNS_SQL);

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("coupon_id")).isEqualTo(4242L);
        assertThat(rows.get(0).get("engine_version")).isEqualTo("V2");
    }

    @Test
    @DisplayName("최신 판정은 id 다 — 나중에 연 run 이 이긴다")
    void theLatestRunWinsRegardlessOfInsertOrder() {
        JdbcTemplate app = new JdbcTemplate(dataSource);
        app.update("DELETE FROM benchmark_runs");
        insert(app, 10L, "MAIN-2", "V2", "OBSERVED", 77L);
        insert(app, 11L, "WARM-2", "V1", "LOAD_STOPPED", 77L);

        List<Map<String, Object>> rows =
            observationJdbcTemplate.queryForList(PendingIssuedGaugeCollector.OBSERVABLE_RUNS_SQL);

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("engine_version")).isEqualTo("V1");
    }

    @Test
    @DisplayName("FINALIZED 가 최신이어도 그 회차의 열린 run 이 이긴다 — 되살아나지도 않는다")
    void finalizedRunNeitherWinsNorResurrects() {
        JdbcTemplate app = new JdbcTemplate(dataSource);
        app.update("DELETE FROM benchmark_runs");
        insert(app, 29L, "OPEN-3", "V2", "LOAD_STOPPED", 55L);
        insert(app, 30L, "DONE-3", "V1", "FINALIZED", 55L);

        List<Map<String, Object>> rows =
            observationJdbcTemplate.queryForList(PendingIssuedGaugeCollector.OBSERVABLE_RUNS_SQL);

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("engine_version")).isEqualTo("V2");
    }

    @Test
    @DisplayName("FINALIZED 만 남은 회차는 대상에서 사라진다")
    void aRoundWhoseOnlyRunIsFinalizedDropsOut() {
        JdbcTemplate app = new JdbcTemplate(dataSource);
        app.update("DELETE FROM benchmark_runs");
        insert(app, 40L, "DONE-4", "V2", "FINALIZED", 99L);

        assertThat(observationJdbcTemplate.queryForList(
            PendingIssuedGaugeCollector.OBSERVABLE_RUNS_SQL)).isEmpty();
    }

    @Test
    @DisplayName("회차가 붙지 않은 run 은 대상이 아니다")
    void runsWithoutARoundAreNotTargets() {
        JdbcTemplate app = new JdbcTemplate(dataSource);
        app.update("DELETE FROM benchmark_runs");
        insert(app, 20L, "OPEN-1", "V2", "LOAD_STOPPED", 88L);
        insert(app, 21L, "NOCOUPON", "V2", "LOAD_STOPPED", null);

        List<Map<String, Object>> rows =
            observationJdbcTemplate.queryForList(PendingIssuedGaugeCollector.OBSERVABLE_RUNS_SQL);

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get("coupon_id")).isEqualTo(88L);
    }

    private static void insert(
        JdbcTemplate app, long id, String runKey, String engine, String status, Long couponId
    ) {
        app.update("""
            INSERT INTO benchmark_runs
              (id, run_key, run_type, scenario_code, engine_version, release_stage, queue_mode,
               coupon_id, run_status, started_at, load_stopped_at, observation_stopped_at,
               finalized_at, requested_by, app_replicas, available_processors,
               tomcat_workers_total, hikari_pool_total, mysql_max_connections,
               offered_rps, load_hold_seconds, observation_hold_seconds, observed_lag_total,
               client_measured_at, client_request_count, client_failure_count,
               client_dropped_iterations, client_tps, client_p95_millis, client_p99_millis)
            VALUES (?, ?, 'MAIN', 'SPIKE', ?, 'V3', 'ADAPTIVE', ?, ?,
                    '2026-08-25 23:00:00', ?, ?, ?, 'tester', 1, 6, 60, 12, 50,
                    20000, 5, 60, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            id, runKey, engine, couponId, status,
            "RUNNING".equals(status) ? null : "2026-08-25 23:01:00",
            List.of("OBSERVED", "FINALIZED").contains(status) ? "2026-08-25 23:02:00" : null,
            "FINALIZED".equals(status) ? "2026-08-26 00:00:00" : null,
            List.of("OBSERVED", "FINALIZED").contains(status) ? 0L : null,
            "FINALIZED".equals(status) ? "2026-08-25 23:01:00" : null,
            "FINALIZED".equals(status) ? 1L : null,
            "FINALIZED".equals(status) ? 0L : null,
            "FINALIZED".equals(status) ? 0L : null,
            "FINALIZED".equals(status) ? 1L : null,
            "FINALIZED".equals(status) ? 1L : null,
            "FINALIZED".equals(status) ? 1L : null);
    }
}
