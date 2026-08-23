package com.kafkick.storage.db.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.kafkick.core.benchmark.RunTimeseriesArchiver.Metric;
import com.kafkick.core.benchmark.RunTimeseriesArchiver.Sample;
import com.kafkick.core.benchmark.RunTimeseriesArchiver.State;

class JdbcRunTimeseriesArchiveStoreTest {

    private static MySQLContainer mysql;
    private static JdbcTemplate jdbc;
    private static JdbcRunTimeseriesArchiveStore store;

    @BeforeAll
    static void start() {
        mysql = new MySQLContainer(DockerImageName.parse("mysql:8.4")).withDatabaseName("app");
        mysql.start();
        Flyway.configure().dataSource(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())
                .locations("classpath:db/migration").load().migrate();
        jdbc = new JdbcTemplate(new org.springframework.jdbc.datasource.DriverManagerDataSource(
                mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword()));
        jdbc.update("""
                INSERT INTO benchmark_runs
                (id, run_key, run_type, scenario_code, engine_version, release_stage, queue_mode,
                 run_status, started_at, requested_by, app_replicas, available_processors,
                 tomcat_workers_total, hikari_pool_total, mysql_max_connections,
                 offered_rps, load_hold_seconds, observation_hold_seconds)
                VALUES (1, 'V3-MAIN-STORE', 'MAIN', 'SPIKE', 'V3', 'V3', 'ADAPTIVE',
                        'RUNNING', '2026-08-23 00:00:00', 'tester', 1, 6, 60, 12, 50, 20000, 5, 60)
                """);
        jdbc.update("""
                INSERT INTO benchmark_runs
                (id, run_key, run_type, scenario_code, engine_version, release_stage, queue_mode,
                 run_status, started_at, load_stopped_at, requested_by, app_replicas, available_processors,
                 tomcat_workers_total, hikari_pool_total, mysql_max_connections,
                 offered_rps, load_hold_seconds, observation_hold_seconds)
                VALUES (2, 'V3-MAIN-STORE-2', 'MAIN', 'SPIKE', 'V3', 'V3', 'ADAPTIVE',
                        'LOAD_STOPPED', '2026-08-23 00:00:00', '2026-08-23 00:00:05', 'tester',
                        1, 6, 60, 12, 50, 20000, 5, 60)
                """);
        store = new JdbcRunTimeseriesArchiveStore(jdbc);
    }

    @AfterAll
    static void stop() { if (mysql != null) mysql.stop(); }

    @Test
    void insertsNullAsSqlNullAndDeletesOnlyTheRun() {
        store.insert(1, List.of(
                new Sample(Metric.STOCK_REMAINING, 0, Instant.parse("2026-08-23T00:00:00Z"),
                        0d, State.VALID, null),
                new Sample(Metric.LATENCY_P99, 1, Instant.parse("2026-08-23T00:00:01Z"),
                        null, State.UNAVAILABLE, "api-1:9090")));
        store.insert(2, List.of(new Sample(
                Metric.DB_POOL_USAGE, 0, Instant.parse("2026-08-23T00:00:00Z"),
                0.5d, State.VALID, null)));

        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM run_timeseries WHERE benchmark_run_id=1",
                Integer.class)).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT value IS NULL FROM run_timeseries"
                + " WHERE benchmark_run_id=1 AND metric='LATENCY_P99'", Boolean.class)).isTrue();

        store.deleteForRun(1);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM run_timeseries WHERE benchmark_run_id=1",
                Integer.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM run_timeseries WHERE benchmark_run_id=2",
                Integer.class)).isEqualTo(1);
    }
}
