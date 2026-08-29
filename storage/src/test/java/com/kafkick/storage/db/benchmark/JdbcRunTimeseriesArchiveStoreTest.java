package com.kafkick.storage.db.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.kafkick.core.benchmark.RunTimeseriesArchiver.Metric;
import com.kafkick.core.benchmark.RunTimeseriesArchiver.Sample;
import com.kafkick.core.benchmark.RunTimeseriesArchiver.State;

class JdbcRunTimeseriesArchiveStoreTest {

    private static final String TOKEN_A = "00000000-0000-4000-8000-000000000001";
    private static final String TOKEN_B = "00000000-0000-4000-8000-000000000002";

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
        claim(1, TOKEN_A);
        store.replaceForRun(1, TOKEN_A, List.of(
                new Sample(Metric.STOCK_REMAINING, 0, Instant.parse("2026-08-23T00:00:00Z"),
                        0d, State.VALID, null),
                new Sample(Metric.LATENCY_P99, 1, Instant.parse("2026-08-23T00:00:01Z"),
                        null, State.UNAVAILABLE, "api-1:9090")), 500);
        claim(2, TOKEN_B);
        store.replaceForRun(2, TOKEN_B, List.of(new Sample(
                Metric.DB_POOL_USAGE, 0, Instant.parse("2026-08-23T00:00:00Z"),
                0.5d, State.VALID, null)), 500);

        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM run_timeseries WHERE benchmark_run_id=1",
                Integer.class)).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT value IS NULL FROM run_timeseries"
                + " WHERE benchmark_run_id=1 AND metric='LATENCY_P99'", Boolean.class)).isTrue();

        claim(1, TOKEN_B);
        store.replaceForRun(1, TOKEN_B, List.of(), 500);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM run_timeseries WHERE benchmark_run_id=1",
                Integer.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM run_timeseries WHERE benchmark_run_id=2",
                Integer.class)).isEqualTo(1);
    }

    @Test
    void failedReplacementRollsBackDeletionAndPartialInsert() throws Exception {
        Sample original = new Sample(Metric.STOCK_REMAINING, 9,
            Instant.parse("2026-08-23T00:00:09Z"), 9d, State.VALID, null);
        claim(1, TOKEN_A);
        store.replaceForRun(1, TOKEN_A, List.of(original), 500);
        Sample duplicate = new Sample(Metric.STOCK_REMAINING, 0,
            Instant.parse("2026-08-23T00:00:00Z"), 1d, State.VALID, null);

        claim(1, TOKEN_B);
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.getEnvironment().getPropertySources().addFirst(
                new org.springframework.core.env.MapPropertySource(
                    "test", java.util.Map.of("observation.datasource.enabled", "true")));
            context.register(TransactionTestConfig.class);
            context.refresh();
            com.kafkick.core.benchmark.RunTimeseriesArchiver.ArchiveStore proxied =
                context.getBean(com.kafkick.core.benchmark.RunTimeseriesArchiver.ArchiveStore.class);
            assertThatThrownBy(() -> proxied.replaceForRun(
                1, TOKEN_B, List.of(duplicate, duplicate), 500))
                .isInstanceOf(org.springframework.dao.DataAccessException.class);
            assertThat(org.springframework.aop.support.AopUtils.isAopProxy(proxied)).isTrue();
        }

        assertThat(jdbc.queryForObject(
            "SELECT snapshot_sequence FROM run_timeseries WHERE benchmark_run_id=1",
            Long.class)).isEqualTo(9L);
        assertThat(JdbcRunTimeseriesArchiveStore.class
            .getMethod("replaceForRun", long.class, String.class, List.class, int.class)
            .isAnnotationPresent(org.springframework.transaction.annotation.Transactional.class)).isTrue();
    }

    @Test
    void staleWorkerCannotReplaceTheNewOwnersCompletedArchive() {
        Sample newOwner = new Sample(Metric.STOCK_REMAINING, 2,
            Instant.parse("2026-08-23T00:00:02Z"), 2d, State.VALID, "owner-b");
        Sample staleOwner = new Sample(Metric.STOCK_REMAINING, 1,
            Instant.parse("2026-08-23T00:00:01Z"), 1d, State.VALID, "owner-a");

        claim(1, TOKEN_A);
        claim(1, TOKEN_B);
        store.replaceForRun(1, TOKEN_B, List.of(newOwner), 500);
        assertThatThrownBy(() -> store.replaceForRun(1, TOKEN_A, List.of(staleOwner), 500))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("소유권");

        assertThat(jdbc.queryForObject(
            "SELECT source_instance FROM run_timeseries WHERE benchmark_run_id=1",
            String.class)).isEqualTo("owner-b");
    }

    private static void claim(long id, String token) {
        jdbc.update("""
            UPDATE benchmark_runs
            SET archive_status='IN_PROGRESS', archive_failure_reason=NULL,
                archive_claimed_at=CURRENT_TIMESTAMP(6), archive_claim_token=?
            WHERE id=?
            """, token, id);
    }

    @Configuration(proxyBeanMethods = false)
    @EnableTransactionManagement
    static class TransactionTestConfig {
        @Bean
        JdbcTemplate jdbcTemplate() {
            return new JdbcTemplate(jdbc.getDataSource());
        }

        @Bean
        PlatformTransactionManager transactionManager() {
            return new DataSourceTransactionManager(jdbc.getDataSource());
        }

        @Bean
        com.kafkick.core.benchmark.RunTimeseriesArchiver.ArchiveStore archiveStore(
            JdbcTemplate template
        ) {
            return new JdbcRunTimeseriesArchiveStore(template);
        }
    }
}
