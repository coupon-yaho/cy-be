package com.kafkick.storage.db.migration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

class RunTimeseriesMigrationTest {

    private static MySQLContainer mysql;

    @BeforeAll
    static void migrate() throws Exception {
        mysql = new MySQLContainer(DockerImageName.parse("mysql:8.4"))
                .withDatabaseName("app")
                .withCommand("--sql-mode=STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,"
                        + "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION");
        mysql.start();
        Flyway.configure().dataSource(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())
                .locations("classpath:db/migration").load().migrate();
        try (Connection connection = connection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("""
                    INSERT INTO benchmark_runs
                    (id, run_key, run_type, scenario_code, engine_version, release_stage, queue_mode,
                     run_status, started_at, requested_by, app_replicas, available_processors,
                     tomcat_workers_total, hikari_pool_total, mysql_max_connections,
                     offered_rps, load_hold_seconds, observation_hold_seconds)
                    VALUES (1, 'V3-MAIN-01', 'MAIN', 'SPIKE', 'V3', 'V3', 'ADAPTIVE',
                            'RUNNING', '2026-08-23 00:00:00', 'tester', 1, 6, 60, 12, 50, 20000, 5, 60)
                    """);
        }
    }

    @AfterAll
    static void stop() { if (mysql != null) mysql.stop(); }

    @AfterEach
    void clearTimeseries() throws Exception {
        execute("DELETE FROM run_timeseries");
    }

    @Test
    void migrationAndForeignKeyAreReal() throws Exception {
        execute("INSERT INTO run_timeseries"
                + " (benchmark_run_id, metric, snapshot_sequence, observed_at, value, state)"
                + " VALUES (1, 'STOCK_REMAINING', 0, '2026-08-23 00:00:00', 10000, 'VALID')");
        assertThat(query("SELECT COUNT(*) FROM benchmark_runs b JOIN run_timeseries t"
                + " ON t.benchmark_run_id=b.id WHERE b.id=1")).isEqualTo(1);
        assertThat(query("SELECT COUNT(*) FROM information_schema.referential_constraints"
                + " WHERE constraint_schema=DATABASE() AND constraint_name='fk_timeseries_benchmark_run'"))
                .isEqualTo(1);
        assertThatThrownBy(() -> execute("INSERT INTO run_timeseries"
                + " (benchmark_run_id, metric, snapshot_sequence, observed_at, value, state)"
                + " VALUES (999, 'STOCK_REMAINING', 0, NOW(6), 1, 'VALID')"))
                .hasMessageContaining("fk_timeseries_benchmark_run");
    }

    @Test
    void rejectsNullCheckLoopholesAndDuplicates() throws Exception {
        assertThatThrownBy(() -> execute("INSERT INTO run_timeseries"
                + " (benchmark_run_id, metric, snapshot_sequence, observed_at, value, state)"
                + " VALUES (1, 'LATENCY_P99', 10, NOW(6), NULL, 'VALID')"))
                .hasMessageContaining("ck_timeseries_state_value");
        assertThatThrownBy(() -> execute("INSERT INTO run_timeseries"
                + " (benchmark_run_id, metric, snapshot_sequence, observed_at, value, state)"
                + " VALUES (1, 'DB_POOL_USAGE', -1, NOW(6), 0, 'VALID')"))
                .hasMessageContaining("ck_timeseries_sequence");
        assertThatThrownBy(() -> execute("INSERT INTO run_timeseries"
                + " (benchmark_run_id, metric, snapshot_sequence, observed_at, value, state)"
                + " VALUES (1, 'LATENCY_P99', 11, NOW(6), 3.2, 'UNAVAILABLE')"))
                .hasMessageContaining("ck_timeseries_state_value");
        assertThatThrownBy(() -> execute("INSERT INTO run_timeseries"
                + " (benchmark_run_id, metric, snapshot_sequence, observed_at, value, state)"
                + " VALUES (1, 'CPU_USAGE', 12, NOW(6), 0.2, 'VALID')"))
                .hasMessageContaining("ck_timeseries_metric");

        execute("INSERT INTO run_timeseries"
                + " (benchmark_run_id, metric, snapshot_sequence, observed_at, value, state)"
                + " VALUES (1, 'DB_POOL_USAGE', 13, NOW(6), 0.2, 'VALID')");
        assertThatThrownBy(() -> execute("INSERT INTO run_timeseries"
                + " (benchmark_run_id, metric, snapshot_sequence, observed_at, value, state)"
                + " VALUES (1, 'DB_POOL_USAGE', 13, NOW(6), 0.3, 'VALID')"))
                .hasMessageContaining("uk_run_metric_seq");
    }

    private static Connection connection() throws Exception {
        return DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
    }

    private static void execute(String sql) throws Exception {
        try (Connection connection = connection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    private static int query(String sql) throws Exception {
        try (Connection connection = connection(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            result.next(); return result.getInt(1);
        }
    }
}
