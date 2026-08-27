package com.kafkick.storage.db.migration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicLong;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

// A 소유 패키지를 테스트에서만 참조한다 — 축 상태 값이 이 enum 의 이름 그대로라는 것이
// 두 모듈에 걸친 계약이라서다. 문자열로 옮겨 적으면 enum 이 바뀌어도 이 테스트는 계속 통과한다.
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AggregateAvailability;

/**
 * analytics_* 네 표의 제약을 <b>실제 MySQL</b> 에 마이그레이션을 적용한 뒤 원시 INSERT 로 검증한다.
 *
 * <p><b>왜 원시 INSERT 인가.</b> 집계 배치는 이 제약을 통과하는 값만 만든다 — 그래서 배치 테스트로는
 * 이 제약들이 한 번도 타지 않는다. 이 제약이 지키는 것은 <b>배치 밖에서 이 표에 쓰는 경로</b>다:
 * 수동 보정, 백필, 그리고 나중에 이 표를 읽고 쓰게 될 다른 코드. 특히 네 상태 합계 불변식은
 * A 쪽 {@code IssuanceStatusAggregate} 생성자와 <b>같은 불변식</b>이라, DB 에서 새면 그 record 가
 * 조립 단계에서 터지고 그때는 어느 집계 회차가 깨뜨렸는지 되짚을 수 없다.
 *
 * <p>Spring 컨텍스트를 띄우지 않는다 — 검증 대상이 스키마뿐이라 JDBC 로 충분하다
 * ({@code IssueAttemptsMigrationTest} 와 같은 이유).
 */
class AnalyticsAggregatesMigrationTest {

    /** testFixtures 는 latest 를 쓴다. 여기서는 운영(compose)과 같은 8.4 로 고정한다. */
    private static final DockerImageName IMAGE = DockerImageName.parse("mysql:8.4");

    /** 회차 id 를 테스트마다 새로 만든다. FK 대상이라 서로 나눠 두면 정리 없이 독립적이다. */
    private static final AtomicLong RUN_SEQUENCE = new AtomicLong(1);

    private static MySQLContainer mysql;

    @BeforeAll
    static void startAndMigrate() throws SQLException {
        mysql = new MySQLContainer(IMAGE)
                .withDatabaseName("app")
                // 제약 판정에 영향을 주는 서버 설정만 운영과 맞춘다.
                .withCommand(
                        "--default-time-zone=+00:00",
                        "--character-set-server=utf8mb4",
                        "--collation-server=utf8mb4_0900_ai_ci",
                        "--default-storage-engine=InnoDB",
                        "--sql-mode=STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,"
                                + "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION");
        mysql.start();

        Flyway.configure()
                .dataSource(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())
                .locations("classpath:db/migration")
                .load()
                .migrate();

        seedCoupon();
    }

    /**
     * 축 표가 {@code coupons} 를 FK 로 문다 — 축 행을 넣으려면 그 회차가 실재해야 한다.
     *
     * <p>그 FK 가 막는 것은 "지워진 회차를 가리키는 고아 집계 행" 이다. 그 상태가 되면 A 의
     * 카탈로그 대조가 실패해 분석 화면이 500 이 되는데, 집계 행만 보고는 원인을 짚을 수 없다.
     */
    private static void seedCoupon() throws SQLException {
        execute("INSERT INTO brands (id, name, category) VALUES (5, '브랜드', '카페')");
        execute("""
                INSERT INTO coupon_templates
                    (id, brand_id, name, policy_type, valid_days, nth_week, day_of_week,
                     start_time, duration_hours, stock_per_occurrence, eligible_grades_mask,
                     active, created_at, updated_at)
                VALUES (5, 5, '템플릿', 'FIXED_AMOUNT', 30, 1, 'MON', '10:00:00', 1, 100, 1, true,
                        '2026-08-01', '2026-08-01')""");
        execute("""
                INSERT INTO coupons
                    (id, template_id, brand_id, name, policy_type, valid_days,
                     eligible_grades_mask, open_at, close_at, status, generated_at, created_at)
                VALUES (900, 5, 5, '회차', 'FIXED_AMOUNT', 30, 1, '2026-08-25', '2026-08-27',
                        'OPEN', '2026-08-24', '2026-08-24')""");
    }

    @AfterAll
    static void stop() {
        if (mysql != null) {
            mysql.stop();
        }
    }

    @Nested
    @DisplayName("마이그레이션 적용")
    class Migration {

        /**
         * 적용 목록 전체를 고정하지 않는다 — 이 표와 무관한 마이그레이션이 늘어도 깨지면 안 된다.
         * A 대역보다 뒤에 왔다는 것만 본다(Flyway 날짜 대역 규약).
         */
        @Test
        @DisplayName("날짜 버전이 A 대역보다 뒤에 성공으로 기록된다")
        void appliedAfterNumericBand() throws SQLException {
            assertThat(query("SELECT success FROM flyway_schema_history WHERE version = '2026082602'"))
                    .isEqualTo("1");
            assertThat(installedRank("2026082602")).isGreaterThan(installedRank("1"));
        }

        @Test
        @DisplayName("네 표가 만들어진다")
        void fourTablesExist() throws SQLException {
            assertThat(query("SELECT COUNT(*) FROM information_schema.tables"
                    + " WHERE table_schema = DATABASE() AND table_name IN"
                    + " ('analytics_runs', 'analytics_daily_issues',"
                    + " 'analytics_hourly_issues', 'analytics_issuance_statuses')"))
                    .isEqualTo("4");
        }
    }

    @Nested
    @DisplayName("analytics_runs — 축 상태와 완료 시각")
    class RunChecks {

        /**
         * A 가 읽는 값은 {@link AggregateAvailability} 의 이름 그대로다. 값을 손으로 적지 않고
         * enum 을 돌린다 — 적어 두면 enum 만 늘어난 상태에서도 계속 통과하고, 실패는 실제 집계에서
         * 처음 나타난다.
         */
        @Test
        @DisplayName("AggregateAvailability 세 값이 모두 축 상태로 적재된다")
        void everyAvailabilityNamePasses() throws SQLException {
            for (AggregateAvailability availability : AggregateAvailability.values()) {
                String completedAt = availability == AggregateAvailability.AVAILABLE
                        ? "'2026-08-26 10:00:00'" : "NULL";
                assertThatCode(() -> insertRun(nextRunId(), "SUCCEEDED", "NULL",
                        availability.name(), completedAt,
                        availability.name(), completedAt,
                        availability.name(), completedAt))
                        .as(availability.name())
                        .doesNotThrowAnyException();
            }
            assertThat(query("SELECT COUNT(DISTINCT monthly_trend_status) FROM analytics_runs"))
                    .isEqualTo(String.valueOf(AggregateAvailability.values().length));
        }

        @Test
        @DisplayName("소문자 축 상태는 거절된다 — 제약이 대소문자 구분 collation 이다")
        void lowerCaseAxisStatusIsRejected() {
            assertThatThrownBy(() -> insertRun(nextRunId(), "SUCCEEDED", "NULL",
                    "available", "'2026-08-26 10:00:00'", "PENDING", "NULL", "PENDING", "NULL"))
                    .hasMessageContaining("ck_analytics_run_monthly_trend");
        }

        @Test
        @DisplayName("AVAILABLE 인데 완료 시각이 없으면 거절된다 — A 의 STALE 판정이 그 시각을 읽는다")
        void availableAxisRequiresCompletedAt() {
            assertThatThrownBy(() -> insertRun(nextRunId(), "SUCCEEDED", "NULL",
                    "AVAILABLE", "NULL", "PENDING", "NULL", "PENDING", "NULL"))
                    .hasMessageContaining("ck_analytics_run_monthly_trend");
        }

        /**
         * 실패한 축이 이전 완료 시각을 들고 있으면 A 는 그것을 <b>정상 관측 시각</b>으로 읽는다.
         * UNAVAILABLE 은 값이 없다는 뜻이라 시각도 없어야 한다.
         */
        @Test
        @DisplayName("AVAILABLE 이 아닌 축은 완료 시각을 가질 수 없다")
        void nonAvailableAxisCannotCarryCompletedAt() {
            assertThatThrownBy(() -> insertRun(nextRunId(), "FAILED", "'monthly'",
                    "UNAVAILABLE", "'2026-08-26 10:00:00'", "PENDING", "NULL", "PENDING", "NULL"))
                    .hasMessageContaining("ck_analytics_run_monthly_trend");
            assertThatThrownBy(() -> insertRun(nextRunId(), "SUCCEEDED", "NULL",
                    "PENDING", "'2026-08-26 10:00:00'", "PENDING", "NULL", "PENDING", "NULL"))
                    .hasMessageContaining("ck_analytics_run_monthly_trend");
        }

        /**
         * 완료 <b>시각</b>과 집계 <b>지점</b>은 다른 값이고, 둘 다 AVAILABLE 과 짝이어야 한다.
         * 지점이 비면 다음 회차가 어디서 이어갈지 모르고, 지점만 있고 상태가 없으면 A 가 안 읽는다.
         */
        @Test
        @DisplayName("AVAILABLE 축은 완료 시각과 집계 지점을 둘 다 가져야 한다")
        void availableAxisRequiresBothMarkers() {
            assertThatThrownBy(() -> execute("INSERT INTO analytics_runs"
                    + " (id, as_of, started_at, status, monthly_trend_status,"
                    + " monthly_trend_completed_at)"
                    + " VALUES (" + nextRunId() + ", '2026-08-26 09:00:00', '2026-08-26 09:00:01',"
                    + " 'SUCCEEDED', 'AVAILABLE', '2026-08-26 10:00:00')"))
                    .as("집계 지점 없이 AVAILABLE")
                    .hasMessageContaining("ck_analytics_run_monthly_trend");
            assertThatThrownBy(() -> execute("INSERT INTO analytics_runs"
                    + " (id, as_of, started_at, status, monthly_trend_aggregated_through)"
                    + " VALUES (" + nextRunId() + ", '2026-08-26 09:00:00', '2026-08-26 09:00:01',"
                    + " 'SUCCEEDED', '2026-08-26 09:00:00')"))
                    .as("PENDING 인데 집계 지점만 있다")
                    .hasMessageContaining("ck_analytics_run_monthly_trend");
        }

        /**
         * 도달하지 못한 지점을 "다 셌다" 로 적으면 그 사이 구간이 <b>영영</b> 건너뛰어진다.
         * 따라잡는 중인 회차가 as_of 를 집계 지점으로 적는 것이 정확히 그 실수다.
         */
        @Test
        @DisplayName("집계 지점은 as_of 를 넘을 수 없다")
        void aggregatedThroughCannotExceedAsOf() {
            assertThatThrownBy(() -> execute("INSERT INTO analytics_runs"
                    + " (id, as_of, started_at, status, monthly_trend_status,"
                    + " monthly_trend_completed_at, monthly_trend_aggregated_through)"
                    + " VALUES (" + nextRunId() + ", '2026-08-26 09:00:00', '2026-08-26 09:00:01',"
                    + " 'SUCCEEDED', 'AVAILABLE', '2026-08-26 10:00:00', '2026-08-26 09:00:01')"))
                    .hasMessageContaining("ck_analytics_run_monthly_trend");
        }

        @Test
        @DisplayName("축 세 개가 서로 다른 상태로 공존한다 — 한 축이 실패해도 나머지는 값을 낸다")
        void axesAreIndependent() throws SQLException {
            long runId = nextRunId();
            insertRun(runId, "FAILED", "'hourly axis timed out'",
                    "AVAILABLE", "'2026-08-26 10:00:00'",
                    "UNAVAILABLE", "NULL",
                    "AVAILABLE", "'2026-08-26 10:00:01'");
            assertThat(query("SELECT hourly_heatmap_status FROM analytics_runs WHERE id = " + runId))
                    .isEqualTo("UNAVAILABLE");
        }

        @Test
        @DisplayName("FAILED 와 사유는 함께여야 한다 — 공백만 있는 사유도 사유가 아니다")
        void failureReasonPairsWithStatus() {
            assertThatThrownBy(() -> insertRun(nextRunId(), "FAILED", "NULL",
                    "PENDING", "NULL", "PENDING", "NULL", "PENDING", "NULL"))
                    .hasMessageContaining("ck_analytics_run_failure");
            assertThatThrownBy(() -> insertRun(nextRunId(), "FAILED", "'   '",
                    "PENDING", "NULL", "PENDING", "NULL", "PENDING", "NULL"))
                    .hasMessageContaining("ck_analytics_run_failure");
            assertThatThrownBy(() -> insertRun(nextRunId(), "SUCCEEDED", "'왜 있지'",
                    "PENDING", "NULL", "PENDING", "NULL", "PENDING", "NULL"))
                    .hasMessageContaining("ck_analytics_run_failure");
        }

        @Test
        @DisplayName("정의되지 않은 회차 상태는 거절된다")
        void unknownRunStatusIsRejected() {
            assertThatThrownBy(() -> insertRun(nextRunId(), "PARTIAL", "NULL",
                    "PENDING", "NULL", "PENDING", "NULL", "PENDING", "NULL"))
                    .hasMessageContaining("ck_analytics_run_status");
        }
    }

    @Nested
    @DisplayName("축 표 — 값과 소속")
    class AxisChecks {

        @Test
        @DisplayName("네 상태 합계가 total_issued 와 다르면 거절된다")
        void stateTotalMustMatch() throws SQLException {
            long runId = succeededRun();
            assertThatThrownBy(() -> insertStatusRow(runId, "'2026-08-26'", 10, 10, 3, 0, 0))
                    .as("합계 13 > total 10")
                    .hasMessageContaining("ck_analytics_status_total");
            assertThatThrownBy(() -> insertStatusRow(runId, "'2026-08-26'", 10, 4, 3, 0, 0))
                    .as("합계 7 < total 10")
                    .hasMessageContaining("ck_analytics_status_total");
            assertThatCode(() -> insertStatusRow(runId, "'2026-08-26'", 10, 4, 3, 2, 1))
                    .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("음수 수량은 거절된다 — 합계만 맞으면 통과하는 구멍을 막는다")
        void negativeCountsAreRejected() throws SQLException {
            long runId = succeededRun();
            assertThatThrownBy(() -> insertStatusRow(runId, "'2026-08-26'", 10, 12, -2, 0, 0))
                    .hasMessageContaining("ck_analytics_status_nonnegative");
        }

        @Test
        @DisplayName("같은 버킷이 회차별로 누적된다 — 상태 축은 최신 회차를 고르는 방식이다")
        void statusRowsAccumulatePerRun() throws SQLException {
            long first = succeededRun();
            long second = succeededRun();
            insertStatusRow(first, "'2026-08-20'", 10, 10, 0, 0, 0);
            insertStatusRow(second, "'2026-08-20'", 10, 7, 3, 0, 0);
            assertThat(query("SELECT COUNT(*) FROM analytics_issuance_statuses"
                    + " WHERE issue_date = '2026-08-20' AND coupon_id = 900"))
                    .isEqualTo("2");
        }

        @Test
        @DisplayName("issued_at 기준 두 축은 버킷당 한 행이다 — 재실행이 행을 늘리지 않는다")
        void issuedAtAxesKeepOneRowPerBucket() throws SQLException {
            long runId = succeededRun();
            execute("INSERT INTO analytics_daily_issues"
                    + " (issue_date, coupon_id, brand_id, issue_count, run_id)"
                    + " VALUES ('2026-08-21', 900, 5, 100, " + runId + ")");
            assertThatThrownBy(() -> execute("INSERT INTO analytics_daily_issues"
                    + " (issue_date, coupon_id, brand_id, issue_count, run_id)"
                    + " VALUES ('2026-08-21', 900, 5, 101, " + runId + ")"))
                    .hasMessageContaining("Duplicate entry");
        }

        @Test
        @DisplayName("시간대는 0~23 만 받는다")
        void hourRangeIsBounded() throws SQLException {
            long runId = succeededRun();
            assertThatThrownBy(() -> execute("INSERT INTO analytics_hourly_issues"
                    + " (issue_date, issue_hour, coupon_id, brand_id, issue_count, run_id)"
                    + " VALUES ('2026-08-21', 24, 900, 5, 1, " + runId + ")"))
                    .hasMessageContaining("ck_analytics_hourly_hour");
            assertThatCode(() -> execute("INSERT INTO analytics_hourly_issues"
                    + " (issue_date, issue_hour, coupon_id, brand_id, issue_count, run_id)"
                    + " VALUES ('2026-08-21', 23, 900, 5, 1, " + runId + ")"))
                    .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("없는 쿠폰 회차를 가리키는 집계 행은 거절된다")
        void aggregateRowsRequireACoupon() throws SQLException {
            long runId = succeededRun();
            assertThatThrownBy(() -> execute("INSERT INTO analytics_daily_issues"
                    + " (issue_date, coupon_id, brand_id, issue_count, run_id)"
                    + " VALUES ('2026-08-23', 999999, 5, 1, " + runId + ")"))
                    .hasMessageContaining("fk_analytics_daily_coupon");
        }

        @Test
        @DisplayName("없는 회차를 가리키는 집계 행은 거절된다")
        void aggregateRowsRequireARun() {
            assertThatThrownBy(() -> execute("INSERT INTO analytics_daily_issues"
                    + " (issue_date, coupon_id, brand_id, issue_count, run_id)"
                    + " VALUES ('2026-08-22', 900, 5, 1, 999999)"))
                    .hasMessageContaining("fk_analytics_daily_run");
        }
    }

    private static long nextRunId() {
        return RUN_SEQUENCE.incrementAndGet();
    }

    /** 축 행이 매달릴 성공 회차 하나. */
    private static long succeededRun() throws SQLException {
        long runId = nextRunId();
        insertRun(runId, "SUCCEEDED", "NULL",
                "AVAILABLE", "'2026-08-26 10:00:00'",
                "AVAILABLE", "'2026-08-26 10:00:00'",
                "AVAILABLE", "'2026-08-26 10:00:00'");
        return runId;
    }

    /** 축이 AVAILABLE 이면 완료 시각과 집계 지점이 둘 다 있어야 한다 — 그 짝을 여기서 맞춰 준다. */
    private static void insertRun(
            long id, String status, String failureReason,
            String monthlyStatus, String monthlyCompletedAt,
            String hourlyStatus, String hourlyCompletedAt,
            String issuanceStatus, String issuanceCompletedAt) throws SQLException {
        execute("INSERT INTO analytics_runs"
                + " (id, as_of, started_at, status, failure_reason,"
                + " monthly_trend_status, monthly_trend_completed_at, monthly_trend_aggregated_through,"
                + " hourly_heatmap_status, hourly_heatmap_completed_at, hourly_heatmap_aggregated_through,"
                + " issuance_status_status, issuance_status_completed_at,"
                + " issuance_status_aggregated_through)"
                + " VALUES (" + id + ", '2026-08-26 09:00:00', '2026-08-26 09:00:01',"
                + " '" + status + "', " + failureReason + ","
                + " '" + monthlyStatus + "', " + monthlyCompletedAt + ", " + through(monthlyStatus) + ","
                + " '" + hourlyStatus + "', " + hourlyCompletedAt + ", " + through(hourlyStatus) + ","
                + " '" + issuanceStatus + "', " + issuanceCompletedAt + ", "
                + through(issuanceStatus) + ")");
    }

    /** as_of(09:00) 이하여야 한다 — 도달하지 못한 지점을 "다 셌다" 로 적을 수 없다. */
    private static String through(String axisStatus) {
        return "AVAILABLE".equals(axisStatus) ? "'2026-08-26 09:00:00'" : "NULL";
    }

    private static void insertStatusRow(
            long runId, String issueDate, long total, long currentlyIssued,
            long used, long cancelled, long expired) throws SQLException {
        execute("INSERT INTO analytics_issuance_statuses"
                + " (issue_date, coupon_id, brand_id, total_issued, currently_issued,"
                + " used, cancelled, expired, observed_at, run_id)"
                + " VALUES (" + issueDate + ", 900, 5, " + total + ", " + currentlyIssued + ","
                + " " + used + ", " + cancelled + ", " + expired + ","
                + " '2026-08-26 10:00:00', " + runId + ")");
    }

    private static Connection connection() throws SQLException {
        return DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
    }

    private static void execute(String sql) throws SQLException {
        try (Connection c = connection(); Statement s = c.createStatement()) {
            s.executeUpdate(sql);
        }
    }

    private static String query(String sql) throws SQLException {
        try (Connection c = connection(); Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private static int installedRank(String version) throws SQLException {
        return Integer.parseInt(query(
                "SELECT installed_rank FROM flyway_schema_history WHERE version = '" + version + "'"));
    }
}
