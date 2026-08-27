package com.kafkick.batch.analytics;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.kafkick.batch.analytics.AnalyticsAggregateReader.DailyRow;
import com.kafkick.core.support.TimeProvider;

/**
 * 집계 배치를 <b>실제 MySQL</b> 에 태운다. 대역만 쓰면 CONVERT_TZ 의 KST 버킷팅, 행 별칭
 * ({@code AS new}) Upsert, {@code MAX_EXECUTION_TIME} 힌트, 드라이버의 시각 변환이
 * 한 줄도 실행되지 않는다 — 리뷰로도 안 잡히는 것들이다.
 *
 * <p>읽기는 <b>SELECT 만 가진 계정</b>으로 나간다. 계정에 주는 표는
 * {@code infra/mysql/obs-grants/allowlist.txt} 를 그대로 읽어 만든다 — 목록 밖 표를 읽는 질의가
 * 들어오면 여기서 MySQL 1142 로 죽는다.
 *
 * <p><b>공용 {@code MySqlContainerConfig} 를 안 쓴다.</b> 그것은 {@code @SpringBootTest} 용
 * {@code @TestConfiguration} 이라 batch 컨텍스트 전체를 띄운다. 여기서 보는 것은 SQL 과 드라이버
 * 동작뿐이라 컨텍스트가 필요 없고, 무엇보다 <b>sql_mode 를 운영과 같게 고정</b>해야 한다.
 * 배선은 {@code AnalyticsWiringTest} 가 그 공용 설정으로 따로 본다.
 *
 * <p><b>이 테스트가 못 보는 것</b> — 실제 배포의 GRANT 절차(compose 의 {@code obs-grants} 서비스와
 * {@code apply.sh})는 {@code ObservationAccountPrivilegeTest} 가 본다. 여기서는 같은 목록으로
 * 만든 계정을 쓸 뿐이다.
 */
class AnalyticsAggregationRunnerTest {

    private static final DockerImageName IMAGE = DockerImageName.parse("mysql:8.4");

    private static final String READ_USER = "analytics_reader";
    private static final String READ_PASSWORD = "reader";

    /** KST 09:00. 기준 시각을 UTC 자정에 두면 KST 날짜 경계가 as_of 안쪽으로 들어온다. */
    private static final Instant AS_OF = Instant.parse("2026-08-26T00:00:00Z");
    private static final Instant LATER_AS_OF = Instant.parse("2026-08-26T06:00:00Z");

    private static final long BRAND_A = 1L;
    private static final long BRAND_B = 2L;
    private static final long COUPON_A1 = 100L;
    private static final long COUPON_B1 = 200L;

    private static MySQLContainer mysql;
    private static SimpleDriverDataSource writeDataSource;
    private static SimpleDriverDataSource readDataSource;
    private static JdbcTemplate writeJdbc;
    private static JdbcTemplate readJdbc;

    private AnalyticsAggregateReader reader;
    private AnalyticsRunStore store;
    private MutableClock clock;

    @BeforeAll
    static void startMySql() {
        mysql = new MySQLContainer(IMAGE)
                .withDatabaseName("app")
                // ⚠️ sql_mode 를 운영과 같게 둔다. compose 의 mysql 은 --sql-mode 를 주지 않아
                //    8.4 **기본값**으로 도는데, 거기에는 ONLY_FULL_GROUP_BY 가 들어 있다.
                //    그것을 뺀 채로 테스트하면 GROUP BY 가 어긋나도 여기서는 초록이고 운영에서만
                //    죽는다 — 이 배치는 질의가 전부 GROUP BY 라 하필 그 자리다.
                .withCommand(
                        "--default-time-zone=+00:00",
                        "--sql-mode=ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,"
                                + "NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION");
        mysql.start();

        Flyway.configure()
                .dataSource(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())
                .locations("classpath:db/migration")
                .load()
                .migrate();

        writeDataSource = dataSource(mysql.getUsername(), mysql.getPassword());
        writeJdbc = new JdbcTemplate(writeDataSource);
        createSelectOnlyReader();
        readDataSource = dataSource(READ_USER, READ_PASSWORD);
        readJdbc = new JdbcTemplate(readDataSource);
    }

    @AfterAll
    static void stop() {
        if (mysql != null) {
            mysql.stop();
        }
    }

    @BeforeEach
    void resetAndSeed() {
        writeJdbc.update("DELETE FROM analytics_daily_issues");
        writeJdbc.update("DELETE FROM analytics_hourly_issues");
        writeJdbc.update("DELETE FROM analytics_issuance_statuses");
        writeJdbc.update("DELETE FROM analytics_runs");
        writeJdbc.update("DELETE FROM issuance_histories");
        writeJdbc.update("DELETE FROM issuances");
        writeJdbc.update("DELETE FROM coupons");
        writeJdbc.update("DELETE FROM coupon_templates");
        writeJdbc.update("DELETE FROM members");
        writeJdbc.update("DELETE FROM brands");
        writeJdbc.update("DELETE FROM grades");

        writeJdbc.update("INSERT INTO grades(code, bit_value) VALUES ('WELCOME', 1)");
        insertBrand(BRAND_A, "브랜드 A");
        insertBrand(BRAND_B, "브랜드 B");
        insertTemplate(10L, BRAND_A);
        insertTemplate(20L, BRAND_B);
        insertCoupon(COUPON_A1, 10L, BRAND_A);
        insertCoupon(COUPON_B1, 20L, BRAND_B);

        // KST(UTC+9) 로 버킷팅했을 때 —
        //   1,2 → 08-25 11시   3 → 08-26 01시(UTC 로는 08-25 다. 날짜 경계가 여기서 갈린다)
        //   4   → 08-25 14시(다른 브랜드)
        insertIssuance(1L, COUPON_A1, "2026-08-25T02:00:00Z", "ISSUED");
        insertIssuance(2L, COUPON_A1, "2026-08-25T02:30:00Z", "ISSUED");
        insertIssuance(3L, COUPON_A1, "2026-08-25T16:00:00Z", "ISSUED");
        insertIssuance(4L, COUPON_B1, "2026-08-25T05:00:00Z", "ISSUED");

        clock = new MutableClock(AS_OF);
        reader = new AnalyticsAggregateReader(readDataSource, properties());
        store = new AnalyticsRunStore(
                writeJdbc, new TransactionTemplate(new JdbcTransactionManager(writeDataSource)));
    }

    private AnalyticsAggregationRunner runner(AnalyticsAggregateReader source) {
        return new AnalyticsAggregationRunner(source, store, properties(), new TimeProvider(clock));
    }

    private static AnalyticsAggregationProperties properties() {
        return properties(200_000, 20);
    }

    private static AnalyticsAggregationProperties properties(int maxWindowRows, int maxSteps) {
        return new AnalyticsAggregationProperties(
                "Asia/Seoul", Duration.ofMinutes(10), Duration.ofSeconds(30),
                maxWindowRows, maxSteps);
    }

    @Test
    @DisplayName("세 축이 KST 버킷으로 집계된다")
    void aggregatesThreeAxes() {
        AnalyticsAggregationResult result = runner(reader).runOnce(AS_OF);

        assertThat(result.succeeded()).isTrue();
        assertThat(dailyCounts()).containsOnly(
                Map.entry("2026-08-25|100", 2L),
                Map.entry("2026-08-26|100", 1L),
                Map.entry("2026-08-25|200", 1L));
        assertThat(hourlyCounts()).containsOnly(
                Map.entry("2026-08-25|11|100", 2L),
                Map.entry("2026-08-26|1|100", 1L),
                Map.entry("2026-08-25|14|200", 1L));
        assertThat(writeJdbc.queryForObject(
                "SELECT SUM(total_issued) FROM analytics_issuance_statuses", Long.class))
                .isEqualTo(4L);
        assertThat(writeJdbc.queryForObject(
                "SELECT SUM(currently_issued) FROM analytics_issuance_statuses", Long.class))
                .isEqualTo(4L);
        assertThat(writeJdbc.queryForObject("SELECT status FROM analytics_runs", String.class))
                .isEqualTo("SUCCEEDED");
    }

    /**
     * 재실행 결정성은 <b>전 구간 재계수</b> 수준에서 본다. 증분 창을 통과시키는 우회 없이,
     * 같은 {@code as_of} 로 같은 값이 나오는지를 직접 묻는다.
     *
     * <p>사이에 끼워 넣는 발급은 {@code as_of} <b>이후</b>지만 <b>같은 버킷</b>에 떨어진다
     * (KST 08-26 버킷은 UTC 08-25 15:00 ~ 08-26 15:00 이라 as_of 를 감싼다). {@code issued_at < as_of}
     * 가 빠지면 이 발급이 두 번째 계수에 섞여 값이 달라진다.
     */
    @Test
    @DisplayName("같은 as_of 재계수는 그 뒤에 들어온 발급에 흔들리지 않는다")
    void recountIsDeterministicForTheSameAsOf() {
        List<DailyRow> before = reader.readDaily(Instant.EPOCH, AS_OF);
        List<AnalyticsAggregateReader.HourlyRow> beforeHourly =
                reader.readHourly(Instant.EPOCH, AS_OF);

        insertIssuance(5L, COUPON_A1, "2026-08-26T01:00:00Z", "ISSUED");

        assertThat(reader.readDaily(Instant.EPOCH, AS_OF)).isEqualTo(before);
        assertThat(reader.readHourly(Instant.EPOCH, AS_OF))
                .as("시간대 축도 같은 기준에서 같은 값을 내야 한다")
                .isEqualTo(beforeHourly);
        // 기준 시각을 뒤로 옮기면 그때는 보인다 — 안 세는 것이 아니라 아직 아닌 것이다.
        assertThat(sumOf(reader.readDaily(Instant.EPOCH, LATER_AS_OF))).isEqualTo(5L);
    }

    @Test
    @DisplayName("안 바뀐 버킷은 새 행도 새 회차 표시도 만들지 않는다")
    void unchangedBucketsAreNotRewritten() {
        long firstRunId = runner(reader).runOnce(AS_OF).runId();
        long statusRowsAfterFirst = countStatusRows();

        clock.set(LATER_AS_OF);
        AnalyticsAggregationResult second = runner(reader).runOnce(LATER_AS_OF);

        assertThat(second.writtenRows()).containsOnly(
                Map.entry(AnalyticsAxis.MONTHLY_TREND, 0),
                Map.entry(AnalyticsAxis.HOURLY_HEATMAP, 0),
                Map.entry(AnalyticsAxis.ISSUANCE_STATUS, 0));
        assertThat(countStatusRows()).isEqualTo(statusRowsAfterFirst);
        assertThat(writeJdbc.queryForObject(
                "SELECT COUNT(DISTINCT run_id) FROM analytics_daily_issues", Long.class))
                .isEqualTo(1L);
        assertThat(writeJdbc.queryForObject(
                "SELECT DISTINCT run_id FROM analytics_daily_issues", Long.class))
                .isEqualTo(firstRunId);
    }

    @Test
    @DisplayName("상태가 바뀌면 그 버킷만 새 회차로 다시 실리고 observed_at 도 함께 간다")
    void statusAxisRecordsObservedAtWithTheValue() {
        runner(reader).runOnce(AS_OF);
        LocalDateTime firstObservedAt = writeJdbc.queryForObject(
                "SELECT observed_at FROM analytics_issuance_statuses"
                        + " WHERE issue_date = '2026-08-25' AND coupon_id = 100",
                LocalDateTime.class);

        // 발급 1이 사용된다. 이력 한 행이 증분 대상 선택의 유일한 근거다.
        writeJdbc.update("UPDATE issuances SET status = 'USED' WHERE id = 1");
        insertHistory(1L, "USE", "2026-08-26T03:00:00Z");

        clock.set(LATER_AS_OF);
        runner(reader).runOnce(LATER_AS_OF);

        Map<String, Object> latest = writeJdbc.queryForMap(
                "SELECT used, currently_issued, observed_at FROM analytics_issuance_statuses"
                        + " WHERE issue_date = '2026-08-25' AND coupon_id = 100"
                        + " ORDER BY run_id DESC LIMIT 1");
        assertThat(latest.get("used")).isEqualTo(1L);
        assertThat(latest.get("currently_issued")).isEqualTo(1L);
        assertThat((LocalDateTime) latest.get("observed_at")).isAfter(firstObservedAt);
        // 안 바뀐 버킷(08-26·회차 100, 08-25·회차 200)은 여전히 첫 회차 행 하나뿐이다.
        assertThat(countStatusRows()).isEqualTo(4L);
    }

    @Test
    @DisplayName("축 하나가 실패해도 나머지 축은 AVAILABLE 로 남고 실패한 축은 완료 시각을 갱신하지 않는다")
    void failedAxisDoesNotTouchTheOthersOrItsCompletedAt() {
        runner(reader).runOnce(AS_OF);
        LocalDateTime firstHourlyCompletedAt = writeJdbc.queryForObject(
                "SELECT hourly_heatmap_completed_at FROM analytics_runs", LocalDateTime.class);

        writeJdbc.update("UPDATE issuances SET status = 'USED' WHERE id = 1");
        insertHistory(1L, "USE", "2026-08-26T03:00:00Z");
        clock.set(LATER_AS_OF);
        AnalyticsAggregationResult second = runner(failingHourly()).runOnce(LATER_AS_OF);

        assertThat(second.succeeded()).isFalse();
        assertThat(second.failedAxes()).containsOnlyKeys(AnalyticsAxis.HOURLY_HEATMAP);
        Map<String, Object> run = writeJdbc.queryForMap(
                "SELECT status, failure_reason, monthly_trend_status, hourly_heatmap_status,"
                        + " hourly_heatmap_completed_at, issuance_status_status"
                        + " FROM analytics_runs WHERE id = " + second.runId());
        assertThat(run.get("status")).isEqualTo("FAILED");
        assertThat((String) run.get("failure_reason")).contains("HOURLY_HEATMAP");
        assertThat(run.get("monthly_trend_status")).isEqualTo("AVAILABLE");
        assertThat(run.get("issuance_status_status")).isEqualTo("AVAILABLE");
        assertThat(run.get("hourly_heatmap_status")).isEqualTo("UNAVAILABLE");
        assertThat(run.get("hourly_heatmap_completed_at")).isNull();
        // 앞 회차의 완료 시각은 그대로다 — 실패가 과거의 성공을 지우지 않는다.
        assertThat(firstHourlyCompletedAt).isNotNull();
    }

    /**
     * 축 기준 시각이 회차 단위였다면 여기서 실패한 축의 변경분이 <b>영영</b> 건너뛰어진다.
     * 실패한 다음 회차가 그 구간을 다시 훑는지 본다.
     */
    @Test
    @DisplayName("실패한 축은 다음 회차가 그 구간을 다시 훑는다")
    void nextRunRescansTheWindowOfTheFailedAxis() {
        runner(reader).runOnce(AS_OF);

        insertIssuance(5L, COUPON_A1, "2026-08-26T01:00:00Z", "ISSUED");
        clock.set(LATER_AS_OF);
        runner(failingHourly()).runOnce(LATER_AS_OF);
        assertThat(hourlyCounts()).doesNotContainKey("2026-08-26|10|100");

        clock.set(Instant.parse("2026-08-26T07:00:00Z"));
        AnalyticsAggregationResult third =
                runner(reader).runOnce(Instant.parse("2026-08-26T07:00:00Z"));
        assertThat(hourlyCounts()).containsEntry("2026-08-26|10|100", 1L);
        // 그러면서도 <b>성공했던 축</b>은 그 구간을 다시 안 훑는다. 기준 시각이 회차 단위였다면
        // 한 축의 실패가 나머지 두 축의 일까지 통째로 되돌린다.
        assertThat(third.writtenRows().get(AnalyticsAxis.MONTHLY_TREND))
                .as("월별 축은 앞 회차에서 이미 성공했다")
                .isZero();
    }

    /**
     * <b>첫 회차가 스스로 못 빠져나오는 함정</b>을 막는다.
     *
     * <p>집계 지점이 없으면 창이 이력 전체가 된다. 실측으로 300만 행에서 그 한 문장이 4.4초라
     * 4초 상한을 넘고, 넘으면 축이 AVAILABLE 을 못 받아 집계 지점이 그대로 남고, 다음 회차도
     * 같은 창으로 또 죽는다.
     *
     * <p>여기서는 창을 <b>이력 2행</b>으로 좁혀 같은 구조를 재현한다. 걸음이 나뉘는지, 그리고
     * 나뉘어도 최종 값이 한 번에 센 것과 같은지를 본다.
     */
    @Test
    @DisplayName("첫 회차가 이력 전체를 한 창에 넣지 않고 걸음으로 나눠 따라잡는다")
    void firstRunCatchesUpInBoundedSteps() {
        java.util.concurrent.atomic.AtomicInteger dailyReads =
                new java.util.concurrent.atomic.AtomicInteger();
        AnalyticsAggregateReader counting =
                new AnalyticsAggregateReader(readDataSource, properties()) {
                    @Override
                    public List<DailyRow> readDaily(Instant since, Instant asOf) {
                        dailyReads.incrementAndGet();
                        return super.readDaily(since, asOf);
                    }
                };
        AnalyticsAggregationRunner stepping = new AnalyticsAggregationRunner(
                counting, store, properties(2, 20), new TimeProvider(clock));

        stepping.runOnce(AS_OF);

        assertThat(dailyReads.get())
                .as("이력 4행을 2행씩 나눠 읽어야 한다 — 한 번에 읽으면 창을 안 자른 것이다")
                .isGreaterThan(1);
        assertThat(dailyCounts()).containsOnly(
                Map.entry("2026-08-25|100", 2L),
                Map.entry("2026-08-26|100", 1L),
                Map.entry("2026-08-25|200", 1L));
        // 걸음마다 집계 지점이 전진해 마지막에는 as_of 에 닿는다.
        assertThat(writeJdbc.queryForObject(
                "SELECT monthly_trend_aggregated_through FROM analytics_runs", LocalDateTime.class))
                .isEqualTo(LocalDateTime.ofInstant(AS_OF, ZoneOffset.UTC));
    }

    /**
     * 만료 일괄 처리 직후를 흉내 낸다.
     *
     * <p>만료는 이력을 대량으로 남기지만 <b>발급 수를 바꾸지 않는다.</b> 그런데도 발급 수 두 축이
     * 그 버킷을 재계수하면, 재계수 한 건이 회차 전량 스캔이라 그대로 상한을 넘긴다.
     *
     * <p>상태 축은 반대다 — 정확히 그 전이를 세는 축이라 반드시 다시 세야 한다.
     */
    @Test
    @DisplayName("상태만 바뀐 버킷은 발급 수 두 축을 다시 세지 않고 상태 축만 갱신한다")
    void statusOnlyChangesSkipTheIssuedAtAxes() {
        runner(reader).runOnce(AS_OF);
        long dailyRunId = writeJdbc.queryForObject(
                "SELECT run_id FROM analytics_daily_issues"
                        + " WHERE issue_date = '2026-08-25' AND coupon_id = 100", Long.class);

        writeJdbc.update("UPDATE issuances SET status = 'EXPIRED' WHERE id IN (1, 2)");
        insertHistory(1L, "EXPIRE", "2026-08-26T03:00:00Z");
        insertHistory(2L, "EXPIRE", "2026-08-26T03:00:01Z");
        clock.set(LATER_AS_OF);
        AnalyticsAggregationResult second = runner(reader).runOnce(LATER_AS_OF);

        assertThat(second.writtenRows().get(AnalyticsAxis.MONTHLY_TREND))
                .as("발급 수는 ISSUE 로만 변한다")
                .isZero();
        assertThat(second.writtenRows().get(AnalyticsAxis.HOURLY_HEATMAP)).isZero();
        assertThat(writeJdbc.queryForObject(
                "SELECT run_id FROM analytics_daily_issues"
                        + " WHERE issue_date = '2026-08-25' AND coupon_id = 100", Long.class))
                .as("안 바뀐 버킷은 회차 표시도 그대로다")
                .isEqualTo(dailyRunId);
        assertThat(second.writtenRows().get(AnalyticsAxis.ISSUANCE_STATUS))
                .as("상태 축은 그 전이를 세는 축이라 반드시 갱신된다")
                .isPositive();
        assertThat(writeJdbc.queryForObject(
                "SELECT expired FROM analytics_issuance_statuses"
                        + " WHERE issue_date = '2026-08-25' AND coupon_id = 100"
                        + " ORDER BY run_id DESC LIMIT 1", Long.class))
                .isEqualTo(2L);
    }

    /**
     * 걸음 <b>1이 성공한 뒤</b> 걸음 2가 실패하는 경우다.
     *
     * <p>그 축은 이미 {@code completed_at}·{@code aggregated_through} 가 채워져 있다. 거기에
     * {@code status='UNAVAILABLE}' 만 덮어쓰면 {@code ck_analytics_run_*} 의 짝 조건에 걸려 UPDATE 가
     * 터지고, 그 예외는 조용히 삼켜진다 — 실패를 기록하려던 코드가 아무것도 안 하고 로그만 남긴다.
     *
     * <p><b>축을 UNAVAILABLE 로 되돌리지 않는 것이 맞다.</b> 걸음 1까지는 실제로 집계됐고,
     * 되돌리려면 집계 지점까지 함께 비워야 해서(CHECK) 따라잡은 진행분이 사라진다. 그래서 이
     * 테스트는 "AVAILABLE 로 남는가" 가 아니라 <b>실패 표시가 조용히 터지지 않는가</b>를 본다.
     */
    @Test
    @DisplayName("걸음 1 성공 뒤의 실패에서 축 표시가 조용히 터지지 않는다")
    void partialAxisFailureDoesNotBlowUpBookkeeping() {
        AnalyticsAggregationProperties properties = new AnalyticsAggregationProperties(
                "Asia/Seoul", Duration.ofMinutes(10), Duration.ofSeconds(30), 2, 20);
        java.util.concurrent.atomic.AtomicInteger calls =
                new java.util.concurrent.atomic.AtomicInteger();
        AnalyticsAggregateReader failsOnSecondStep =
                new AnalyticsAggregateReader(readDataSource, properties) {
                    @Override
                    public List<HourlyRow> readHourly(Instant since, Instant asOf) {
                        if (calls.incrementAndGet() >= 2) {
                            throw new IllegalStateException("두 번째 걸음 실패(테스트)");
                        }
                        return super.readHourly(since, asOf);
                    }
                };

        AnalyticsAggregationResult result = new AnalyticsAggregationRunner(
                failsOnSecondStep, store, properties, new TimeProvider(clock)).runOnce(AS_OF);

        assertThat(result.failedAxes()).containsKey(AnalyticsAxis.HOURLY_HEATMAP);
        Map<String, Object> run = writeJdbc.queryForMap(
                "SELECT status, hourly_heatmap_status, hourly_heatmap_aggregated_through,"
                        + " hourly_heatmap_completed_at FROM analytics_runs");
        assertThat(run.get("status")).isEqualTo("FAILED");
        // 걸음 1까지는 실제로 집계됐다 — 되돌리면 CHECK 때문에 집계 지점까지 비워야 하고,
        // 그러면 따라잡은 진행분이 사라진다. [A 확정] 회차가 FAILED 여도 축은 쓸 수 있다.
        assertThat(run.get("hourly_heatmap_status")).isEqualTo("AVAILABLE");
        assertThat(run.get("hourly_heatmap_aggregated_through")).isNotNull();
        assertThat(run.get("hourly_heatmap_completed_at")).isNotNull();
        // 그 지점은 as_of 에 못 미친다 — 부분 성공이라는 사실이 값으로 남아야 한다.
        assertThat((LocalDateTime) run.get("hourly_heatmap_aggregated_through"))
                .isBefore(LocalDateTime.ofInstant(AS_OF, ZoneOffset.UTC));
    }

    /**
     * 거둔 회차가 <b>살아 있었을</b> 때를 본다.
     *
     * <p>겹쳐 뜬 다른 컨테이너가 이 회차를 "마감 안 됨" 으로 보고 FAILED + 사유를 적어 버릴 수
     * 있다. 그 뒤 이 회차가 정상적으로 끝나면 SUCCEEDED 로 마감하는데, 사유를 안 지우면
     * {@code ck_analytics_run_failure}(FAILED ↔ 사유 있음)에 걸려 <b>마감 자체가 터진다.</b>
     */
    @Test
    @DisplayName("성공 마감은 남아 있던 실패 사유를 지운다")
    void successfulCloseClearsAnyLeftoverReason() {
        long runId = store.openRun(AS_OF, AS_OF);
        writeJdbc.update("UPDATE analytics_runs SET status = 'FAILED',"
                + " failure_reason = '앞선 회차가 마감되지 않았다(프로세스 종료 추정)' WHERE id = ?", runId);

        store.closeRun(runId, Map.of());

        Map<String, Object> run = writeJdbc.queryForMap(
                "SELECT status, failure_reason FROM analytics_runs WHERE id = " + runId);
        assertThat(run.get("status")).isEqualTo("SUCCEEDED");
        assertThat(run.get("failure_reason")).isNull();
    }

    /**
     * 거두기는 <b>죽은</b> 회차만 대상이다. 방금 열린 회차까지 거두면, 겹쳐 뜬 컨테이너가 서로의
     * 살아 있는 회차를 실패로 적는다.
     */
    @Test
    @DisplayName("방금 열린 회차는 거두지 않는다")
    void reapingSparesRunsThatCouldStillBeAlive() {
        long alive = store.openRun(AS_OF, AS_OF);

        store.openRun(AS_OF, AS_OF);

        assertThat(writeJdbc.queryForObject(
                "SELECT status FROM analytics_runs WHERE id = " + alive, String.class))
                .isEqualTo("IN_PROGRESS");

        writeJdbc.update("UPDATE analytics_runs SET started_at = ? WHERE id = ?",
                LocalDateTime.ofInstant(AS_OF.minus(Duration.ofDays(1)), ZoneOffset.UTC), alive);
        store.openRun(AS_OF, AS_OF);

        assertThat(writeJdbc.queryForObject(
                "SELECT status FROM analytics_runs WHERE id = " + alive, String.class))
                .as("오래 매달린 회차는 거둔다")
                .isEqualTo("FAILED");
    }

    /**
     * <b>한 걸음이 실제로 훑는 이력 행 수</b>를 잰다.
     *
     * <p>걸음 수와 하한만 보는 것으로는 부족하다 — 끝점을 다시 잡는 폴백 경로에서 하한이 그대로
     * 남으면, 창은 {@code max-window-rows} 를 그대로 넘긴다. 4초 예산은 <b>창에 든 행 수</b>로
     * 잰 값이라, 재야 할 것은 그 숫자다.
     *
     * <p>겹쳐 훑기 구간(lag)에 이미 창 크기만큼 이력이 있는 상태를 만들어 폴백을 태운다.
     */
    @Test
    @DisplayName("한 걸음이 훑는 이력이 창 상한을 넘지 않는다")
    void everyStepStaysWithinTheRowBudget() {
        int maxWindowRows = 1;
        Duration lag = Duration.ofHours(2);
        // lag 구간(22:00~00:00)과 그 뒤 구간에 각각 이력을 둔다.
        insertIssuance(5L, COUPON_A1, "2026-08-25T23:00:00Z", "ISSUED");
        insertIssuance(6L, COUPON_A1, "2026-08-25T23:30:00Z", "ISSUED");
        runner(reader).runOnce(AS_OF);
        insertIssuance(7L, COUPON_A1, "2026-08-26T01:00:00Z", "ISSUED");
        insertIssuance(8L, COUPON_A1, "2026-08-26T02:00:00Z", "ISSUED");

        List<Instant[]> windows = new java.util.ArrayList<>();
        AnalyticsAggregationProperties properties = new AnalyticsAggregationProperties(
                "Asia/Seoul", lag, Duration.ofSeconds(30), maxWindowRows, 20);
        AnalyticsAggregateReader recording =
                new AnalyticsAggregateReader(readDataSource, properties) {
                    @Override
                    public List<DailyRow> readDaily(Instant since, Instant asOf) {
                        windows.add(new Instant[] {since, asOf});
                        return super.readDaily(since, asOf);
                    }
                };
        clock.set(LATER_AS_OF);
        new AnalyticsAggregationRunner(recording, store, properties, new TimeProvider(clock))
                .runOnce(LATER_AS_OF);

        assertThat(windows).isNotEmpty();
        for (Instant[] window : windows) {
            Long walked = writeJdbc.queryForObject(
                    "SELECT COUNT(*) FROM issuance_histories WHERE created_at > ? AND created_at <= ?",
                    Long.class,
                    LocalDateTime.ofInstant(window[0], ZoneOffset.UTC),
                    LocalDateTime.ofInstant(window[1], ZoneOffset.UTC));
            assertThat(walked)
                    .as("창 " + window[0] + " ~ " + window[1] + " 가 상한 " + maxWindowRows + " 를 넘었다")
                    .isLessThanOrEqualTo((long) maxWindowRows);
        }
    }

    /**
     * <b>새로 볼 구간이 없는 걸음</b>도 예산 안에 있어야 한다.
     *
     * <p>이미 {@code as_of} 까지 센 상태에서 같은 기준으로 다시 돌면, 끝점을 고를 여지가 없고
     * 늦은 커밋 겹쳐 훑기만 남는다. 그때 하한을 그대로 두면 창이 <b>행 수가 아니라 lag 로만</b>
     * 정해진다 — 이 배치의 전제(10분에 100만 행)면 그 한 구간이 기본 상한과 맞먹는다.
     */
    @Test
    @DisplayName("새 구간이 없는 걸음도 창이 행 수로 묶인다")
    void theOverlapOnlyStepAlsoStaysWithinTheRowBudget() {
        int maxWindowRows = 1;
        runner(reader).runOnce(AS_OF);

        List<Instant[]> windows = new java.util.ArrayList<>();
        AnalyticsAggregationProperties properties = new AnalyticsAggregationProperties(
                "Asia/Seoul", Duration.ofDays(30), Duration.ofSeconds(30), maxWindowRows, 20);
        AnalyticsAggregateReader recording =
                new AnalyticsAggregateReader(readDataSource, properties) {
                    @Override
                    public List<DailyRow> readDaily(Instant since, Instant asOf) {
                        windows.add(new Instant[] {since, asOf});
                        return super.readDaily(since, asOf);
                    }
                };
        // 같은 기준으로 다시 — 집계 지점이 이미 as_of 라 새 구간이 없다.
        new AnalyticsAggregationRunner(recording, store, properties, new TimeProvider(clock))
                .runOnce(AS_OF);

        assertThat(windows).hasSize(1);
        Long walked = writeJdbc.queryForObject(
                "SELECT COUNT(*) FROM issuance_histories WHERE created_at > ? AND created_at <= ?",
                Long.class,
                LocalDateTime.ofInstant(windows.get(0)[0], ZoneOffset.UTC),
                LocalDateTime.ofInstant(windows.get(0)[1], ZoneOffset.UTC));
        assertThat(walked)
                .as("lag 를 30일로 열어 둬도 창은 상한 안이어야 한다")
                .isLessThanOrEqualTo((long) maxWindowRows);
    }

    /**
     * <b>배포 중 두 컨테이너가 겹치는 순간</b>을 흉내 낸다.
     *
     * <p>구·신 컨테이너가 잠깐 같이 떠서 각자 회차를 연다. 늦게 열린 쪽이 <b>덜 따라잡은</b> 채로
     * 쓰면, 회차 번호는 그쪽이 더 커서 상태 축에서 최신이 되고 발급 수 두 축은 그냥 덮어써진다.
     * 재계수가 {@code issued_at <= 기준} 이라 값이 <b>작아진 채로</b> 남고 되돌아오지 않는다.
     *
     * <p>여기서는 "이미 지나간 지점을 못 본 회차" 를 만들어 같은 상황을 재현한다 —
     * 기준 시각 조회가 옛 값을 돌려주는 회차다. {@code requireNotGoingBackwards} 는 자기가 읽은
     * 값만 보므로 이 경우를 못 잡는다.
     */
    @Test
    @DisplayName("덜 따라잡은 회차가 이미 앞서 간 집계를 덮어쓰지 못한다")
    void aLaggingConcurrentRunCannotOverwriteAheadResults() {
        insertIssuance(5L, COUPON_A1, "2026-08-26T01:00:00Z", "ISSUED");
        clock.set(LATER_AS_OF);
        runner(reader).runOnce(LATER_AS_OF);
        Map<String, Long> ahead = dailyCounts();
        assertThat(ahead).containsEntry("2026-08-26|100", 2L);

        // 겹쳐 뜬 다른 컨테이너: 기준 시각을 못 보고 과거부터 다시 센다.
        AnalyticsRunStore stale = new AnalyticsRunStore(
                writeJdbc, new TransactionTemplate(new JdbcTransactionManager(writeDataSource))) {
            @Override
            public Instant watermark(AnalyticsAxis axis) {
                return Instant.EPOCH;
            }
        };
        clock.set(AS_OF);
        AnalyticsAggregationResult lagging = new AnalyticsAggregationRunner(
                reader, stale, properties(), new TimeProvider(clock)).runOnce(AS_OF);

        assertThat(dailyCounts())
                .as("덜 따라잡은 회차가 발급 수를 줄이면 안 된다")
                .isEqualTo(ahead);
        assertThat(lagging.succeeded())
                .as("덮어쓰기를 막았다면 그 회차는 실패로 드러나야 한다")
                .isFalse();
        assertThat(writeJdbc.queryForObject(
                "SELECT SUM(total_issued) FROM analytics_issuance_statuses s"
                        + " JOIN (SELECT issue_date, coupon_id, MAX(run_id) AS run_id"
                        + "       FROM analytics_issuance_statuses GROUP BY issue_date, coupon_id) l"
                        + "   ON l.issue_date = s.issue_date AND l.coupon_id = s.coupon_id"
                        + "  AND l.run_id = s.run_id", Long.class))
                .as("상태 축도 버킷별 최신 회차 선택에서 줄면 안 된다")
                .isEqualTo(5L);
    }

    /**
     * <b>한 걸음이 읽는 창은 max-window-rows 를 넘지 않아야 한다.</b>
     *
     * <p>끝점은 커서부터 N행을 세어 잡는데 조회 하한이 {@code 커서 − lag} 면, 실제로 읽는 창은
     * N + (lag 안의 행) 이다. 부하 구간에서는 그 둘이 비슷해서 창이 <b>두 배</b>가 되고,
     * 창 크기를 재서 정한 4초 예산의 근거가 무너진다. 게다가 <b>걸음마다</b> 그 폭을 다시 읽는다.
     *
     * <p>늦은 커밋을 겹쳐 훑는 것은 <b>회차의 첫 걸음</b>에서 한 번이면 된다 — 뒤 걸음이 보는 구간은
     * 이 회차가 방금 읽은 곳이라 다시 볼 이유가 없다. 그래서 걸음들은 서로 <b>맞닿아야</b> 한다.
     */
    @Test
    @DisplayName("걸음 창은 서로 겹치지 않는다 — lag 는 회차의 첫 걸음에만 든다")
    void onlyTheFirstStepOverlapsByTheLag() {
        Duration lag = Duration.ofHours(2);
        runner(reader).runOnce(AS_OF);
        insertIssuance(5L, COUPON_A1, "2026-08-26T01:00:00Z", "ISSUED");
        insertIssuance(6L, COUPON_A1, "2026-08-26T02:00:00Z", "ISSUED");
        insertIssuance(7L, COUPON_A1, "2026-08-26T03:00:00Z", "ISSUED");

        List<Instant[]> windows = new java.util.ArrayList<>();
        AnalyticsAggregationProperties properties = new AnalyticsAggregationProperties(
                "Asia/Seoul", lag, Duration.ofSeconds(30), 1, 20);
        AnalyticsAggregateReader recording =
                new AnalyticsAggregateReader(readDataSource, properties) {
                    @Override
                    public List<DailyRow> readDaily(Instant since, Instant asOf) {
                        windows.add(new Instant[] {since, asOf});
                        return super.readDaily(since, asOf);
                    }
                };
        clock.set(LATER_AS_OF);
        new AnalyticsAggregationRunner(recording, store, properties, new TimeProvider(clock))
                .runOnce(LATER_AS_OF);

        assertThat(windows).hasSizeGreaterThan(1);
        assertThat(windows.get(0)[0])
                .as("첫 걸음만 lag 만큼 되돌려 늦은 커밋을 겹쳐 훑는다")
                .isEqualTo(AS_OF.minus(lag));
        for (int i = 1; i < windows.size(); i++) {
            assertThat(windows.get(i)[0])
                    .as("걸음 " + i + " 의 하한은 앞 걸음의 끝점이어야 한다(겹치면 창이 커진다)")
                    .isEqualTo(windows.get(i - 1)[1]);
        }
    }

    /**
     * <b>lag 가 걸음 자르기를 삼키면 안 된다.</b>
     *
     * <p>끝점을 {@code cursor − lag} 부터 세면, lag 구간 안에 이미 창 크기만큼의 이력이 있을 때
     * 끝점이 커서를 못 넘는다. 그러면 자르기를 포기하고 {@code as_of} 로 점프한다 — 그리고 그
     * 조건은 <b>부하 구간에서 항상 참이다</b>(이 코드의 전제가 30분에 300만 건, 즉 10분에 100만 행).
     * 즉 창 자르기가 정작 필요한 구간에서만 통째로 꺼진다.
     *
     * <p>여기서는 lag 를 2시간으로 키워 같은 구조를 만든다 — 시드 이력이 그 안에 다 들어온다.
     * 끝점의 하한이 커서면 걸음 수가 lag 와 무관해야 한다.
     */
    @Test
    @DisplayName("lag 를 키워도 걸음 수가 줄지 않는다 — 끝점의 하한은 커서다")
    void watermarkLagDoesNotSwallowTheStepWindow() {
        assertThat(dailyReadsWith(Duration.ofHours(2)))
                .as("lag 가 창을 삼키면 as_of 로 점프해 걸음이 사라진다")
                .isEqualTo(dailyReadsWith(Duration.ofMinutes(10)));
    }

    /** 같은 시드를 주고 lag 만 바꿔 걸음 수(=집계 조회 횟수)를 센다. */
    private int dailyReadsWith(Duration lag) {
        writeJdbc.update("DELETE FROM analytics_daily_issues");
        writeJdbc.update("DELETE FROM analytics_hourly_issues");
        writeJdbc.update("DELETE FROM analytics_issuance_statuses");
        writeJdbc.update("DELETE FROM analytics_runs");
        java.util.concurrent.atomic.AtomicInteger reads =
                new java.util.concurrent.atomic.AtomicInteger();
        AnalyticsAggregationProperties properties = new AnalyticsAggregationProperties(
                "Asia/Seoul", lag, Duration.ofSeconds(30), 2, 20);
        AnalyticsAggregateReader counting =
                new AnalyticsAggregateReader(readDataSource, properties) {
                    @Override
                    public List<DailyRow> readDaily(Instant since, Instant asOf) {
                        reads.incrementAndGet();
                        return super.readDaily(since, asOf);
                    }
                };
        new AnalyticsAggregationRunner(counting, store, properties, new TimeProvider(clock))
                .runOnce(AS_OF);
        return reads.get();
    }

    /**
     * 걸음 수를 다 쓰면 <b>거기까지만</b> 남고, 다음 회차가 이어받는다.
     *
     * <p>집계 지점을 {@code as_of} 로 적으면 도달하지 못한 구간을 "다 셌다" 로 읽어 영영 건너뛴다.
     */
    @Test
    @DisplayName("걸음 수를 다 써도 진행분은 남고 다음 회차가 이어받는다")
    void partialCatchUpIsKeptAndResumed() {
        AnalyticsAggregationRunner oneStep = new AnalyticsAggregationRunner(
                reader, store, properties(1, 1), new TimeProvider(clock));

        oneStep.runOnce(AS_OF);

        LocalDateTime firstThrough = writeJdbc.queryForObject(
                "SELECT monthly_trend_aggregated_through FROM analytics_runs", LocalDateTime.class);
        assertThat(firstThrough)
                .as("한 걸음만 밟았으니 as_of 에 못 닿는다")
                .isBefore(LocalDateTime.ofInstant(AS_OF, ZoneOffset.UTC));
        assertThat(writeJdbc.queryForObject(
                "SELECT monthly_trend_status FROM analytics_runs", String.class))
                .as("따라잡는 중이어도 그 지점까지의 값은 쓸 수 있다")
                .isEqualTo("AVAILABLE");

        // 다음 회차가 남은 구간을 이어받는다.
        AnalyticsAggregationRunner rest = new AnalyticsAggregationRunner(
                reader, store, properties(200_000, 20), new TimeProvider(clock));
        rest.runOnce(AS_OF);

        assertThat(dailyCounts()).containsOnly(
                Map.entry("2026-08-25|100", 2L),
                Map.entry("2026-08-26|100", 1L),
                Map.entry("2026-08-25|200", 1L));
    }

    /**
     * 브랜드별 인덱스({@code ix_analytics_daily_brand})의 존재 이유가 이 컬럼인데, Upsert 의
     * UPDATE 절에서 빠져 있으면 <b>최초 INSERT 값에 영구히 고정</b>된다. 회차의 소속 브랜드가
     * 교정돼도 집계 표만 옛 브랜드에 남아, 브랜드별 조회가 조용히 어긋난다.
     */
    @Test
    @DisplayName("회차의 브랜드가 바뀌면 다시 센 행의 brand_id 도 따라간다")
    void brandIdFollowsTheCouponOnRecount() {
        runner(reader).runOnce(AS_OF);
        assertThat(writeJdbc.queryForObject(
                "SELECT brand_id FROM analytics_daily_issues"
                        + " WHERE issue_date = '2026-08-26' AND coupon_id = 100", Long.class))
                .isEqualTo(BRAND_A);

        writeJdbc.update("UPDATE coupons SET brand_id = ? WHERE id = ?", BRAND_B, COUPON_A1);
        // 그 버킷을 다시 세게 만든다 — 발급 수 축은 ISSUE 이벤트로만 대상이 된다.
        insertIssuance(5L, COUPON_A1, "2026-08-26T01:00:00Z", "ISSUED");
        clock.set(LATER_AS_OF);
        runner(reader).runOnce(LATER_AS_OF);

        assertThat(writeJdbc.queryForObject(
                "SELECT brand_id FROM analytics_daily_issues"
                        + " WHERE issue_date = '2026-08-26' AND coupon_id = 100", Long.class))
                .isEqualTo(BRAND_B);
    }

    /**
     * 시계가 뒤로 조정되면(NTP) 기준 시각이 이미 센 지점보다 이르다. 그대로 다시 세면
     * {@code issued_at < as_of} 때문에 <b>더 작은 값</b>이 더 큰 run_id 를 달고 최신이 된다 —
     * 화면 발급 수가 조용히 줄고 되돌아오지 않는다.
     */
    @Test
    @DisplayName("as_of 가 뒤로 가면 그 축을 건드리지 않고 사유를 남긴다")
    void backwardAsOfDoesNotShrinkStoredCounts() {
        runner(reader).runOnce(LATER_AS_OF);
        Map<String, Long> before = dailyCounts();

        AnalyticsAggregationResult backwards = runner(reader).runOnce(AS_OF);

        assertThat(backwards.succeeded()).isFalse();
        assertThat(backwards.failedAxes()).containsOnlyKeys(
                AnalyticsAxis.MONTHLY_TREND, AnalyticsAxis.HOURLY_HEATMAP,
                AnalyticsAxis.ISSUANCE_STATUS);
        assertThat(dailyCounts()).as("값이 줄어들면 안 된다").isEqualTo(before);
        assertThat(writeJdbc.queryForObject(
                "SELECT failure_reason FROM analytics_runs ORDER BY id DESC LIMIT 1", String.class))
                .contains("as_of");
    }

    /**
     * A 가 할 조회를 여기서 재현한다 — 저장 단위(발급일)와 조회 단위(임의 기간)가 다르므로,
     * "버킷별 최신 회차만 골라 합산" 이 실제 발급 수와 맞는지는 저장 쪽에서 확인해 둬야 한다.
     */
    @Test
    @DisplayName("버킷별 최신 회차 선택이 임의 기간 합산에서 실제 발급과 맞는다")
    void latestRunPerBucketSumsToReality() {
        runner(reader).runOnce(AS_OF);
        writeJdbc.update("UPDATE issuances SET status = 'USED' WHERE id = 1");
        insertHistory(1L, "USE", "2026-08-26T03:00:00Z");
        writeJdbc.update("UPDATE issuances SET status = 'CANCELLED' WHERE id = 4");
        insertHistory(4L, "CANCEL", "2026-08-26T03:10:00Z");
        clock.set(LATER_AS_OF);
        runner(reader).runOnce(LATER_AS_OF);

        // 시드 전 구간(08-25 ~ 08-26)
        Map<String, Object> whole = latestPerBucket("2026-08-25", "2026-08-26");
        assertThat(asLong(whole.get("total_issued"))).isEqualTo(4L);
        assertThat(asLong(whole.get("currently_issued"))).isEqualTo(2L);
        assertThat(asLong(whole.get("used"))).isEqualTo(1L);
        assertThat(asLong(whole.get("cancelled"))).isEqualTo(1L);
        assertThat(asLong(whole.get("expired"))).isEqualTo(0L);

        // ⚠️ 전 구간만 보면 "기간을 자른다" 는 성질을 아예 안 탄다. 하루만 잘라서
        //    그 날 발급분(회차 100 두 건 + 회차 200 한 건)만 잡히는지 본다.
        Map<String, Object> oneDay = latestPerBucket("2026-08-25", "2026-08-25");
        assertThat(asLong(oneDay.get("total_issued")))
                .as("08-25 발급분만 — 08-26 버킷(1건)이 섞이면 안 된다")
                .isEqualTo(3L);
        assertThat(asLong(oneDay.get("used"))).isEqualTo(1L);
        assertThat(asLong(oneDay.get("cancelled"))).isEqualTo(1L);
        assertThat(asLong(oneDay.get("currently_issued"))).isEqualTo(1L);

        // 반대쪽 하루
        Map<String, Object> otherDay = latestPerBucket("2026-08-26", "2026-08-26");
        assertThat(asLong(otherDay.get("total_issued"))).isEqualTo(1L);
        assertThat(asLong(otherDay.get("currently_issued"))).isEqualTo(1L);
    }

    /** A 가 할 조회 그대로 — 버킷별 최신 회차를 고른 뒤 요청 기간을 합산한다. */
    private Map<String, Object> latestPerBucket(String from, String to) {
        return writeJdbc.queryForMap("""
                SELECT SUM(s.total_issued) AS total_issued,
                       SUM(s.currently_issued) AS currently_issued,
                       SUM(s.used) AS used,
                       SUM(s.cancelled) AS cancelled,
                       SUM(s.expired) AS expired
                FROM analytics_issuance_statuses s
                JOIN (SELECT issue_date, coupon_id, MAX(run_id) AS run_id
                      FROM analytics_issuance_statuses
                      WHERE issue_date BETWEEN ? AND ?
                      GROUP BY issue_date, coupon_id) latest
                  ON latest.issue_date = s.issue_date
                 AND latest.coupon_id = s.coupon_id
                 AND latest.run_id = s.run_id
                """, from, to);
    }

    private static long asLong(Object value) {
        return ((Number) value).longValue();
    }

    /**
     * 축 실패를 <b>기록하는 중에</b> 운영 풀이 한 번 끊기는 경우다.
     *
     * <p>{@code closeRun} 이 {@code finally} 밖에 있으면 이 예외가 루프를 뚫고 나가 회차가
     * {@code IN_PROGRESS}·{@code failure_reason=NULL} 로 <b>영구히</b> 남고, 남은 축은 시도조차
     * 되지 않는다. 축 격리가 순단 한 번에 무너지는 자리다.
     */
    @Test
    @DisplayName("실패 기록이 터져도 회차는 마감되고 남은 축은 계속 돈다")
    void runIsClosedEvenIfFailureBookkeepingThrows() {
        AnalyticsRunStore brittle = new AnalyticsRunStore(
                writeJdbc, new TransactionTemplate(new JdbcTransactionManager(writeDataSource))) {
            private boolean firstCall = true;

            @Override
            public Instant watermark(AnalyticsAxis axis) {
                // 첫 축의 기준 시각 조회에서 한 번 터뜨린다 — 축 하나가 실패해도 뒤 축이
                // 계속 돌고 회차가 마감되는지를 본다.
                if (firstCall) {
                    firstCall = false;
                    throw new IllegalStateException("운영 풀 순단(테스트)");
                }
                return super.watermark(axis);
            }
        };
        AnalyticsAggregationRunner runner = new AnalyticsAggregationRunner(
                reader, brittle, properties(), new TimeProvider(clock));

        AnalyticsAggregationResult result = runner.runOnce(AS_OF);

        assertThat(result.failedAxes()).containsKey(AnalyticsAxis.MONTHLY_TREND);
        Map<String, Object> run = writeJdbc.queryForMap(
                "SELECT status, failure_reason, monthly_trend_status, issuance_status_status"
                        + " FROM analytics_runs");
        assertThat(run.get("status")).as("IN_PROGRESS 로 남으면 안 된다").isEqualTo("FAILED");
        assertThat((String) run.get("failure_reason")).isNotBlank();
        assertThat(run.get("issuance_status_status"))
                .as("앞 축이 터져도 뒤 축은 시도돼야 한다")
                .isEqualTo("AVAILABLE");
        assertThat(run.get("monthly_trend_status"))
                .as("실패한 축은 미집계(PENDING)가 아니라 장애(UNAVAILABLE)로 구분돼야 한다")
                .isEqualTo("UNAVAILABLE");
    }

    /** {@code readHourly} 만 터지는 원천. 축 하나의 실패가 나머지에 번지는지 본다. */
    private AnalyticsAggregateReader failingHourly() {
        return new AnalyticsAggregateReader(readDataSource, properties()) {
            @Override
            public List<AnalyticsAggregateReader.HourlyRow> readHourly(Instant since, Instant asOf) {
                throw new IllegalStateException("시간대 축 조회 실패(테스트)");
            }
        };
    }

    private static long sumOf(List<DailyRow> rows) {
        return rows.stream().mapToLong(DailyRow::issueCount).sum();
    }

    private Map<String, Long> dailyCounts() {
        return toMap("SELECT CONCAT(issue_date, '|', coupon_id) AS k, issue_count AS v"
                + " FROM analytics_daily_issues");
    }

    private Map<String, Long> hourlyCounts() {
        return toMap("SELECT CONCAT(issue_date, '|', issue_hour, '|', coupon_id) AS k,"
                + " issue_count AS v FROM analytics_hourly_issues");
    }

    private Map<String, Long> toMap(String sql) {
        return writeJdbc.query(sql, rs -> {
            java.util.HashMap<String, Long> result = new java.util.HashMap<>();
            while (rs.next()) {
                result.put(rs.getString("k"), rs.getLong("v"));
            }
            return result;
        });
    }

    private long countStatusRows() {
        Long count = writeJdbc.queryForObject(
                "SELECT COUNT(*) FROM analytics_issuance_statuses", Long.class);
        return count == null ? 0 : count;
    }

    // ── 시드 ──────────────────────────────────────────────────────────────────

    private static void insertBrand(long id, String name) {
        writeJdbc.update("INSERT INTO brands(id, name, category) VALUES (?, ?, '카페')", id, name);
    }

    private static void insertTemplate(long id, long brandId) {
        writeJdbc.update("""
                INSERT INTO coupon_templates(
                    id, brand_id, name, policy_type, valid_days, nth_week, day_of_week,
                    start_time, duration_hours, stock_per_occurrence, eligible_grades_mask,
                    active, created_at, updated_at)
                VALUES (?, ?, ?, 'FIXED_AMOUNT', 30, 1, 'MON', '10:00:00', 1, 100, 1, true, ?, ?)
                """, id, brandId, "템플릿 " + id, utc(AS_OF), utc(AS_OF));
    }

    private static void insertCoupon(long id, long templateId, long brandId) {
        writeJdbc.update("""
                INSERT INTO coupons(
                    id, template_id, brand_id, name, policy_type, valid_days,
                    eligible_grades_mask, open_at, close_at, status, generated_at, created_at)
                VALUES (?, ?, ?, ?, 'FIXED_AMOUNT', 30, 1, ?, ?, 'OPEN', ?, ?)
                """, id, templateId, brandId, "회차 " + id,
                utc(Instant.parse("2026-08-25T00:00:00Z")),
                utc(Instant.parse("2026-08-27T00:00:00Z")),
                utc(Instant.parse("2026-08-24T00:00:00Z")),
                utc(Instant.parse("2026-08-24T00:00:00Z")));
    }

    /** 발급 한 건과 그 {@code ISSUE} 이력. 증분 대상 선택이 이력을 근거로 삼는다. */
    private static void insertIssuance(long id, long couponId, String issuedAt, String status) {
        Instant issued = Instant.parse(issuedAt);
        writeJdbc.update("INSERT INTO members(id, membership_grade, created_at) VALUES (?, 'WELCOME', ?)",
                id, utc(Instant.parse("2026-08-01T00:00:00Z")));
        writeJdbc.update("""
                INSERT INTO issuances(
                    id, coupon_id, member_id, code, issued_grade, status,
                    issued_at, expires_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'WELCOME', ?, ?, ?, ?, ?)
                """, id, couponId, id, String.format("C%015d", id), status,
                utc(issued), utc(issued.plusSeconds(86_400)), utc(issued), utc(issued));
        insertHistory(id, "ISSUE", issuedAt);
    }

    private static void insertHistory(long issuanceId, String eventType, String createdAt) {
        writeJdbc.update("""
                INSERT INTO issuance_histories(issuance_id, event_type, to_status, created_at)
                VALUES (?, ?, 'ISSUED', ?)
                """, issuanceId, eventType, utc(Instant.parse(createdAt)));
    }

    private static LocalDateTime utc(Instant instant) {
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    // ── SELECT 전용 계정 ──────────────────────────────────────────────────────

    /**
     * 관측 계정과 <b>같은 목록</b>으로 SELECT 권한을 준다. 목록 밖 표를 읽는 질의가 들어오면
     * 여기서 1142 로 죽는다 — 목록에 없는 표를 읽는 코드가 초록불로 들어오지 않는다.
     */
    private static void createSelectOnlyReader() {
        // 계정 생성·권한 부여는 root 로만 된다. 컨테이너 기본 계정에는 CREATE USER 가 없다.
        JdbcTemplate rootJdbc = new JdbcTemplate(dataSource("root", mysql.getPassword()));
        rootJdbc.execute("CREATE USER '" + READ_USER + "'@'%' IDENTIFIED BY '" + READ_PASSWORD + "'");
        for (String table : observationTableAllowlist()) {
            rootJdbc.execute("GRANT SELECT ON app.`" + table + "` TO '" + READ_USER + "'@'%'");
        }
        rootJdbc.execute("FLUSH PRIVILEGES");
    }

    private static List<String> observationTableAllowlist() {
        Path allowlist = repositoryRoot().resolve("infra/mysql/obs-grants/allowlist.txt");
        try {
            return Files.readAllLines(allowlist).stream()
                    .map(String::trim)
                    .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                    .toList();
        } catch (IOException exception) {
            throw new UncheckedIOException("관측 테이블 allowlist를 읽을 수 없습니다: " + allowlist, exception);
        }
    }

    private static Path repositoryRoot() {
        Path current = Path.of("").toAbsolutePath();
        while (current != null) {
            if (Files.isRegularFile(current.resolve("infra/mysql/obs-grants/allowlist.txt"))) {
                return current;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("저장소 루트에서 관측 테이블 allowlist를 찾을 수 없습니다.");
    }

    private static SimpleDriverDataSource dataSource(String username, String password) {
        // 드라이버는 런타임 클래스패스에만 있다(storage 가 runtimeOnly 로 문다).
        SimpleDriverDataSource source = new SimpleDriverDataSource();
        source.setDriverClass(driverClass());
        source.setUrl(mysql.getJdbcUrl());
        source.setUsername(username);
        source.setPassword(password);
        return source;
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends java.sql.Driver> driverClass() {
        try {
            return (Class<? extends java.sql.Driver>) Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException exception) {
            throw new IllegalStateException("MySQL 드라이버가 테스트 런타임에 없다.", exception);
        }
    }

    /** 집계 완료 시각과 observed_at 이 실제로 나아가는지 보려면 시계를 손으로 움직여야 한다. */
    private static final class MutableClock extends Clock {

        private Instant instant;

        private MutableClock(Instant instant) {
            this.instant = instant;
        }

        private void set(Instant next) {
            this.instant = next;
        }

        @Override
        public java.time.ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
