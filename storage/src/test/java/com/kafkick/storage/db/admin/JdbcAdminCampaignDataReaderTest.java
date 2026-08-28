package com.kafkick.storage.db.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.MapPropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.AbstractDataSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.kafkick.core.admin.CouponPolicyType;
import com.kafkick.core.admin.campaignsource.AdminCampaignCatalog;
import com.kafkick.core.admin.campaignsource.AdminCampaignDetailData;
import com.kafkick.core.admin.campaignsource.DetailAvailability;
import com.kafkick.core.admin.campaignsource.PreparationSource;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSource;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.SourceStatus;

/** Flyway 전체 스키마와 SELECT 전용 관측 계정에서 JDBC 캠페인 조회 계약을 검증합니다. */
class JdbcAdminCampaignDataReaderTest {

    private static final DockerImageName IMAGE = DockerImageName.parse("mysql:8.4.6");
    private static final Instant SNAPSHOT = Instant.parse("2026-08-24T12:00:00Z");
    private static final Instant FROM = Instant.parse("2026-08-24T11:00:00Z");
    private static final Instant TO = Instant.parse("2026-08-24T12:00:00Z");

    private static MySQLContainer mysql;
    private static HikariDataSource writeDataSource;
    private static HikariDataSource rawObservationDataSource;
    private static CountingDataSource observationDataSource;
    private static JdbcTemplate writeJdbc;
    private static JdbcTemplate observationJdbc;
    private static AnnotationConfigApplicationContext context;
    private static JdbcAdminCampaignDataReader reader;

    @BeforeAll
    static void startMySql() {
        mysql = new MySQLContainer(IMAGE)
                .withDatabaseName("app")
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

        writeDataSource = hikari(mysql.getUsername(), mysql.getPassword());
        writeJdbc = new JdbcTemplate(writeDataSource);
        try (HikariDataSource rootDataSource = hikari("root", mysql.getPassword())) {
            JdbcTemplate root = new JdbcTemplate(rootDataSource);
            root.execute("CREATE USER 'campaign_obs'@'%' IDENTIFIED BY 'campaign_obs'");
            for (String table : observationTableAllowlist()) {
                if (tableExists(root, table)) {
                    root.execute("GRANT SELECT ON app.`" + table + "` TO 'campaign_obs'@'%'");
                }
            }
            root.execute("FLUSH PRIVILEGES");
        }

        rawObservationDataSource = hikari("campaign_obs", "campaign_obs");
        observationDataSource = new CountingDataSource(rawObservationDataSource);
        observationJdbc = new JdbcTemplate(observationDataSource);
        ReaderTestConfiguration.dataSource = observationDataSource;
        context = new AnnotationConfigApplicationContext();
        context.getEnvironment().getPropertySources().addFirst(new MapPropertySource(
                "reader-test", Map.of("observation.datasource.enabled", "true")));
        context.register(ReaderTestConfiguration.class);
        context.refresh();
        reader = context.getBean(JdbcAdminCampaignDataReader.class);
    }

    @AfterAll
    static void stopMySql() {
        if (context != null) {
            context.close();
        }
        if (rawObservationDataSource != null) {
            rawObservationDataSource.close();
        }
        if (writeDataSource != null) {
            writeDataSource.close();
        }
        if (mysql != null) {
            mysql.stop();
        }
    }

    @BeforeEach
    void seedReferences() {
        writeJdbc.update("INSERT INTO grades(code, bit_value) VALUES ('WELCOME', 1)");
        insertMember(1);
        writeJdbc.update("INSERT INTO brands(id, name, category) VALUES (1, '모카빈', '카페')");
        insertTemplate(1L, 1L);
    }

    @AfterEach
    void clearTables() {
        writeJdbc.update("DELETE FROM issuance_histories");
        writeJdbc.update("DELETE FROM issuances");
        writeJdbc.update("DELETE FROM coupon_stocks");
        writeJdbc.update("DELETE FROM coupons");
        writeJdbc.update("DELETE FROM coupon_templates");
        writeJdbc.update("DELETE FROM brands");
        writeJdbc.update("DELETE FROM members");
        writeJdbc.update("DELETE FROM grades");
    }

    @Test
    @DisplayName("활성 스위치가 켜진 경우에만 관측 저장소 빈으로 등록된다")
    void readerIsConditionalRepository() {
        Class<JdbcAdminCampaignDataReader> type = JdbcAdminCampaignDataReader.class;

        assertThat(type).hasAnnotation(Repository.class);
        ConditionalOnProperty condition = type.getAnnotation(ConditionalOnProperty.class);
        assertThat(condition).isNotNull();
        assertThat(condition.name()).containsExactly("observation.datasource.enabled");
        assertThat(condition.havingValue()).isEqualTo("true");
    }

    @Test
    @DisplayName("Reader가 사용하는 관측 계정은 쓰기를 거부한다")
    void observationAccountIsSelectOnly() {
        assertThatThrownBy(() -> observationJdbc.update(
                "UPDATE brands SET name = '변경' WHERE id = 1"))
                .rootCause()
                .hasMessageContaining("UPDATE command denied to user 'campaign_obs'");
    }

    @Test
    @DisplayName("상세 조회는 이름이 지정된 관측 read-only 트랜잭션을 사용한다")
    void detailUsesNamedReadOnlyTransaction() throws Exception {
        Transactional transactional = JdbcAdminCampaignDataReader.class
                .getMethod("findDetail", long.class, Instant.class, Instant.class, Instant.class)
                .getAnnotation(Transactional.class);

        assertThat(transactional).isNotNull();
        assertThat(transactional.transactionManager()).isEqualTo("observationTransactionManager");
        assertThat(transactional.readOnly()).isTrue();
        assertThat(AopUtils.isAopProxy(reader)).isTrue();
    }

    @Test
    @DisplayName("카탈로그는 오픈 시각과 ID 내림차순이며 재고로 준비 상태를 판정한다")
    void catalogIsSortedAndDerivesPreparationFromStock() {
        insertTemplate(2L, 1L);
        insertCoupon(10, 1, 1, "오래된", "CLOSED", SNAPSHOT.minusSeconds(7200));
        insertCoupon(20, 1, 1, "최신 작은 ID", "OPEN", SNAPSHOT.minusSeconds(3600));
        insertCoupon(21, 2, 1, "최신 큰 ID", "SCHEDULED", SNAPSHOT.minusSeconds(3600));
        insertStock(10, 100, 0, SNAPSHOT.minusSeconds(60));
        insertStock(20, 200, 0, SNAPSHOT.minusSeconds(30));

        AdminCampaignCatalog catalog = reader.loadCatalog(SNAPSHOT);

        assertThat(catalog.status()).isEqualTo(SourceStatus.VALID);
        assertThat(catalog.observedAt()).isEqualTo(SNAPSHOT);
        assertThat(catalog.campaigns()).extracting(AdminCampaignCatalog.CampaignData::couponId)
                .containsExactly(21L, 20L, 10L);
        assertThat(catalog.campaigns().get(0).stock().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(catalog.campaigns().get(1).stock().value())
                .isEqualTo(new CouponMetricsSource.StockCounts(200, 0));
        assertThat(catalog.campaigns().get(0).preparation())
                .isEqualTo(new PreparationSource(
                        true, false, CouponPolicyType.FIXED_AMOUNT, SourceStatus.VALID, SNAPSHOT));
        assertThat(catalog.campaigns().get(1).preparation())
                .isEqualTo(new PreparationSource(
                        true, true, CouponPolicyType.FIXED_AMOUNT, SourceStatus.VALID, SNAPSHOT));
        assertThat(catalog.campaigns().get(2).preparation())
                .isEqualTo(new PreparationSource(
                        true, true, CouponPolicyType.FIXED_AMOUNT, SourceStatus.VALID, SNAPSHOT));
    }

    /** 실제 회차 도메인의 24시간 상한을 넘는 기간은 설정 실패로 판정하는지 검증합니다. */
    @Test
    @DisplayName("24시간을 초과한 회차는 캠페인 설정이 준비되지 않는다")
    void couponRoundDurationCannotExceedTwentyFourHours() {
        Instant opensAt = SNAPSHOT.minusSeconds(60);
        insertCoupon(10, 1, 1, "기간 초과", "OPEN", opensAt);
        writeJdbc.update(
                "UPDATE coupons SET close_at = ? WHERE id = 10",
                timestamp(opensAt.plusSeconds(86_401)));
        insertStock(10, 100, 0, SNAPSHOT.minusSeconds(5));

        PreparationSource preparation = reader.loadCatalog(SNAPSHOT)
                .campaigns().getFirst().preparation();

        assertThat(preparation.campaignConfigurationReady()).isFalse();
    }

    @Test
    @DisplayName("역정규화 브랜드가 없으면 catalog와 detail 모두 UNAVAILABLE이다")
    void orphanBrandMakesBothQueriesUnavailable() {
        insertCoupon(10, 1, 999, "고아 브랜드", "OPEN", SNAPSHOT.minusSeconds(60));

        assertThat(reader.loadCatalog(SNAPSHOT).status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(reader.findDetail(10, FROM, TO, SNAPSHOT).availability())
                .isEqualTo(DetailAvailability.UNAVAILABLE);
    }

    @Test
    @DisplayName("상세는 네 현재 상태와 네 전이 이벤트를 정확히 매핑한다")
    void detailMapsHoldingAndTransitionCounts() {
        insertCoupon(10, 1, 1, "상태 캠페인", "OPEN", SNAPSHOT.minusSeconds(7200));
        insertStock(10, 20, 3, SNAPSHOT.minusSeconds(5));
        insertIssuance(101, 10, 1, "ISSUED", 1);
        insertMember(2);
        insertMember(3);
        insertMember(4);
        insertMember(5);
        insertIssuance(102, 10, 2, "ISSUED", 2);
        insertIssuance(103, 10, 3, "USED", 3);
        insertIssuance(104, 10, 4, "CANCELLED", 4);
        insertIssuance(105, 10, 5, "EXPIRED", 5);
        insertHistory(201, 101, "USE", FROM.plusSeconds(10));
        insertHistory(202, 102, "USE", FROM.plusSeconds(20));
        insertHistory(203, 103, "CANCEL_USE", FROM.plusSeconds(30));
        insertHistory(204, 104, "CANCEL", FROM.plusSeconds(40));
        insertHistory(205, 105, "EXPIRE", FROM.plusSeconds(50));

        AdminCampaignDetailData result = reader.findDetail(10, FROM, TO, SNAPSHOT);

        assertThat(result.availability()).isEqualTo(DetailAvailability.AVAILABLE);
        assertThat(result.value().campaign().status()).isEqualTo(CouponRoundStatus.OPEN);
        assertThat(result.value().holdingCounts().status()).isEqualTo(SourceStatus.VALID);
        assertThat(result.value().holdingCounts().value())
                .isEqualTo(new CouponMetricsSource.IssuanceStatusCounts(2, 1, 1, 1));
        assertThat(result.value().transitions().status()).isEqualTo(SourceStatus.VALID);
        assertThat(result.value().transitions().value()).containsExactly(
                new CouponMetricsSource.TransitionBucket(FROM, TO, 2, 1, 1, 1));
    }

    @Test
    @DisplayName("재고 활성 수와 ISSUED+USED가 다르면 불완전 상세를 내보내지 않는다")
    void inconsistentActiveCountMakesDetailUnavailable() {
        insertCoupon(10, 1, 1, "불일치", "OPEN", SNAPSHOT.minusSeconds(60));
        insertStock(10, 20, 2, SNAPSHOT.minusSeconds(5));
        insertIssuance(101, 10, 1, "ISSUED", 1);

        try (LogCapture logs = LogCapture.start()) {
            assertThat(reader.findDetail(10, FROM, TO, SNAPSHOT).availability())
                    .isEqualTo(DetailAvailability.UNAVAILABLE);
            assertThat(logs.messages()).anySatisfy(message -> assertThat(message)
                    .contains("admin campaign stock drift", "couponId=10", "activeCount=2", "issuedPlusUsed=1"));
        }
    }

    @Test
    @DisplayName("상태 전이 구간은 시작을 포함하고 종료를 제외하며 microsecond를 보존한다")
    void transitionWindowIsHalfOpenAtMicrosecondPrecision() {
        insertCoupon(10, 1, 1, "경계", "OPEN", SNAPSHOT.minusSeconds(60));
        insertStock(10, 10, 1, SNAPSHOT.minusSeconds(5));
        insertIssuance(101, 10, 1, "ISSUED", 1);
        Instant start = FROM.plusNanos(123_000);
        Instant end = TO.plusNanos(456_000);
        insertHistory(201, 101, "USE", start.minusNanos(1_000));
        insertHistory(202, 101, "USE", start);
        insertHistory(203, 101, "USE", end.minusNanos(1_000));
        insertHistory(204, 101, "USE", end);

        AdminCampaignDetailData result = reader.findDetail(10, start, end, SNAPSHOT);

        assertThat(result.value().transitions().value().get(0).use()).isEqualTo(2);
        assertThat(result.value().transitions().value().get(0).windowStart()).isEqualTo(start);
        assertThat(result.value().transitions().value().get(0).windowEnd()).isEqualTo(end);
    }

    @Test
    @DisplayName("재고 행이 없으면 상세는 존재하되 재고만 UNAVAILABLE이다")
    void missingStockKeepsAvailableDetail() {
        insertCoupon(10, 1, 1, "재고 없음", "OPEN", SNAPSHOT.minusSeconds(60));

        AdminCampaignDetailData result = reader.findDetail(10, FROM, TO, SNAPSHOT);

        assertThat(result.availability()).isEqualTo(DetailAvailability.AVAILABLE);
        assertThat(result.value().stock().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(result.value().holdingCounts().status()).isEqualTo(SourceStatus.NO_TRAFFIC);
    }

    @Test
    @DisplayName("발급과 전이가 비면 네 상태와 한 구간의 0값을 NO_TRAFFIC으로 반환한다")
    void emptyIssuanceAndTransitionAreNoTraffic() {
        insertCoupon(10, 1, 1, "빈 캠페인", "SCHEDULED", SNAPSHOT.plusSeconds(60));
        insertStock(10, 10, 0, SNAPSHOT.minusSeconds(5));

        AdminCampaignDetailData.DetailValue value = reader.findDetail(10, FROM, TO, SNAPSHOT).value();

        assertThat(value.holdingCounts().status()).isEqualTo(SourceStatus.NO_TRAFFIC);
        assertThat(value.holdingCounts().value())
                .isEqualTo(new CouponMetricsSource.IssuanceStatusCounts(0, 0, 0, 0));
        assertThat(value.transitions().status()).isEqualTo(SourceStatus.NO_TRAFFIC);
        assertThat(value.transitions().value()).containsExactly(
                new CouponMetricsSource.TransitionBucket(FROM, TO, 0, 0, 0, 0));
    }

    @Test
    @DisplayName("모르는 캠페인 또는 발급 상태는 합계를 왜곡하지 않고 UNAVAILABLE이다")
    void unknownStatesAreUnavailable() {
        insertCoupon(10, 1, 1, "모르는 캠페인 상태", "BROKEN", SNAPSHOT.minusSeconds(60));
        assertThat(reader.findDetail(10, FROM, TO, SNAPSHOT).availability())
                .isEqualTo(DetailAvailability.UNAVAILABLE);
        writeJdbc.update("DELETE FROM coupons WHERE id = 10");

        insertCoupon(11, 1, 1, "모르는 발급 상태", "OPEN", SNAPSHOT.minusSeconds(60));
        insertStock(11, 10, 0, SNAPSHOT.minusSeconds(5));
        insertIssuance(101, 11, 1, "BROKEN", 1);
        assertThat(reader.findDetail(11, FROM, TO, SNAPSHOT).availability())
                .isEqualTo(DetailAvailability.UNAVAILABLE);
    }

    @Test
    @DisplayName("집계 대상이 아닌 이력 이벤트는 네 전이 합계에 섞지 않는다")
    void unrelatedHistoryEventIsExcluded() {
        insertCoupon(10, 1, 1, "관련 없는 이벤트", "OPEN", SNAPSHOT.minusSeconds(60));
        insertStock(10, 10, 1, SNAPSHOT.minusSeconds(5));
        insertIssuance(101, 10, 1, "ISSUED", 1);
        insertHistory(201, 101, "BROKEN", FROM.plusSeconds(10));

        CouponMetricsSource.Observation<List<CouponMetricsSource.TransitionBucket>> transitions =
                reader.findDetail(10, FROM, TO, SNAPSHOT).value().transitions();

        assertThat(transitions.status()).isEqualTo(SourceStatus.NO_TRAFFIC);
        assertThat(transitions.value().get(0))
                .isEqualTo(new CouponMetricsSource.TransitionBucket(FROM, TO, 0, 0, 0, 0));
    }

    @Test
    @DisplayName("존재하지 않는 캠페인은 DB 장애와 다른 NOT_FOUND이다")
    void missingCampaignIsNotFound() {
        assertThat(reader.findDetail(404, FROM, TO, SNAPSHOT).availability())
                .isEqualTo(DetailAvailability.NOT_FOUND);
    }

    @Test
    @DisplayName("SQL 연결 실패는 빈 값이나 NOT_FOUND가 아니라 UNAVAILABLE이다")
    void sqlFailureIsUnavailable() {
        NamedParameterJdbcTemplate failingTemplate = new NamedParameterJdbcTemplate(
                new AbstractDataSource() {
                    @Override
                    public Connection getConnection() throws SQLException {
                        throw new SQLException("forced observation failure");
                    }

                    @Override
                    public Connection getConnection(String username, String password) throws SQLException {
                        throw new SQLException("forced observation failure");
                    }
                });
        JdbcAdminCampaignDataReader failingReader = new JdbcAdminCampaignDataReader(failingTemplate);

        try (LogCapture logs = LogCapture.start()) {
            assertThat(failingReader.loadCatalog(SNAPSHOT).status()).isEqualTo(SourceStatus.UNAVAILABLE);
            assertThat(failingReader.findDetail(10, FROM, TO, SNAPSHOT).availability())
                    .isEqualTo(DetailAvailability.UNAVAILABLE);
            assertThat(logs.messages()).anySatisfy(message -> assertThat(message)
                    .contains("admin campaign catalog observation failed", "snapshotAt=" + SNAPSHOT));
            assertThat(logs.messages()).anySatisfy(message -> assertThat(message)
                    .contains("admin campaign detail observation failed", "couponId=10"));
        }
    }

    @Test
    @DisplayName("상세의 세 SELECT는 한 관측 트랜잭션 connection을 공유한다")
    void detailQueriesShareOneConnection() {
        insertCoupon(10, 1, 1, "한 스냅샷", "OPEN", SNAPSHOT.minusSeconds(60));
        insertStock(10, 10, 0, SNAPSHOT.minusSeconds(5));
        observationDataSource.resetCount();

        AdminCampaignDetailData result = reader.findDetail(10, FROM, TO, SNAPSHOT);

        assertThat(result.availability()).isEqualTo(DetailAvailability.AVAILABLE);
        assertThat(observationDataSource.connectionCount()).isEqualTo(1);
    }

    private static HikariDataSource hikari(String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(mysql.getJdbcUrl());
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(4);
        config.addDataSourceProperty("rewriteBatchedStatements", "true");
        return new HikariDataSource(config);
    }

    private static List<String> observationTableAllowlist() {
        Path allowlist = repositoryRoot().resolve("infra/mysql/obs-grants/allowlist.txt");
        try {
            return Files.readAllLines(allowlist).stream()
                    .map(String::trim)
                    .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                    .peek(table -> {
                        if (!table.matches("[A-Za-z0-9_]+")) {
                            throw new IllegalArgumentException("잘못된 관측 테이블 이름: " + table);
                        }
                    })
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

    private static boolean tableExists(JdbcTemplate jdbc, String table) {
        Integer count = jdbc.queryForObject("""
                SELECT COUNT(*)
                  FROM information_schema.tables
                 WHERE table_schema = 'app'
                   AND table_name = ?
                """, Integer.class, table);
        return count != null && count == 1;
    }

    private static void insertTemplate(long id, long brandId) {
        writeJdbc.update("""
                INSERT INTO coupon_templates(
                    id, brand_id, name, policy_type, valid_days,
                    nth_week, day_of_week, start_time, duration_hours,
                    stock_per_occurrence, eligible_grades_mask, active,
                    created_at, updated_at
                ) VALUES (?, ?, ?, 'FIXED_AMOUNT', 30, 1, 'MON', '10:00:00', 1, 100, 1, true, ?, ?)
                """, id, brandId, "템플릿 " + id, timestamp(SNAPSHOT), timestamp(SNAPSHOT));
    }

    private static void insertMember(long id) {
        writeJdbc.update("INSERT INTO members(id, membership_grade, created_at) VALUES (?, 'WELCOME', ?)",
                id, timestamp(SNAPSHOT.minusSeconds(86_400)));
    }

    private static void insertCoupon(long id, long templateId, long brandId, String name,
                                     String status, Instant opensAt) {
        writeJdbc.update("""
                INSERT INTO coupons(
                    id, template_id, brand_id, name, policy_type, discount_amount, valid_days,
                    eligible_grades_mask, open_at, close_at, status, generated_at, created_at
                ) VALUES (?, ?, ?, ?, 'FIXED_AMOUNT', 5000, 30, 1, ?, ?, ?, ?, ?)
                """, couponArguments(id, templateId, brandId, name, status, opensAt));
    }

    private static Object[] couponArguments(long id, long templateId, long brandId, String name,
                                            String status, Instant opensAt) {
        return new Object[]{id, templateId, brandId, name, timestamp(opensAt),
                timestamp(opensAt.plusSeconds(3600)), status,
                timestamp(opensAt.minusSeconds(60)), timestamp(opensAt.minusSeconds(60))};
    }

    private static void insertStock(long couponId, int total, int active, Instant updatedAt) {
        writeJdbc.update("""
                INSERT INTO coupon_stocks(coupon_id, total_quantity, active_count, updated_at)
                VALUES (?, ?, ?, ?)
                """, couponId, total, active, timestamp(updatedAt));
    }

    private static void insertIssuance(long id, long couponId, long memberId, String status, int suffix) {
        Instant issuedAt = SNAPSHOT.minusSeconds(3600L - suffix);
        writeJdbc.update("""
                INSERT INTO issuances(
                    id, coupon_id, member_id, code, issued_grade, status,
                    issued_at, expires_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'WELCOME', ?, ?, ?, ?, ?)
                """, id, couponId, memberId, String.format("C%015d", suffix), status,
                timestamp(issuedAt), timestamp(issuedAt.plusSeconds(86_400)),
                timestamp(issuedAt), timestamp(issuedAt));
    }

    private static void insertHistory(long id, long issuanceId, String eventType, Instant createdAt) {
        writeJdbc.update("""
                INSERT INTO issuance_histories(
                    id, issuance_id, event_type, from_status, to_status, created_at
                ) VALUES (?, ?, ?, 'ISSUED', 'USED', ?)
                """, id, issuanceId, eventType, timestamp(createdAt));
    }

    private static Timestamp timestamp(Instant instant) {
        return Timestamp.from(instant);
    }

    @Configuration(proxyBeanMethods = false)
    @EnableTransactionManagement(proxyTargetClass = true)
    @Import(JdbcAdminCampaignDataReader.class)
    static class ReaderTestConfiguration {
        private static DataSource dataSource;

        @Bean
        @Qualifier("obs")
        NamedParameterJdbcTemplate observationNamedParameterJdbcTemplate() {
            return new NamedParameterJdbcTemplate(dataSource);
        }

        @Bean("observationTransactionManager")
        PlatformTransactionManager observationTransactionManager() {
            return new DataSourceTransactionManager(dataSource);
        }
    }

    private static final class CountingDataSource extends DelegatingDataSource {
        private final AtomicInteger connections = new AtomicInteger();

        private CountingDataSource(DataSource targetDataSource) {
            super(targetDataSource);
        }

        @Override
        public Connection getConnection() throws SQLException {
            connections.incrementAndGet();
            return super.getConnection();
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            connections.incrementAndGet();
            return super.getConnection(username, password);
        }

        private void resetCount() {
            connections.set(0);
        }

        private int connectionCount() {
            return connections.get();
        }
    }

    private record LogCapture(Logger logger, ListAppender<ILoggingEvent> appender)
            implements AutoCloseable {

        private static LogCapture start() {
            Logger logger = (Logger) LoggerFactory.getLogger(JdbcAdminCampaignDataReader.class);
            ListAppender<ILoggingEvent> appender = new ListAppender<>();
            appender.start();
            logger.addAppender(appender);
            return new LogCapture(logger, appender);
        }

        private List<String> messages() {
            return appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        }

        @Override
        public void close() {
            logger.detachAppender(appender);
            appender.stop();
        }
    }
}
