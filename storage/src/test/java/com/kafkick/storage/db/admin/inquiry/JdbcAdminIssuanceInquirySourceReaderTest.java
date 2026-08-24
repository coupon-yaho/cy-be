package com.kafkick.storage.db.admin.inquiry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.zaxxer.hikari.HikariDataSource;

import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryErrorCode;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryQuery;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryQuery.InquiryPosition;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryQuery.SourceKind;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryReadResult;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource.RawAttempt;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource.RawHistoryLink;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource.RawIssuance;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySourceReader;
import com.kafkick.core.admin.inquiry.IssuanceInquiryCalculator;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.support.exception.BusinessException;

/** 실제 MySQL 8.4.6에서 관리자 회원 발급 문의 SQL, 매핑과 관측 풀 경계를 검증합니다. */
class JdbcAdminIssuanceInquirySourceReaderTest {

    private static final Instant SNAPSHOT = Instant.parse("2026-08-25T12:00:00Z");
    private static final DockerImageName IMAGE = DockerImageName.parse("mysql:8.4.6");

    private static MySQLContainer mysql;
    private static HikariDataSource writeDataSource;
    private static HikariDataSource observationDataSource;
    private static JdbcTemplate writeJdbc;
    private static AnnotationConfigApplicationContext context;
    private static AdminIssuanceInquirySourceReader reader;

    @BeforeAll
    static void startDatabase() {
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

        writeDataSource = dataSource(mysql.getUsername(), mysql.getPassword());
        writeJdbc = new JdbcTemplate(writeDataSource);
        try (HikariDataSource root = dataSource("root", mysql.getPassword())) {
            JdbcTemplate rootJdbc = new JdbcTemplate(root);
            rootJdbc.execute("CREATE USER 'obs_inquiry'@'%' IDENTIFIED BY 'obs_inquiry'");
            rootJdbc.execute("GRANT SELECT ON app.* TO 'obs_inquiry'@'%'");
            rootJdbc.execute("FLUSH PRIVILEGES");
        }
        observationDataSource = dataSource("obs_inquiry", "obs_inquiry");
        ReaderContext.observationDataSource = observationDataSource;

        context = new AnnotationConfigApplicationContext();
        TestPropertyValues.of("observation.datasource.enabled=true").applyTo(context);
        context.register(ReaderContext.class);
        context.refresh();
        reader = context.getBean(AdminIssuanceInquirySourceReader.class);
    }

    @AfterAll
    static void stopDatabase() {
        if (context != null) context.close();
        if (observationDataSource != null) observationDataSource.close();
        if (writeDataSource != null) writeDataSource.close();
        if (mysql != null) mysql.stop();
    }

    @AfterEach
    void resetFixture() {
        writeJdbc.execute("DELETE FROM issue_attempts");
        writeJdbc.execute("DELETE FROM issuance_histories");
        writeJdbc.execute("DELETE FROM issuances");
        writeJdbc.execute("DELETE FROM coupons");
        writeJdbc.execute("DELETE FROM coupon_templates");
        writeJdbc.execute("DELETE FROM brands");
        writeJdbc.execute("DELETE FROM members");
        writeJdbc.execute("DELETE FROM grades");
    }

    @Test
    @DisplayName("회원 미존재, 쿠폰 미존재와 존재하지만 빈 결과를 구분한다")
    void distinguishesExistenceFromEmptyRows() {
        baseFixture();

        assertThat(read(query(999L, null, null, null, null, 10)).availability())
                .isEqualTo(AdminIssuanceInquiryReadResult.Availability.MEMBER_NOT_FOUND);
        assertThat(read(query(101L, 999L, null, null, null, 10)).availability())
                .isEqualTo(AdminIssuanceInquiryReadResult.Availability.COUPON_NOT_FOUND);

        AdminIssuanceInquiryReadResult empty = read(query(101L, 201L, null, null, null, 10));
        assertThat(empty.availability()).isEqualTo(AdminIssuanceInquiryReadResult.Availability.AVAILABLE);
        assertThat(empty.source().attempts()).isEmpty();
        assertThat(empty.source().issuances()).isEmpty();
        assertThat(empty.source().histories()).isEmpty();
    }

    @Test
    @DisplayName("회원 조건은 같은 requestId를 가진 다른 회원의 attempt를 격리한다")
    void attemptQueryRejectsAnotherMemberWithSameRequestId() {
        baseFixture();
        attempt(1, "ISSUE_RESULT", "same-request", 102, 201, null, 409,
                "ALREADY_ISSUED", seconds(-1));

        assertThat(source(query(101L, null, null, null, null, 10)).attempts()).isEmpty();
    }

    @Test
    @DisplayName("결과 행을 대표로 고른 뒤 HTTP 상태와 사유를 필터링한다")
    void filtersAfterChoosingResultRepresentative() {
        baseFixture();
        attempt(10, "ISSUE_ATTEMPT", "representative", 101, 201, null, null, null, seconds(-1));
        attempt(11, "ISSUE_RESULT", "representative", 101, 201, null, 409,
                "STOCK_EXHAUSTED", seconds(-3));

        AdminIssuanceInquirySource matching = source(query(
                101L, null, 409, ReasonCode.STOCK_EXHAUSTED, null, 10));
        assertThat(matching.attempts()).extracting(RawAttempt::attemptId).containsExactly(11L);
        assertThat(source(query(101L, null, null, null, null, 10)).attempts())
                .extracting(RawAttempt::attemptId).containsExactly(11L);
        assertThat(source(query(101L, null, 500, null, null, 10)).attempts()).isEmpty();
    }

    @Test
    @DisplayName("direct ID, ISSUE history와 DB 단독 발급을 각각 보존한다")
    void readsDirectHistoryAndDatabaseOnlyIssuances() {
        baseFixture();
        issuance(301, 101, 201, "USED", seconds(-20));
        issuance(302, 101, 202, "ISSUED", seconds(-19));
        issuance(303, 101, 203, "CANCELLED", seconds(-18));
        attempt(21, "ISSUE_RESULT", "direct", 101, 201, 301L, 201, null, seconds(-10));
        attempt(22, "ISSUE_ATTEMPT", "history", 101, 202, null, null, null, seconds(-9));
        history(401, 302, "ISSUE", "history", seconds(-8));

        AdminIssuanceInquirySource result = source(query(101L, null, null, null, null, 10));

        assertThat(result.attempts()).extracting(RawAttempt::attemptId).containsExactly(22L, 21L);
        assertThat(result.issuances()).extracting(RawIssuance::issuanceId)
                .containsExactlyInAnyOrder(301L, 302L, 303L);
        assertThat(result.histories()).extracting(RawHistoryLink::historyId).containsExactly(401L);
    }

    @Test
    @DisplayName("이전 페이지 attempt와 연결된 발급은 다음 페이지에서 DB 단독 후보가 되지 않는다")
    void linkedIssuanceDoesNotReappearOnLaterPage() {
        baseFixture();
        issuance(310, 101, 201, "ISSUED", seconds(-30));
        attempt(31, "ISSUE_RESULT", "page-one", 101, 201, 310L, 201, null, seconds(-5));

        InquiryPosition afterAttempt = new InquiryPosition(seconds(-5), SourceKind.ATTEMPT, 31);
        AdminIssuanceInquirySource later = source(query(101L, null, null, null, afterAttempt, 1));

        assertThat(later.attempts()).isEmpty();
        assertThat(later.issuances()).isEmpty();
    }

    @Test
    @DisplayName("존재하지 않거나 다른 범위인 issuanceId와 같은 requestId로 상태를 위조하지 않는다")
    void preservesScopeWhenEnrichingAttempts() {
        baseFixture();
        issuance(320, 102, 201, "USED", seconds(-30));
        issuance(321, 101, 202, "CANCELLED", seconds(-29));
        history(420, 321, "ISSUE", "shared", seconds(-15));
        attempt(40, "ISSUE_RESULT", "missing", 101, 201, 999L, 201, null, seconds(-5));
        attempt(41, "ISSUE_RESULT", "foreign-direct", 101, 201, 320L, 201, null, seconds(-4));
        attempt(42, "ISSUE_ATTEMPT", "shared", 101, 201, null, null, null, seconds(-3));

        AdminIssuanceInquirySource result = source(query(101L, 201L, null, null, null, 10));
        var page = new IssuanceInquiryCalculator().calculate(result,
                query(101L, 201L, null, null, null, 10));

        assertThat(result.issuances()).isEmpty();
        assertThat(result.histories()).isEmpty();
        assertThat(page.items()).hasSize(3).allSatisfy(item -> {
            assertThat(item.issuanceId()).isNull();
            assertThat(item.currentStatus()).isNull();
        });
    }

    @Test
    @DisplayName("ATTEMPT와 ISSUANCE cursor의 같은 시각 네 조합과 같은 원천 ID 동률을 지킨다")
    void appliesSourceSpecificCursorTruthTable() {
        baseFixture();
        Instant tied = seconds(-10);
        attempt(50, "ISSUE_ATTEMPT", "a50", 101, 201, null, null, null, tied);
        attempt(51, "ISSUE_ATTEMPT", "a51", 101, 201, null, null, null, tied);
        issuance(350, 101, 201, "ISSUED", tied);
        issuance(351, 101, 202, "ISSUED", tied);

        AdminIssuanceInquirySource beforeAttempt = source(query(101L, null, null, null,
                new InquiryPosition(tied, SourceKind.ATTEMPT, 51), 10));
        assertThat(beforeAttempt.attempts()).extracting(RawAttempt::attemptId).containsExactly(50L);
        assertThat(beforeAttempt.issuances()).isEmpty();

        AdminIssuanceInquirySource beforeIssuance = source(query(101L, null, null, null,
                new InquiryPosition(tied, SourceKind.ISSUANCE, 351), 10));
        assertThat(beforeIssuance.attempts()).extracting(RawAttempt::attemptId).containsExactly(51L, 50L);
        assertThat(beforeIssuance.issuances()).extracting(RawIssuance::issuanceId).containsExactly(350L);
    }

    @Test
    @DisplayName("limit 1의 세 페이지가 새 행을 중복하거나 누락하지 않는다")
    void pagesAcrossSourcesWithoutDuplicatesOrGaps() {
        baseFixture();
        attempt(60, "ISSUE_ATTEMPT", "p60", 101, 201, null, null, null, seconds(-1));
        issuance(360, 101, 201, "ISSUED", seconds(-2));
        attempt(61, "ISSUE_RESULT", "p61", 101, 201, null, 409,
                "ALREADY_ISSUED", seconds(-3));

        InquiryPosition before = null;
        Set<InquiryPosition> positions = new HashSet<>();
        List<SourceKind> kinds = new ArrayList<>();
        for (int pageNumber = 0; pageNumber < 3; pageNumber++) {
            AdminIssuanceInquiryQuery pageQuery = query(101L, null, null, null, before, 1);
            var page = new IssuanceInquiryCalculator().calculate(source(pageQuery), pageQuery);
            assertThat(page.items()).hasSize(1);
            assertThat(positions.add(page.items().getFirst().position())).isTrue();
            kinds.add(page.items().getFirst().position().sourceKind());
            before = page.nextBefore();
        }
        assertThat(kinds).containsExactly(SourceKind.ATTEMPT, SourceKind.ISSUANCE, SourceKind.ATTEMPT);
    }

    @Test
    @DisplayName("각 원천은 후보가 더 있어도 정확히 limit + 1개만 읽는다")
    void eachRawSourceStopsAtLimitPlusOne() {
        baseFixture();
        attempt(62, "ISSUE_ATTEMPT", "limit-a", 101, 201, null, null, null, seconds(-1));
        attempt(63, "ISSUE_ATTEMPT", "limit-b", 101, 201, null, null, null, seconds(-2));
        attempt(64, "ISSUE_ATTEMPT", "limit-c", 101, 201, null, null, null, seconds(-3));
        issuance(362, 101, 201, "ISSUED", seconds(-11));
        issuance(363, 101, 202, "ISSUED", seconds(-12));
        issuance(364, 101, 203, "ISSUED", seconds(-13));

        AdminIssuanceInquirySource raw = source(query(101L, null, null, null, null, 1));

        assertThat(raw.attempts()).extracting(RawAttempt::attemptId).containsExactly(62L, 63L);
        assertThat(raw.issuances()).extracting(RawIssuance::issuanceId).containsExactly(362L, 363L);
    }

    @Test
    @DisplayName("snapshot 이후 원천은 혼입되지 않고 기존 발급을 미리 숨기지도 않는다")
    void snapshotBoundaryAlsoAppliesToDeduplicationLinks() {
        baseFixture();
        issuance(370, 101, 201, "ISSUED", seconds(-20));
        issuance(371, 101, 202, "ISSUED", seconds(-19));
        attempt(70, "ISSUE_RESULT", "future-direct", 101, 201, 370L, 201, null, seconds(1));
        attempt(71, "ISSUE_ATTEMPT", "future-history", 101, 202, null, null, null, seconds(1));
        history(470, 371, "ISSUE", "future-history", seconds(1));

        AdminIssuanceInquirySource result = source(query(101L, null, null, null, null, 10));

        assertThat(result.attempts()).isEmpty();
        assertThat(result.histories()).isEmpty();
        assertThat(result.issuances()).extracting(RawIssuance::issuanceId)
                .containsExactlyInAnyOrder(370L, 371L);
    }

    @Test
    @DisplayName("snapshot 이후 발급은 과거 direct attempt의 상태 보강에 사용하지 않는다")
    void futureDirectIssuanceIsNotUsedForEnrichment() {
        baseFixture();
        issuance(372, 101, 201, "ISSUED", seconds(1));
        attempt(72, "ISSUE_RESULT", "past-direct", 101, 201, 372L, 201, null, seconds(-1));

        AdminIssuanceInquirySource result = source(query(101L, 201L, null, null, null, 10));

        assertThat(result.attempts()).extracting(RawAttempt::attemptId).containsExactly(72L);
        assertThat(result.issuances()).isEmpty();
    }

    @Test
    @DisplayName("snapshot 이후 발급은 과거 ISSUE history 보강에 사용하지 않는다")
    void futureHistoryIssuanceIsNotUsedForEnrichment() {
        baseFixture();
        issuance(373, 101, 201, "ISSUED", seconds(1));
        history(473, 373, "ISSUE", "past-history", seconds(-1));
        attempt(73, "ISSUE_ATTEMPT", "past-history", 101, 201, null, null, null, seconds(-2));

        AdminIssuanceInquirySource result = source(query(101L, 201L, null, null, null, 10));

        assertThat(result.attempts()).extracting(RawAttempt::attemptId).containsExactly(73L);
        assertThat(result.issuances()).isEmpty();
        assertThat(result.histories()).isEmpty();
    }

    @Test
    @DisplayName("결과 필터가 있으면 DB 단독 발급 후보를 조회하지 않는다")
    void resultFilterExcludesDatabaseOnlyIssuances() {
        baseFixture();
        issuance(380, 101, 201, "ISSUED", seconds(-1));

        assertThat(source(query(101L, null, 201, null, null, 10)).issuances()).isEmpty();
        assertThat(source(query(101L, null, null, ReasonCode.STOCK_EXHAUSTED, null, 10))
                .issuances()).isEmpty();
    }

    @Test
    @DisplayName("미정의 DB enum은 행을 버리지 않고 MYSQL 원천 실패로 변환한다")
    void unknownEnumBecomesSourceUnavailable() {
        baseFixture();
        attempt(80, "ISSUE_RESULT", "unknown", 101, 201, null, 409,
                "UNKNOWN_REASON", seconds(-1));

        assertSourceUnavailable(() -> reader.read(query(101L, null, null, null, null, 10), SNAPSHOT));
    }

    @Test
    @DisplayName("SQL 실패는 쿼리나 PII를 상세 메시지에 담지 않고 MYSQL 원천 실패로 변환한다")
    void sqlFailureBecomesSanitizedSourceUnavailable() {
        baseFixture();
        writeJdbc.execute("RENAME TABLE issue_attempts TO issue_attempts_unavailable");
        try {
            assertThatThrownBy(() -> reader.read(query(101L, null, null, null, null, 10), SNAPSHOT))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(exception -> {
                        BusinessException business = (BusinessException) exception;
                        assertThat(business.getErrorCode())
                                .isEqualTo(AdminIssuanceInquiryErrorCode.SOURCE_UNAVAILABLE);
                        assertThat(causeChainText(business))
                                .doesNotContain(
                                        "SELECT", "issue_attempts", "app", "obs_inquiry",
                                        "101", "members", "coupons");
                        assertThat(business).hasNoCause();
                    });
        } finally {
            writeJdbc.execute("RENAME TABLE issue_attempts_unavailable TO issue_attempts");
        }
    }

    @Test
    @DisplayName("관측 계정은 SELECT만 성공하고 INSERT는 거부된다")
    void observationAccountRemainsReadOnlyByPrivilege() {
        baseFixture();
        NamedParameterJdbcTemplate observation = context.getBean(NamedParameterJdbcTemplate.class);
        assertThat(observation.queryForObject("SELECT COUNT(*) FROM members", java.util.Map.of(), Long.class))
                .isEqualTo(2L);
        assertThatThrownBy(() -> observation.update(
                "INSERT INTO grades(code, bit_value) VALUES ('VIP', 8)", java.util.Map.of()))
                .rootCause().hasMessageContaining("INSERT command denied");
    }

    @Test
    @DisplayName("조회 인덱스 migration은 2026082004 뒤에 적용되어 지정한 세 컬럼 순서를 만든다")
    void migrationCreatesExactInquiryIndex() {
        Integer previousRank = writeJdbc.queryForObject("""
                SELECT installed_rank FROM flyway_schema_history
                 WHERE version = '2026082004' AND success = 1
                """, Integer.class);
        Integer inquiryRank = writeJdbc.queryForObject("""
                SELECT installed_rank FROM flyway_schema_history
                 WHERE version = '2026082005' AND success = 1
                """, Integer.class);
        assertThat(inquiryRank).isGreaterThan(previousRank);

        List<String> columns = writeJdbc.queryForList("""
                SELECT column_name
                  FROM information_schema.statistics
                 WHERE table_schema = DATABASE()
                   AND table_name = 'issue_attempts'
                   AND index_name = 'ix_issue_attempts_member_occurred_id'
                 ORDER BY seq_in_index
                """, String.class);
        assertThat(columns).containsExactly("member_id", "occurred_at", "id");
    }

    @Test
    @DisplayName("reader는 관측 활성일 때만 조건부 등록된다")
    void registrationRequiresObservationEnabled() {
        new ApplicationContextRunner()
                .withUserConfiguration(ReaderContext.class)
                .run(ctx -> assertThat(ctx).doesNotHaveBean(AdminIssuanceInquirySourceReader.class));
        new ApplicationContextRunner()
                .withUserConfiguration(ReaderContext.class)
                .withPropertyValues("observation.datasource.enabled=true")
                .run(ctx -> assertThat(ctx).hasSingleBean(AdminIssuanceInquirySourceReader.class));
    }

    private static void assertSourceUnavailable(org.assertj.core.api.ThrowableAssert.ThrowingCallable call) {
        assertThatThrownBy(call).isInstanceOf(BusinessException.class)
                .satisfies(exception -> {
                    assertThat(((BusinessException) exception).getErrorCode())
                            .isEqualTo(AdminIssuanceInquiryErrorCode.SOURCE_UNAVAILABLE);
                    assertThat(exception).hasNoCause();
                });
    }

    private static String causeChainText(Throwable throwable) {
        StringBuilder text = new StringBuilder();
        Throwable current = throwable;
        while (current != null) {
            text.append(current.getClass().getName()).append(':')
                    .append(current.getMessage()).append('\n');
            current = current.getCause();
        }
        return text.toString();
    }

    private static AdminIssuanceInquiryReadResult read(AdminIssuanceInquiryQuery query) {
        return reader.read(query, SNAPSHOT);
    }

    private static AdminIssuanceInquirySource source(AdminIssuanceInquiryQuery query) {
        AdminIssuanceInquiryReadResult result = read(query);
        assertThat(result.availability()).isEqualTo(AdminIssuanceInquiryReadResult.Availability.AVAILABLE);
        return result.source();
    }

    private static AdminIssuanceInquiryQuery query(
            long memberId, Long couponId, Integer httpStatus, ReasonCode reasonCode,
            InquiryPosition before, int limit
    ) {
        return new AdminIssuanceInquiryQuery(memberId, couponId, httpStatus, reasonCode, before, limit);
    }

    private static Instant seconds(long offset) {
        return SNAPSHOT.plusSeconds(offset);
    }

    private static void baseFixture() {
        writeJdbc.update("INSERT INTO grades(code, bit_value) VALUES ('WELCOME', 1)");
        writeJdbc.update("""
                INSERT INTO members(id, membership_grade, created_at)
                VALUES (101, 'WELCOME', ?), (102, 'WELCOME', ?)
                """, seconds(-100), seconds(-100));
        writeJdbc.update("INSERT INTO brands(id, name, category) VALUES (1, 'brand', 'cafe')");
        writeJdbc.update("""
                INSERT INTO coupon_templates(
                    id, brand_id, name, policy_type, discount_amount, valid_days,
                    nth_week, day_of_week, start_time, duration_hours,
                    stock_per_occurrence, eligible_grades_mask, active, created_at, updated_at)
                VALUES (1, 1, 'template', 'FIXED_AMOUNT', 1000, 30,
                        1, 'MON', '10:00:00', 1, 100, 1, 1, ?, ?)
                """, seconds(-100), seconds(-100));
        writeJdbc.update("""
                INSERT INTO coupons(
                    id, template_id, brand_id, name, policy_type, discount_amount, valid_days,
                    eligible_grades_mask, open_at, close_at, status, created_at, generated_at)
                VALUES
                    (201, 1, 1, 'coupon-201', 'FIXED_AMOUNT', 1000, 30, 1, ?, ?, 'OPEN', ?, ?),
                    (202, 1, 1, 'coupon-202', 'FIXED_AMOUNT', 1000, 30, 1, ?, ?, 'OPEN', ?, ?),
                    (203, 1, 1, 'coupon-203', 'FIXED_AMOUNT', 1000, 30, 1, ?, ?, 'OPEN', ?, ?)
                """, seconds(-100), seconds(100), seconds(-100),
                seconds(-100), seconds(-90), seconds(110), seconds(-90), seconds(-90),
                seconds(-80), seconds(120), seconds(-80), seconds(-80));
    }

    private static void issuance(long id, long memberId, long couponId, String status, Instant issuedAt) {
        writeJdbc.update("""
                INSERT INTO issuances(
                    id, coupon_id, member_id, code, issued_grade, status,
                    issued_at, expires_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'WELCOME', ?, ?, ?, ?, ?)
                """, id, couponId, memberId, "CODE" + String.format("%012d", id), status,
                issuedAt, issuedAt.plusSeconds(3600), issuedAt, issuedAt);
    }

    private static void history(
            long id, long issuanceId, String eventType, String requestId, Instant occurredAt
    ) {
        writeJdbc.update("""
                INSERT INTO issuance_histories(
                    id, issuance_id, event_type, from_status, to_status, request_id, created_at)
                VALUES (?, ?, ?, NULL, 'ISSUED', ?, ?)
                """, id, issuanceId, eventType, requestId, occurredAt);
    }

    private static void attempt(
            long id, String eventType, String requestId, long memberId, long couponId,
            Long issuanceId, Integer httpStatus, String reasonCode, Instant occurredAt
    ) {
        String issuanceCode = issuanceId == null ? null : "CODE" + String.format("%012d", issuanceId);
        writeJdbc.update("""
                INSERT INTO issue_attempts(
                    id, schema_version, event_id, event_type, request_id, member_id, coupon_id,
                    issuance_id, issuance_code, http_status, reason_code, dependency, replayed,
                    occurred_at, engine_version, release_stage, queue_mode, producer_instance_id,
                    topic, kafka_partition, kafka_offset)
                VALUES (?, 1, UUID_TO_BIN(UUID(), 0), ?, ?, ?, ?, ?, ?, ?, ?, 'NONE', 0,
                        ?, 'V1', 'PROD', 'DIRECT', 'test', 'coupon.issue.attempt', 0, ?)
                """, id, eventType, requestId, memberId, couponId, issuanceId, issuanceCode,
                httpStatus, reasonCode, occurredAt, id);
    }

    private static HikariDataSource dataSource(String username, String password) {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(mysql.getJdbcUrl());
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setMaximumPoolSize(2);
        return dataSource;
    }

    @Configuration(proxyBeanMethods = false)
    @EnableTransactionManagement
    @Import(JdbcAdminIssuanceInquirySourceReader.class)
    static class ReaderContext {
        private static DataSource observationDataSource;

        @Bean
        @Qualifier("obs")
        NamedParameterJdbcTemplate observationNamedParameterJdbcTemplate() {
            return new NamedParameterJdbcTemplate(observationDataSource);
        }

        @Bean(name = "observationTransactionManager")
        PlatformTransactionManager observationTransactionManager() {
            return new JdbcTransactionManager(observationDataSource);
        }
    }
}
