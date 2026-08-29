package com.kafkick.storage.db.migration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * issue_attempts 의 DB 제약을 실제 MySQL 에 마이그레이션을 적용한 뒤 검증한다.
 *
 * <p>CHECK 5종이 막는 값은 Consumer 가 만들 수 없다 — replayed 는 primitive boolean 이고, Kafka 좌표는
 * ConsumerRecord 에서 오며, 필드 조합은 IssuanceFlowEvent 의 생성자가 거른다. 이 제약들이 지키는 건
 * DLT 수동 보정 · 백필처럼 이 테이블에 직접 쓰는 경로다. 그래서 Consumer 테스트로는 한 번도 타지 않고,
 * 여기서 원시 INSERT 로 검증한다.
 *
 * <p>Spring 컨텍스트를 띄우지 않는다. storage 에는 @SpringBootConfiguration 이 없고 엔티티가 0개라
 * @DataJpaTest(=@RepositoryTest) 는 "JPA metamodel must not be empty" 로 죽는다
 * (ObservationDataSourceConfigTest 주석 참고). 검증 대상이 스키마뿐이므로 JDBC 로 충분하다.
 */
class IssueAttemptsMigrationTest {

    /** testFixtures 의 MySqlContainerConfig 는 latest 를 쓴다. 여기서는 운영(compose)과 같은 8.4 로 고정한다. */
    private static final DockerImageName IMAGE = DockerImageName.parse("mysql:8.4");

    /**
     * uk_kafka 가 (topic, partition, offset) 유니크라 행마다 offset 이 달라야 서로 안 부딪힌다.
     * Kafka 의 offset 을 흉내 내는 게 아니라 테스트 행을 가르는 일련번호다.
     * 테스트가 병렬로 돌면 ++ 가 같은 값을 두 번 내주고 엉뚱한 테스트가 uk_kafka 로 실패한다.
     */
    private static final java.util.concurrent.atomic.AtomicLong ROW_SEQUENCE =
            new java.util.concurrent.atomic.AtomicLong(1);

    private static MySQLContainer mysql;

    @BeforeAll
    static void startAndMigrate() {
        mysql = new MySQLContainer(IMAGE)
                .withDatabaseName("app")
                // 서버 설정 중 제약 판정에 영향을 주는 것만 운영과 맞춘다.
                // STRICT_TRANS_TABLES 가 없으면 길이 초과가 예외가 아니라 조용한 절단이 된다.
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
    }

    @AfterAll
    static void stop() {
        if (mysql != null) {
            mysql.stop();
        }
    }

    @AfterEach
    void clear() throws SQLException {
        try (Connection c = connection(); Statement s = c.createStatement()) {
            s.executeUpdate("DELETE FROM issue_attempts");
        }
    }

    @Nested
    @DisplayName("마이그레이션 적용")
    class Migration {

        /**
         * 적용된 마이그레이션 목록 전체를 고정하지 않는다 — OBS-14b 의 V2026082002 처럼 이 테이블과
         * 무관한 마이그레이션이 늘어도 깨지면 안 된다. 대신 A 대역 둘이 실제로 적용됐다는 것과
         * 그 뒤에 이 마이그레이션이 왔다는 것을 따로 단언한다.
         */
        @Test
        @DisplayName("날짜 버전이 A 대역보다 뒤에 성공으로 기록된다")
        void appliedAfterNumericBand() throws SQLException {
            assertThat(appliedVersions()).contains("1", "2", "2026082001");
            assertThat(installedRank("2026082001"))
                    .isGreaterThan(installedRank("1"))
                    .isGreaterThan(installedRank("2"));
            assertThat(query("SELECT success FROM flyway_schema_history WHERE version = '2026082001'"))
                    .isEqualTo("1");
        }

        @Test
        @DisplayName("컬럼 26개와 CHECK 5종이 만들어진다")
        void schemaShape() throws SQLException {
            assertThat(query("SELECT COUNT(*) FROM information_schema.columns"
                    + " WHERE table_schema = DATABASE() AND table_name = 'issue_attempts'"))
                    .isEqualTo("26");
            assertThat(query("SELECT COUNT(*) FROM information_schema.table_constraints"
                    + " WHERE constraint_schema = DATABASE() AND table_name = 'issue_attempts'"
                    + " AND constraint_type = 'CHECK'"))
                    .isEqualTo("5");
        }
    }

    @Nested
    @DisplayName("CHECK — 직접 쓰기 경로 방어")
    class Checks {

        @Test
        @DisplayName("replayed 는 0 과 1 만 받는다 — tinyint(1) 은 값을 제한하지 않는다")
        void replayedIsBoolean() {
            assertRejected(issueResult().replayed(2), "ck_attempt_replayed");
            assertRejected(issueResult().replayed(127), "ck_attempt_replayed");
            assertRejected(issueResult().replayed(-1), "ck_attempt_replayed");
        }

        @Test
        @DisplayName("schema_version 은 0 과 음수를 받지 않는다")
        void schemaVersionIsPositive() {
            assertRejected(issueResult().schemaVersion(0), "ck_attempt_schema_version");
            assertRejected(issueResult().schemaVersion(-1), "ck_attempt_schema_version");
        }

        @Test
        @DisplayName("Kafka 좌표는 음수를 받지 않는다 — 좌표가 어긋나면 uk_kafka 멱등이 무너진다")
        void kafkaCoordinatesAreNonNegative() {
            assertRejected(issueResult().partition(-1), "ck_attempt_kafka_coords");
            assertRejected(issueResult().offset(-5), "ck_attempt_kafka_coords");
        }

        @Test
        @DisplayName("offset 0 은 정상값이라 통과한다")
        void offsetZeroIsValid() {
            assertAccepted(issueResult().partition(0).offset(0));
        }

        @Test
        @DisplayName("http_status 가 없으면 발급 식별자도 결과 코드도 가질 수 없다")
        void nullHttpStatusCannotCarryResult() {
            // CHECK 는 FALSE 일 때만 거부하고 NULL 이면 통과시킨다.
            // http_status IS NOT NULL 을 함께 검사하지 않으면 이 두 건이 그대로 들어온다.
            assertRejected(queueAdmitted().issuance(7L, "CODE0123456789AB"), "ck_attempt_issue_ids");
            assertRejected(queueAdmitted().reason("STOCK_EXHAUSTED"), "ck_attempt_reason");
        }

        @Test
        @DisplayName("발급 식별자는 201 에서만, 그리고 둘이 함께여야 한다")
        void issuanceIdentifiersOnlyOn201() {
            assertRejected(issueResult().status(409).reason("ALREADY_ISSUED")
                    .issuance(7L, "CODE0123456789AB"), "ck_attempt_issue_ids");
            assertRejected(issueResult().status(201).reason(null).issuance(null, "CODE0123456789AB"),
                    "ck_attempt_issue_ids");
            assertRejected(issueResult().status(201).reason(null).issuance(7L, null),
                    "ck_attempt_issue_ids");
        }

        @Test
        @DisplayName("실패 응답에는 결과 코드가 있어야 하고 성공 응답에는 없어야 한다")
        void reasonCodeMatchesHttpStatus() {
            assertRejected(issueResult().status(409).reason(null), "ck_attempt_reason");
            assertRejected(issueResult().status(202).reason("STOCK_EXHAUSTED"), "ck_attempt_reason");
        }

        /**
         * OBS-24 가 EventType 을 4종으로 늘리며 "DDL 변경 불필요" 로 판단한 근거를 실제 DB 로 닫는다.
         * varchar(20) 수용과 CHECK 3종의 NULL 갈래 통과를 손계산이 아니라 여기서 확인한다.
         */
        @Test
        @DisplayName("ISSUE_ATTEMPT 행이 DDL 변경 없이 적재된다")
        void issueAttemptRowIsAccepted() throws SQLException {
            assertAccepted(issueAttempt());

            assertThat(query("SELECT event_type FROM issue_attempts")).isEqualTo("ISSUE_ATTEMPT");
        }

        @Test
        @DisplayName("ISSUE_ATTEMPT 도 결과 컬럼을 가질 수 없다 — http_status 가 NULL 이라 NULL 갈래로 샌다")
        void issueAttemptCannotCarryResultColumns() {
            assertRejected(issueAttempt().issuance(7L, "CODE0123456789AB"), "ck_attempt_issue_ids");
            assertRejected(issueAttempt().reason("STOCK_EXHAUSTED"), "ck_attempt_reason");
        }

        @Test
        @DisplayName("이벤트 유형별 정상 조합은 모두 통과한다")
        void validRowsPass() {
            assertAccepted(queueAdmitted());
            assertAccepted(issueAttempt());
            assertAccepted(issueResult().status(201).reason(null).issuance(7L, "CODE0123456789AB"));
            assertAccepted(issueResult().status(409).reason("STOCK_EXHAUSTED"));
            assertAccepted(issueResult().status(202).reason(null));
        }
    }

    @Nested
    @DisplayName("멱등 — 유니크 키")
    class Idempotency {

        @Test
        @DisplayName("같은 event_id 는 두 번 적재되지 않는다")
        void duplicateEventIdRejected() {
            Row first = issueResult();
            assertAccepted(first);
            assertRejected(issueResult().eventId(first.eventId).offset(99), "uk_event");
        }

        @Test
        @DisplayName("같은 토픽의 같은 (partition, offset) 은 두 번 적재되지 않는다")
        void duplicateKafkaCoordinateRejected() {
            assertAccepted(issueResult().partition(3).offset(17));
            assertRejected(issueResult().partition(3).offset(17), "uk_kafka");
        }

        @Test
        @DisplayName("토픽이 다르면 같은 (partition, offset) 도 별개다 — 토픽 재생성 시 offset 이 0 부터 다시 시작한다")
        void sameCoordinateOnAnotherTopicIsDistinct() {
            assertAccepted(issueResult().partition(3).offset(17));
            assertAccepted(issueResult().topic("coupon.issue.attempt.v2").partition(3).offset(17));
            assertThat(rowCount()).isEqualTo(2);
        }

        @Test
        @DisplayName("ON DUPLICATE KEY UPDATE 로 넣으면 중복이 예외 없이 넘어가고 행도 늘지 않는다")
        void upsertSwallowsDuplicate() throws SQLException {
            Row row = issueResult();
            assertAccepted(row);
            assertThatCode(() -> upsert(issueResult().eventId(row.eventId).offset(98)))
                    .doesNotThrowAnyException();
            assertThat(rowCount()).isEqualTo(1);
        }

        /**
         * Connector/J 는 기본으로 CLIENT_FOUND_ROWS 를 켠다. 그러면 "값이 그대로인 갱신" 이 0 이 아니라 1 을
         * 돌려주므로, 신규 적재(1)와 중복 무시(1)를 affectedRows 로 구분할 수 없다.
         * OBS-15 가 무시 건수를 세려면 URL 에 useAffectedRows=true 를 붙이거나,
         * 설계 스케치처럼 건별 INSERT 의 DuplicateKeyException 을 잡아 세야 한다.
         */
        @Test
        @DisplayName("중복 건수를 affectedRows 로 세려면 useAffectedRows=true 가 필요하다")
        void affectedRowsNeedsExplicitFlag() throws SQLException {
            Row row = issueResult();
            assertAccepted(row);
            Row duplicate = issueResult().eventId(row.eventId).offset(97);

            assertThat(upsert(duplicate)).isOne();
            assertThat(upsertWith(duplicate, "useAffectedRows=true")).isZero();
        }
    }

    @Nested
    @DisplayName("컬럼 계약")
    class Columns {

        @Test
        @DisplayName("ingested_at 을 빼도 DEFAULT 로 채워진다 — 누락이 1364 무한 재시도가 되지 않는다")
        void ingestedAtDefaulted() throws SQLException {
            assertAccepted(issueResult());
            assertThat(query("SELECT COUNT(*) FROM issue_attempts WHERE ingested_at IS NOT NULL"))
                    .isEqualTo("1");
        }

        @Test
        @DisplayName("producer_instance_id 는 계약 상한인 100 자를 받는다")
        void producerInstanceIdHoldsContractLength() {
            assertAccepted(issueResult().producerInstanceId("x".repeat(100)));
        }

        @Test
        @DisplayName("계약이 보장하는 다섯 컬럼은 NULL 을 받지 않는다")
        void contractGuaranteedColumnsAreNotNull() throws SQLException {
            assertThat(nullableColumns()).doesNotContain(
                    "dependency", "engine_version", "release_stage", "queue_mode", "replayed");
        }
    }

    // --- 헬퍼 -------------------------------------------------------------

    private static final String INSERT = """
            INSERT INTO issue_attempts (
                schema_version, event_id, event_type, request_id, member_id, coupon_id,
                issuance_id, issuance_code, http_status, reason_code, dependency, replayed,
                occurred_at, engine_version, release_stage, queue_mode, producer_instance_id,
                topic, kafka_partition, kafka_offset)
            VALUES (?, UUID_TO_BIN(?), ?, ?, 1, 1, ?, ?, ?, ?, 'NONE', ?,
                    NOW(6), 'V1', 'V2_1', 'OFF', ?, ?, ?, ?)
            """;

    /** 컬럼 하나만 바꿔 가며 INSERT 하려고 둔 값 묶음이다. */
    private static final class Row {
        int schemaVersion = 1;
        String eventId = java.util.UUID.randomUUID().toString();
        String eventType = "ISSUE_RESULT";
        String requestId = java.util.UUID.randomUUID().toString();
        Long issuanceId;
        String issuanceCode;
        Integer httpStatus = 409;
        String reasonCode = "STOCK_EXHAUSTED";
        int replayed = 0;
        String producerInstanceId = "api-1";
        String topic = "coupon.issue.attempt";
        int partition = 0;
        long offset = ROW_SEQUENCE.getAndIncrement();

        Row schemaVersion(int v) { this.schemaVersion = v; return this; }
        Row eventId(String v) { this.eventId = v; return this; }
        Row status(Integer v) { this.httpStatus = v; return this; }
        Row reason(String v) { this.reasonCode = v; return this; }
        Row replayed(int v) { this.replayed = v; return this; }
        Row topic(String v) { this.topic = v; return this; }
        Row partition(int v) { this.partition = v; return this; }
        Row offset(long v) { this.offset = v; return this; }
        Row producerInstanceId(String v) { this.producerInstanceId = v; return this; }

        Row issuance(Long id, String code) {
            this.issuanceId = id;
            this.issuanceCode = code;
            return this;
        }
    }

    private static Row issueResult() {
        return new Row();
    }

    /** QUEUE_ADMITTED 는 http_status 도 reason_code 도 없다. ISSUE_ATTEMPT 도 같다(OBS-24). */
    private static Row queueAdmitted() {
        Row row = new Row();
        row.eventType = "QUEUE_ADMITTED";
        row.requestId = null;
        row.httpStatus = null;
        row.reasonCode = null;
        return row;
    }

    /**
     * ISSUE_ATTEMPT 는 결과가 아니라 단계라 결과 컬럼이 전부 NULL 이다.
     *
     * <p>여기서 request_id 를 채우는 것은 자바 계약({@code IssuanceFlowEvent})이 요구하기 때문이고,
     * <b>DB 는 강제하지 않는다</b> — request_id 는 NULL 허용이고 CHECK 5종 어디에도 없다. 백필과
     * DLT 수동 보정은 request_id 없는 attempt 행을 넣을 수 있다.
     */
    private static Row issueAttempt() {
        Row row = new Row();
        row.eventType = "ISSUE_ATTEMPT";
        row.httpStatus = null;
        row.reasonCode = null;
        return row;
    }

    private static void assertAccepted(Row row) {
        assertThatCode(() -> insert(row)).doesNotThrowAnyException();
    }

    private static void assertRejected(Row row, String constraint) {
        assertThatThrownBy(() -> insert(row)).hasMessageContaining(constraint);
    }

    private static void insert(Row row) throws SQLException {
        try (Connection c = connection(); PreparedStatement ps = c.prepareStatement(INSERT)) {
            bind(ps, row);
            ps.executeUpdate();
        }
    }

    private static int upsert(Row row) throws SQLException {
        return upsertWith(row, null);
    }

    private static int upsertWith(Row row, String urlParam) throws SQLException {
        try (Connection c = connection(urlParam);
                PreparedStatement ps = c.prepareStatement(INSERT + " ON DUPLICATE KEY UPDATE id = id")) {
            bind(ps, row);
            return ps.executeUpdate();
        }
    }

    private static void bind(PreparedStatement ps, Row row) throws SQLException {
        ps.setInt(1, row.schemaVersion);
        ps.setString(2, row.eventId);
        ps.setString(3, row.eventType);
        ps.setString(4, row.requestId);
        ps.setObject(5, row.issuanceId);
        ps.setString(6, row.issuanceCode);
        ps.setObject(7, row.httpStatus);
        ps.setString(8, row.reasonCode);
        ps.setInt(9, row.replayed);
        ps.setString(10, row.producerInstanceId);
        ps.setString(11, row.topic);
        ps.setInt(12, row.partition);
        ps.setLong(13, row.offset);
    }

    private static long rowCount() {
        try {
            return Long.parseLong(query("SELECT COUNT(*) FROM issue_attempts"));
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static int installedRank(String version) throws SQLException {
        java.util.List<String> ranks = queryList(
                "SELECT installed_rank FROM flyway_schema_history WHERE version = '" + version + "'");
        assertThat(ranks).as("마이그레이션 %s 가 적용되지 않았다", version).hasSize(1);
        return Integer.parseInt(ranks.getFirst());
    }

    private static java.util.List<String> appliedVersions() throws SQLException {
        return queryList("SELECT version FROM flyway_schema_history ORDER BY installed_rank");
    }

    private static java.util.List<String> nullableColumns() throws SQLException {
        return queryList("SELECT column_name FROM information_schema.columns"
                + " WHERE table_schema = DATABASE() AND table_name = 'issue_attempts'"
                + " AND is_nullable = 'YES'");
    }

    private static String query(String sql) throws SQLException {
        return queryList(sql).getFirst();
    }

    private static java.util.List<String> queryList(String sql) throws SQLException {
        try (Connection c = connection();
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery(sql)) {
            java.util.List<String> values = new java.util.ArrayList<>();
            while (rs.next()) {
                values.add(rs.getString(1));
            }
            return values;
        }
    }

    private static Connection connection() throws SQLException {
        return connection(null);
    }

    private static Connection connection(String urlParam) throws SQLException {
        String url = mysql.getJdbcUrl();
        if (urlParam != null) {
            url += (url.contains("?") ? "&" : "?") + urlParam;
        }
        return DriverManager.getConnection(url, mysql.getUsername(), mysql.getPassword());
    }
}
