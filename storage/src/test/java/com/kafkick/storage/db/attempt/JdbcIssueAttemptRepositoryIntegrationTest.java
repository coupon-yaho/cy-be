package com.kafkick.storage.db.attempt;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.UUID;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.kafkick.core.member.Grade;
import com.kafkick.core.observation.Dependency;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.IssuanceFlowEventFactory;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.observation.attempt.AttemptRecord;

/**
 * 실제 MySQL 에 태운다. <b>대역으로는 안 도는 것들이 여기서만 실행된다.</b>
 *
 * <ul>
 *   <li>{@code binary(16)} 바인딩이 {@code UUID_TO_BIN(x, 0)} 과 같은 바이트인지.
 *       다르면 {@code BIN_TO_UUID} 가 다른 값을 돌려주고, 같은 이벤트가 두 표현으로 들어간다.</li>
 *   <li>유니크 키 둘이 실제로 {@link org.springframework.dao.DuplicateKeyException} 으로 오는지.
 *       드라이버가 다른 예외로 올리면 그 catch 가 아무것도 안 잡고, 컨슈머가 무한 재시도한다.</li>
 *   <li>{@code datetime(6)} 반올림. {@code Instant} 의 나노초를 그대로 넣으면
 *       {@code ingested_at < occurred_at} 이 되어 지연이 음수로 나온다.</li>
 *   <li>CHECK 5종을 정상 이벤트가 통과하는지. 컬럼 순서를 한 칸 밀어 써도 컴파일은 되고,
 *       그 오류는 제약 위반이나 엉뚱한 값으로만 드러난다.</li>
 * </ul>
 */
class JdbcIssueAttemptRepositoryIntegrationTest {

    private static final DockerImageName IMAGE = DockerImageName.parse("mysql:8.4");
    private static final String TOPIC = "coupon.issue.attempt";

    private static MySQLContainer mysql;
    private static JdbcTemplate jdbcTemplate;
    private static JdbcIssueAttemptRepository repository;

    @BeforeAll
    static void startAndMigrate() {
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

        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
        jdbcTemplate = new JdbcTemplate((DataSource) dataSource);
        repository = new JdbcIssueAttemptRepository(jdbcTemplate);
    }

    @AfterAll
    static void stop() {
        if (mysql != null) {
            mysql.stop();
        }
    }

    @AfterEach
    void clear() {
        jdbcTemplate.update("DELETE FROM issue_attempts");
    }

    /** 이 티켓의 인수 조건 — <b>같은 이벤트를 재처리해도 중복이 안 생긴다.</b> */
    @Test
    void treatsTheSameEventIdAsAlreadyProcessed() {
        AttemptRecord first = record(issued(), 0, 10L);
        // 리밸런싱 후 재소비는 좌표가 같다. 아래 uk_kafka 테스트와 구분하려고 좌표만 바꾼 짝을 따로 둔다.
        AttemptRecord replay = new AttemptRecord(first.event(), TOPIC, 1, 99L, first.ingestedAt());

        assertThat(repository.append(first)).isTrue();
        assertThat(repository.append(replay)).as("event_id 가 같으면 이미 처리됨이다").isFalse();
        assertThat(rowCount()).isEqualTo(1);
    }

    /** 좌표가 같으면 event_id 가 달라도 이미 처리됨이다 — 정확히 리밸런싱 재소비의 모양이다. */
    @Test
    void treatsTheSameKafkaCoordinatesAsAlreadyProcessed() {
        assertThat(repository.append(record(issued(), 3, 42L))).isTrue();
        assertThat(repository.append(record(issued(), 3, 42L))).isFalse();
        assertThat(rowCount()).isEqualTo(1);
    }

    /** {@code UUID_TO_BIN(x, 0)} 과 같은 바이트여야 조회의 {@code BIN_TO_UUID} 가 맞는다. */
    @Test
    void storesTheEventIdInTheSameBinaryFormAsUuidToBin() {
        IssuanceFlowEvent event = issued();
        repository.append(record(event, 0, 1L));

        String storedAsUuid = jdbcTemplate.queryForObject(
                "SELECT BIN_TO_UUID(event_id) FROM issue_attempts", String.class);
        Integer matchesUuidToBin = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM issue_attempts WHERE event_id = UUID_TO_BIN(?, 0)",
                Integer.class, event.eventId().toString());

        assertThat(storedAsUuid).isEqualTo(event.eventId().toString());
        assertThat(matchesUuidToBin).isEqualTo(1);
    }

    /**
     * 나노초를 그대로 넣으면 MySQL 이 <b>반올림</b>해서 도착 시각이 발생 시각보다 앞서 보인다.
     *
     * <p>{@code occurredAt} 은 {@code .9999995} 로 두어 마이크로초 자리에서 <b>올림</b>이 나게
     * 하고, {@code ingestedAt} 은 그보다 1 마이크로초 뒤로 둔다. 자르지 않으면 저장된 두 값이
     * 같아지거나 순서가 뒤집힌다.
     */
    @Test
    void keepsIngestedAtFromDriftingBeforeOccurredAt() {
        Instant occurredAt = Instant.parse("2026-08-25T00:00:00Z").plusNanos(999_999_500L);
        Instant ingestedAt = occurredAt.plusNanos(500L);
        IssuanceFlowEvent event = issued(occurredAt);

        repository.append(new AttemptRecord(event, TOPIC, 0, 1L, ingestedAt));

        // 엄격 부등호다. >= 로 두면 이 테스트가 아무것도 검사하지 않는다 — 자르지 않으면
        // 두 값이 01.000000 으로 <b>같아지고</b>, >= 는 그것을 통과시킨다(일부러 절단을 빼고
        // 돌려서 확인했다: 초록이었다). 지연이 0 으로 뭉개지는 것도 음수가 되는 것과 같은 결손이다.
        Integer strictlyLater = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM issue_attempts WHERE ingested_at > occurred_at", Integer.class);
        assertThat(strictlyLater).as("반올림이 남으면 지연이 0 이나 음수가 된다").isEqualTo(1);
    }

    /** 정상 이벤트 네 종류가 CHECK 5종을 전부 통과한다. 컬럼 순서 오류도 여기서 드러난다. */
    @Test
    void storesEveryEventShapeThePipelineCanProduce() {
        IssuanceFlowEventFactory factory = new IssuanceFlowEventFactory(UUID::randomUUID);

        assertThat(repository.append(record(factory.issued(context(), 301L, "CODE0123456789AB"), 0, 1L))).isTrue();
        assertThat(repository.append(record(
                factory.issueRejected(context(), 409, ReasonCode.ALREADY_ISSUED, Dependency.NONE), 0, 2L))).isTrue();
        assertThat(repository.append(record(
                factory.entry(context(), 202, null, Dependency.NONE, 3L, 8L), 0, 3L))).isTrue();
        assertThat(repository.append(record(factory.issueAttempt(context()), 0, 4L))).isTrue();
        assertThat(repository.append(record(factory.admitted(context(), 8L), 0, 5L))).isTrue();

        assertThat(rowCount()).isEqualTo(5);
    }

    /**
     * 큐 컬럼 둘이 <b>이벤트 종류마다 제자리에</b> 들어간다.
     *
     * <p>위 테스트는 건수만 세고, 컬럼 값을 보는 테스트는 {@code ISSUE_RESULT} 하나뿐인데 거기서는
     * 두 값이 전부 null 이다. 그래서 {@code queue_position} 과 {@code queue_sequence} 를 서로
     * 바꿔 넣어도 지금까지는 아무 테스트도 안 깨졌다 — 두 컬럼이 같은 타입이라 컴파일도 통과한다.
     */
    @Test
    void storesQueueColumnsInTheRightPlaceForEachEventShape() {
        IssuanceFlowEventFactory factory = new IssuanceFlowEventFactory(UUID::randomUUID);

        repository.append(record(factory.entry(context(), 202, null, Dependency.NONE, 3L, 8L), 0, 1L));
        repository.append(record(factory.admitted(context(), 12L), 0, 2L));

        assertThat(jdbcTemplate.queryForMap(
                "SELECT queue_position, queue_sequence, http_status FROM issue_attempts"
                        + " WHERE event_type = 'ENTRY_RESULT'"))
                .containsEntry("queue_position", 3L)
                .containsEntry("queue_sequence", 8L)
                .containsEntry("http_status", 202);
        assertThat(jdbcTemplate.queryForMap(
                "SELECT queue_position, queue_sequence, http_status FROM issue_attempts"
                        + " WHERE event_type = 'QUEUE_ADMITTED'"))
                .as("QUEUE_ADMITTED 에는 위치가 없고 순번만 있다")
                .containsEntry("queue_position", null)
                .containsEntry("queue_sequence", 12L)
                .containsEntry("http_status", null);
    }

    /**
     * 이 티켓의 인수 조건 — <b>매핑 없는 ErrorCode 로 실패한 이벤트가 UNMAPPED 로 남는다.</b>
     *
     * <p>DEC-10. 매핑이 없다는 것은 대개 <b>새 오류가 방금 배포됐다</b>는 뜻이고, 그 순간이
     * 정확히 관제가 가장 필요한 때다. 미매핑을 유실로 처리하면 그때 그 실패가 통째로 사라져
     * "안 보이는 것" 과 "없는 것" 이 같은 모양이 된다.
     *
     * <p>적재 경로가 이 값을 특별 취급하지 않는다는 것을 여기서 고정한다 — {@code reason_code}
     * 는 {@code varchar(40)} 이고 {@code ck_attempt_reason} 은 4xx·5xx 에 사유가 있을 것만
     * 요구하므로, UNMAPPED 는 다른 사유와 완전히 같은 길로 간다.
     */
    @Test
    void keepsUnmappedReasonsInsteadOfDroppingThem() {
        IssuanceFlowEvent unmapped = new IssuanceFlowEventFactory(UUID::randomUUID)
                .issueRejected(context(), 500, ReasonCode.UNMAPPED, Dependency.NONE);

        assertThat(repository.append(record(unmapped, 0, 1L))).isTrue();

        assertThat(jdbcTemplate.queryForObject(
                "SELECT reason_code FROM issue_attempts", String.class)).isEqualTo("UNMAPPED");
        assertThat(rowCount()).isEqualTo(1);
    }

    /** 모든 컬럼이 제자리에 들어갔는지 값으로 확인한다. */
    @Test
    void mapsEveryColumnToTheRightValue() {
        IssuanceFlowEvent event = new IssuanceFlowEventFactory(UUID::randomUUID)
                .issued(context(), 301L, "CODE0123456789AB");

        repository.append(record(event, 4, 77L));

        assertThat(jdbcTemplate.queryForMap("SELECT * FROM issue_attempts"))
                .containsEntry("schema_version", 1)
                .containsEntry("event_type", "ISSUE_RESULT")
                .containsEntry("request_id", "request-1")
                .containsEntry("member_id", 101L)
                .containsEntry("coupon_id", 201L)
                .containsEntry("issuance_id", 301L)
                .containsEntry("issuance_code", "CODE0123456789AB")
                .containsEntry("grade", "GOLD")
                .containsEntry("http_status", 201)
                .containsEntry("reason_code", null)
                .containsEntry("dependency", "NONE")
                .containsEntry("replayed", false)
                .containsEntry("engine_version", "V3")
                .containsEntry("release_stage", "V3")
                .containsEntry("queue_mode", "ADAPTIVE")
                .containsEntry("benchmark_run_id", 901L)
                .containsEntry("producer_instance_id", "api-1")
                .containsEntry("topic", TOPIC)
                .containsEntry("kafka_partition", 4)
                .containsEntry("kafka_offset", 77L);
    }

    private static int rowCount() {
        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM issue_attempts", Integer.class);
        return count == null ? 0 : count;
    }

    private static AttemptRecord record(IssuanceFlowEvent event, int partition, long offset) {
        return new AttemptRecord(event, TOPIC, partition, offset,
                Instant.parse("2026-08-25T00:00:01Z"));
    }

    private static IssuanceFlowEvent issued() {
        return issued(Instant.parse("2026-08-25T00:00:00Z"));
    }

    private static IssuanceFlowEvent issued(Instant occurredAt) {
        return new IssuanceFlowEventFactory(UUID::randomUUID)
                .issued(context().withOccurredAt(occurredAt), 301L, "CODE0123456789AB");
    }

    private static IssuanceFlowEvent.Ctx context() {
        return new IssuanceFlowEvent.Ctx("request-1", 101L, 201L, Grade.GOLD, false,
                Instant.parse("2026-08-25T00:00:00Z"), EngineVersion.V3, ReleaseStage.V3,
                QueueMode.ADAPTIVE, 901L, "api-1");
    }
}
