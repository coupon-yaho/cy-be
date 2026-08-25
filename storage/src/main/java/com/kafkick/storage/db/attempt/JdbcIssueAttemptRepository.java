package com.kafkick.storage.db.attempt;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.UUID;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.attempt.AttemptArchive;
import com.kafkick.core.observation.attempt.AttemptRecord;

/**
 * {@code issue_attempts} 적재 어댑터. 쓰기 전용이다.
 *
 * <h2>중복은 예외로 오고, 예외를 흡수하는 것이 계약이다</h2>
 *
 * 유니크 키가 둘이다 — {@code uk_event(event_id)} 와 {@code uk_kafka(topic, partition, offset)}.
 * 리밸런싱 후 재소비는 정상 경로라 중복은 반드시 온다. 여기서 던지면 컨슈머가 offset 을 못
 * 넘기고 같은 자리에서 무한 재시도한다.
 *
 * <p><b>{@code INSERT IGNORE} 를 쓰지 않는다.</b> 어느 키에 걸렸는지 구분하지 못해
 * {@code uk_kafka} 위반까지 함께 삼킨다 — 토픽을 재생성하면 offset 이 0 부터 다시 시작하는데,
 * 그때 들어오는 <b>내용이 전혀 다른 정상 이벤트</b>가 옛 행과 좌표만 겹쳐 전량 거부된다.
 * 그 거부가 조용하면 대시보드에는 "attempt 0건" 이 그려진다. 유실이 아니라 전량 소실인데
 * 아무도 모르는 형태다. 건별 {@code INSERT} 의 {@link DuplicateKeyException} 을 잡으면 적어도
 * 그 건수가 {@code duplicate} 카운터로 드러난다.
 *
 * <p>{@code ON DUPLICATE KEY UPDATE id = id} 도 쓰지 않는다. 무시된 건수를 세려면 JDBC URL 에
 * {@code useAffectedRows=true} 가 필요한데, Connector/J 의 기본값은 {@code CLIENT_FOUND_ROWS}
 * 라 "값이 그대로인 갱신" 도 1 을 돌려준다 — 신규 적재와 중복 무시가 같은 값이라 카운터가
 * 아무것도 세지 못한다. URL 플래그 하나에 지표의 의미가 걸리는 구조를 만들지 않는다.
 *
 * <h2>두 시각을 마이크로초로 자른다</h2>
 *
 * 컬럼이 {@code datetime(6)} 인데 MySQL 은 초과 정밀도를 <b>버리지 않고 반올림한다.</b>
 * {@code Instant} 는 나노초라, 자르지 않으면 {@code occurred_at} 이 올라가고
 * {@code ingested_at} 은 안 올라가는 조합이 생겨 <b>지연이 음수로 나온다.</b>
 * {@code ingestedAt} 은 {@link AttemptRecord} 가 이미 자르지만 여기서 다시 자른다 — 이 SQL 이
 * 그 성질에 기대고 있다는 것을 이 자리에서 읽을 수 있어야 하고, 백필 같은 다른 경로가
 * 이 클래스를 부를 때도 같아야 한다.
 */
@Repository
public class JdbcIssueAttemptRepository implements AttemptArchive {

    private static final String INSERT_SQL = """
            INSERT INTO issue_attempts (
                schema_version, event_id, event_type, request_id, member_id, coupon_id,
                issuance_id, issuance_code, grade, http_status, reason_code, dependency,
                queue_position, queue_sequence, replayed, occurred_at, ingested_at,
                engine_version, release_stage, queue_mode, benchmark_run_id,
                producer_instance_id, topic, kafka_partition, kafka_offset
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private final JdbcTemplate jdbcTemplate;

    public JdbcIssueAttemptRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate, "jdbcTemplate");
    }

    @Override
    public boolean append(AttemptRecord record) {
        Objects.requireNonNull(record, "record");
        IssuanceFlowEvent event = record.event();
        try {
            jdbcTemplate.update(INSERT_SQL,
                    event.schemaVersion(),
                    toBinary(event.eventId()),
                    event.eventType().name(),
                    event.requestId(),
                    event.memberId(),
                    event.couponId(),
                    event.issuanceId(),
                    event.issuanceCode(),
                    nameOrNull(event.grade()),
                    event.httpStatus(),
                    nameOrNull(event.reasonCode()),
                    event.dependency().name(),
                    event.queuePosition(),
                    event.queueSequence(),
                    event.replayed(),
                    micros(event.occurredAt()),
                    micros(record.ingestedAt()),
                    event.engineVersion().name(),
                    event.releaseStage().name(),
                    event.queueMode().name(),
                    event.benchmarkRunId(),
                    event.producerInstanceId(),
                    record.topic(),
                    record.partition(),
                    record.offset());
            return true;
        } catch (DuplicateKeyException alreadyStored) {
            // uk_event 든 uk_kafka 든 '이미 처리됨' 이다. 정상 반환하고 컨슈머가 commit 하게 둔다.
            return false;
        }
    }

    /**
     * {@code UUID_TO_BIN(x, 0)} 과 같은 바이트 순서다 — msb 를 먼저, lsb 를 그대로.
     *
     * <p>{@code UUID_TO_BIN(x, 1)} 은 v1 UUID 의 시간 필드를 재배열해 인덱스 지역성을 얻는
     * 형태인데, 우리 {@code eventId} 는 v4(랜덤)라 재배열할 시간 필드가 없다. 두 형태를 섞으면
     * 조회의 {@code BIN_TO_UUID(event_id)} 가 다른 값을 돌려주고, 그 사실이 <b>중복 검출에도</b>
     * 영향을 준다 — 같은 이벤트가 두 표현으로 각각 들어간다.
     */
    private static byte[] toBinary(UUID eventId) {
        return ByteBuffer.allocate(16)
                .putLong(eventId.getMostSignificantBits())
                .putLong(eventId.getLeastSignificantBits())
                .array();
    }

    private static Timestamp micros(Instant instant) {
        return Timestamp.from(instant.truncatedTo(ChronoUnit.MICROS));
    }

    private static String nameOrNull(Enum<?> value) {
        return value == null ? null : value.name();
    }
}
