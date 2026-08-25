package com.kafkick.core.observation.attempt;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

import com.kafkick.core.observation.IssuanceFlowEvent;

/**
 * 컨슈머가 손에 쥔 한 건 — 이벤트 본문에 <b>도착 좌표</b>를 붙인 것.
 *
 * <p>이 레코드가 core 에 있는 이유는 소비자와 구현자가 서로 다른 모듈이기 때문이다.
 * {@code infra:mq} 가 만들고 {@code storage}(archive) 와 {@code infra:redis}(live) 가 받는다.
 * 세 모듈의 공통 조상은 core 뿐이다. Kafka 타입은 하나도 싣지 않는다 — 좌표는 문자열과
 * 정수라서, core 가 브로커를 아는 일 없이 그대로 옮길 수 있다.
 *
 * <h2>{@code ingestedAt} 은 프로듀서가 아니라 컨슈머가 찍는다</h2>
 *
 * {@code occurredAt} 은 발급 API 인스턴스의 시계다. 인스턴스가 N 대면 그 시계들이 서로 다르고,
 * 그래서 {@code occurredAt} 정렬은 근사다. 도착 순서는 별도 사실이라 따로 남긴다.
 *
 * <p><b>마이크로초로 자른다.</b> {@code issue_attempts} 의 시각 컬럼은 {@code datetime(6)} 인데
 * MySQL 은 초과 정밀도를 <b>버리지 않고 반올림한다.</b> {@code Instant} 는 나노초라 자르지 않으면
 * 올림이 남아 {@code ingested_at < occurred_at} 이 되고, 지연이 음수로 나온다. 자르는 자리를
 * 여기로 둔 것은 archive 와 live 가 <b>같은 값</b>을 봐야 하기 때문이다 — 적재 쪽에서만 자르면
 * 화면과 DB 의 같은 이벤트가 서로 다른 도착 시각을 갖는다.
 *
 * @param event 발급 경로가 발행한 이벤트 본문
 * @param topic 도착한 토픽. 오프셋은 토픽 안에서만 유일하므로 좌표의 일부다
 * @param partition 도착한 파티션
 * @param offset 그 파티션 안의 오프셋
 * @param ingestedAt 컨슈머 도착 시각. 마이크로초로 잘려 있다
 */
public record AttemptRecord(
        IssuanceFlowEvent event,
        String topic,
        int partition,
        long offset,
        Instant ingestedAt
) {

    public AttemptRecord {
        Objects.requireNonNull(event, "event");
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(ingestedAt, "ingestedAt");
        if (topic.isBlank()) {
            throw new IllegalArgumentException("topic 이 비어 있습니다.");
        }
        if (partition < 0) {
            throw new IllegalArgumentException("partition 은 0 이상이어야 합니다.");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset 은 0 이상이어야 합니다.");
        }
        ingestedAt = ingestedAt.truncatedTo(ChronoUnit.MICROS);
    }
}
