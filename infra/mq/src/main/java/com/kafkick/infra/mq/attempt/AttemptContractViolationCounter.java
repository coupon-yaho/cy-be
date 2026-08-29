package com.kafkick.infra.mq.attempt;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.core.observation.IssuanceFlowEvent;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * 계약을 위반해 버린 레코드를 센다. <b>버렸다는 사실이 어딘가에 남아야 한다.</b>
 *
 * <h2>왜 DLT 가 아닌가</h2>
 *
 * {@code coupon.issue.attempt} 는 {@link com.kafkick.infra.mq.config.KafkaTopicConfig#TOPICS_WITHOUT_DLT}
 * 에 들어 있다. 격리 구역을 유지하는 비용이 이득보다 크다고 이미 판정된 토픽이다 — 관측 로그라
 * 한 건 버려도 회차 판정이 안 바뀐다.
 *
 * <p>그리고 여기에 공용 에러 핸들러를 붙이면 더 나쁘다. Spring 의 기본 목적지 해석기는
 * 원본 이름 + {@code .DLT} 로 보내는데 그 토픽은 선언되어 있지 않다 — <b>브로커가 RF1 으로
 * 새로 만든다.</b> 그 토픽은 {@code allTopics()} 밖이라 선언 검증도 영영 못 본다. 관측 로그를
 * 안 버리려다 아무도 모르는 토픽이 생기는 것이다.
 *
 * <p>OBS-15 명세는 "DLT 를 함께 둔다" 고 적었지만 그 토픽 계층의 결정과 정면으로 어긋난다.
 * 명세가 실제로 요구하는 것 — <b>격리하되 offset 은 넘긴다</b> — 은 DLT 없이도 지켜지고,
 * 이 카운터가 "무엇을 몇 건 버렸는지" 를 대신 남긴다.
 *
 * <h2>로그는 요약만</h2>
 *
 * 구버전 레코드가 한 파티션에 잔뜩 쌓인 배포 직후에는 이 경로가 초당 수천 번 열린다.
 * 건당 ERROR 를 찍으면 그 자체가 부하가 된다({@link AttemptFailureCounter} 와 같은 이유).
 */
public class AttemptContractViolationCounter {

    private static final Logger log = LoggerFactory.getLogger(AttemptContractViolationCounter.class);
    private static final long SUMMARY_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);

    private final Counter unknownEnum;
    private final Counter unsupportedSchema;
    private final Counter other;
    private final AtomicLong total = new AtomicLong();
    private final AtomicLong nextSummaryAt = new AtomicLong(System.nanoTime());

    public AttemptContractViolationCounter(MeterRegistry meterRegistry) {
        Objects.requireNonNull(meterRegistry, "meterRegistry");
        this.unknownEnum = counter(meterRegistry, DomainMeterNames.VIOLATION_UNKNOWN_ENUM);
        this.unsupportedSchema = counter(meterRegistry, DomainMeterNames.VIOLATION_UNSUPPORTED_SCHEMA);
        this.other = counter(meterRegistry, DomainMeterNames.VIOLATION_OTHER);
    }

    /**
     * 한 건을 버렸다고 기록한다.
     *
     * @param failure 역직렬화·계약 검증에서 나온 원인
     * @param topic 원본 토픽
     * @param partition 원본 파티션
     * @param offset 원본 오프셋. 사람이 손으로 열어 볼 때 필요한 유일한 좌표다
     */
    public void record(Throwable failure, String topic, int partition, long offset) {
        String reason = classify(failure);
        counterFor(reason).increment();
        long seen = total.incrementAndGet();
        logSummaryAtMostOncePerInterval(reason, topic, partition, offset, seen);
    }

    private Counter counterFor(String reason) {
        return switch (reason) {
            case DomainMeterNames.VIOLATION_UNKNOWN_ENUM -> unknownEnum;
            case DomainMeterNames.VIOLATION_UNSUPPORTED_SCHEMA -> unsupportedSchema;
            default -> other;
        };
    }

    /**
     * 원인을 닫힌 세 값 중 하나로 접는다.
     *
     * <p><b>원인 체인을 끝까지 본다.</b> 역직렬화 실패는 {@code SerializationException} 또는
     * {@code RecordDeserializationException} 으로 여러 겹 감싸여 온다. 최상위 타입만 보면
     * 지원하지 않는 {@code schemaVersion} 과 모르는 enum 값이 둘 다 {@code other} 로 떨어져,
     * 나눠 둔 의미가 사라진다.
     *
     * <p>미지원 스키마는 {@link IssuanceFlowEvent#UNSUPPORTED_SCHEMA_MESSAGE} 를 <b>공유 상수로</b>
     * 본다. 리터럴을 양쪽에 적어 두면 core 의 문구를 고치는 순간 이 분류가 조용히
     * {@code other} 로 떨어진다.
     *
     * <p>알 수 없는 enum 은 그렇게 못 한다 — 그 문구는 Jackson 것이라 우리 상수가 없다.
     * 라이브러리가 문구를 바꾸면 조용히 {@code other} 로 떨어진다. <b>잘못 분류될 뿐 이벤트를
     * 잃지는 않고</b> 셋의 합은 어떤 경우에도 맞으므로 그 대가를 받아들인다.
     */
    static String classify(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            String message = cause.getMessage();
            if (message == null) {
                continue;
            }
            if (message.contains(IssuanceFlowEvent.UNSUPPORTED_SCHEMA_MESSAGE)) {
                return DomainMeterNames.VIOLATION_UNSUPPORTED_SCHEMA;
            }
            if (message.contains("not one of the values accepted for Enum class")
                    || message.contains("Cannot deserialize value of type")
                            && message.contains("Enum")) {
                return DomainMeterNames.VIOLATION_UNKNOWN_ENUM;
            }
        }
        return DomainMeterNames.VIOLATION_OTHER;
    }

    private void logSummaryAtMostOncePerInterval(
            String reason, String topic, int partition, long offset, long seen) {
        long now = System.nanoTime();
        long due = nextSummaryAt.get();
        if (now - due < 0 || !nextSummaryAt.compareAndSet(due, now + SUMMARY_INTERVAL_NANOS)) {
            return;
        }
        log.warn("attempt 레코드를 계약 위반으로 버리고 offset 을 넘겼다."
                        + " 누적 {}건, reason={}, topic={}, partition={}, offset={}",
                seen, reason, topic, partition, offset);
    }

    private static Counter counter(MeterRegistry meterRegistry, String reason) {
        return Counter.builder(DomainMeterNames.ATTEMPT_CONTRACT_VIOLATIONS)
                .description("계약 위반으로 버린 attempt 레코드 수 (offset 은 넘어갔다)")
                .tag(DomainMeterNames.TAG_REASON, reason)
                .register(meterRegistry);
    }
}
