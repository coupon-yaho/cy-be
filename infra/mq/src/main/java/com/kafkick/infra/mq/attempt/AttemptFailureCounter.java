package com.kafkick.infra.mq.attempt;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.infra.mq.config.KafkaTopicConfig;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * 삼킨 attempt 발행 실패를 센다. <b>발급 경로에서 실행된다</b>는 전제로 만들어졌다.
 *
 * <h2>비용을 호출 스레드가 문다</h2>
 *
 * 실패 알림이 오는 스레드는 <b>둘</b>이다. 메타데이터가 아직 없어 {@code max.block.ms} 로
 * 막히면 콜백이 호출 스레드(발급 요청을 처리하던 톰캣 워커)에서 인라인으로 실행되고 — 실측으로
 * 확인했다 — 메타데이터가 이미 있으면 {@code kafka-producer-network-thread} 에서 온다.
 *
 * <p>어느 쪽이든 건당 비용을 최소로 둔다. 앞 경우는 비용이 그대로 발급 응답 시간이 되고,
 * 뒤 경우는 네트워크 스레드 하나가 그 프로듀서의 모든 전송을 처리하므로 여기서 지체하면
 * 발행 전체가 밀린다. 집계 자체는 두 스레드가 동시에 불러도 안전하다.
 *
 * <ul>
 *   <li>스택트레이스를 만들지 않는다. {@code log.warn(msg, throwable)} 은 logback 의 동기
 *       appender 가 워커를 붙잡는다 — {@code max.block.ms} 로 벌어 둔 것을 로그 I/O 가 되돌린다.</li>
 *   <li>요약 WARN 은 10초에 한 번만 낸다. 사람이 알아야 하는 것은 "새고 있다" 는 사실이고
 *       그건 한 번만 말하면 된다.</li>
 *   <li>{@link Counter} 를 생성자에서 만들어 필드로 들고 있는다. 실패마다
 *       {@code Counter.builder(...).register(...)} 를 부르면 건당 객체 3~4개와 해시 조회가
 *       붙는다 — 락은 아니지만 GC 압력이 되고, 그게 곧 측정 오염이다.</li>
 * </ul>
 *
 * <h2>원인은 태그로 남긴다</h2>
 *
 * 로그를 줄이면 원인이 사라지므로 카운터에 {@code reason} 태그를 붙인다. 직렬화 실패는
 * 코드 결함(롤백 대상)이고 타임아웃은 브로커·부하 문제라 대응이 다르다. 태그 값은
 * <b>닫힌 집합</b>이다 — 예외 클래스 이름을 그대로 넣으면 카디널리티가 열려 1초 scrape 가
 * 먼저 죽는다.
 *
 * <p><b>원인 체인을 끝까지 본다.</b> {@code KafkaTemplate} 은 이미 실패한 future 를 만나면
 * 그 원인을 {@code KafkaException("Send failed")} 로 감싸 다시 던진다. 최상위 타입만 보면
 * 브로커 다운이 {@code other} 로 분류되어, 원인별로 나눈 의미가 사라진다.
 */
public class AttemptFailureCounter {

    /**
     * 미터 이름은 core 가 소유한다 — 읽는 쪽은 api 인데 api 는 이 모듈을 {@code runtimeOnly} 로만
     * 보므로, 이름을 여기 두면 조회하는 쪽이 문자열을 옮겨 적는 것 말고는 방법이 없다.
     */
    public static final String FAILURE_COUNTER = DomainMeterNames.KAFKA_ATTEMPT_PUBLISH_FAILURES;

    static final String REASON_TIMEOUT = "timeout";
    static final String REASON_SERIALIZATION = "serialization";
    static final String REASON_OTHER = "other";

    private static final int MAX_CAUSE_DEPTH = 16;

    private static final long LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);

    private static final Logger log = LoggerFactory.getLogger(AttemptFailureCounter.class);

    private final Counter timeouts;
    private final Counter serializationFailures;
    private final Counter otherFailures;

    private final AtomicLong total = new AtomicLong();
    private final AtomicLong nextLogAtNanos = new AtomicLong(System.nanoTime());

    public AttemptFailureCounter(MeterRegistry meterRegistry) {
        // 실패가 나기 전에도 0 으로 노출시킨다. 없는 시계열과 0 인 시계열은 화면에서 다르게 읽힌다.
        this.timeouts = counter(meterRegistry, REASON_TIMEOUT);
        this.serializationFailures = counter(meterRegistry, REASON_SERIALIZATION);
        this.otherFailures = counter(meterRegistry, REASON_OTHER);
    }

    /**
     * 실패 하나를 기록한다. <b>발행 1회당 정확히 한 번만 불려야 한다</b> — 두 경로에서 세면
     * 실패율이 2배로 보이고 원인이 반씩 갈린다(실제로 그렇게 만들어 봤고, 발행 1회에 합계가
     * 2.0 이 나왔다).
     */
    public void record(Throwable failure, IssuanceFlowEvent event) {
        counterOf(reasonOf(failure)).increment();
        long count = total.incrementAndGet();
        if (log.isDebugEnabled()) {
            log.debug("attempt 이벤트 발행 실패 [{}] {}", identify(event), failure.toString());
        }
        logSummaryAtMostOncePerInterval(count, failure);
    }

    /** 지금까지 삼킨 실패 수. 테스트와 요약 로그가 쓴다. */
    public long total() {
        return total.get();
    }

    /**
     * {@code compareAndSet} 으로 한 스레드만 통과시킨다 — 실패가 몰릴 때 이 자리에 락을
     * 두면 그 락이 발급 경로의 병목이 된다.
     */
    private void logSummaryAtMostOncePerInterval(long count, Throwable failure) {
        long now = System.nanoTime();
        long due = nextLogAtNanos.get();
        if (now - due >= 0 && nextLogAtNanos.compareAndSet(due, now + LOG_INTERVAL_NANOS)) {
            // 스택트레이스를 넘기지 않는다. 포맷 비용을 호출 스레드가 문다.
            log.warn("attempt 이벤트 발행 실패 누적 {}건 — 최근 원인 {}. 발급은 계속한다",
                    count, failure.toString());
        }
    }

    /** 실패한 이벤트의 정체. PII 는 넣지 않는다 — 식별자만. */
    private String identify(IssuanceFlowEvent event) {
        if (event == null) {
            return "unknown";
        }
        return "eventId=" + event.eventId() + " requestId=" + event.requestId();
    }

    private Counter counterOf(String reason) {
        return switch (reason) {
            case REASON_TIMEOUT -> timeouts;
            case REASON_SERIALIZATION -> serializationFailures;
            default -> otherFailures;
        };
    }

    /**
     * {@code topic} 라벨은 지금 값이 하나뿐이다. 남겨 두는 이유 — persist 쪽 실패 카운터가
     * 붙는 순간(OBS-15) 같은 미터를 공유하면 그때 값이 둘이 된다. 그 계획이 없어지면 라벨을
     * 빼는 게 맞다. 미터 이름에 이미 {@code attempt} 가 들어 있다.
     */
    private static Counter counter(MeterRegistry meterRegistry, String reason) {
        return Counter.builder(FAILURE_COUNTER)
                .description("발급 경로에서 삼킨 attempt 이벤트 발행 실패")
                .tag(DomainMeterNames.TAG_TOPIC, KafkaTopicConfig.ISSUE_ATTEMPT)
                .tag(DomainMeterNames.TAG_REASON, reason)
                .register(meterRegistry);
    }

    static String reasonOf(Throwable failure) {
        // 깊이 상한을 둔다. 여기는 발급 요청 스레드가 직접 도는 자리라 순환 체인에 걸리면
        // 그 워커가 영영 소모된다 — 관측이 발급을 죽이면 안 된다는 제1원칙에 정면으로 걸린다.
        int depth = 0;
        for (Throwable cause = failure; cause != null && depth++ < MAX_CAUSE_DEPTH; cause = cause.getCause()) {
            if (cause instanceof TimeoutException) {
                return REASON_TIMEOUT;
            }
            if (cause instanceof SerializationException) {
                return REASON_SERIALIZATION;
            }
            if (cause == cause.getCause()) {
                break;
            }
        }
        return REASON_OTHER;
    }
}
