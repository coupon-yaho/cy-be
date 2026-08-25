package com.kafkick.infra.mq.attempt;

import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.attempt.AttemptLiveEntry;
import com.kafkick.core.observation.attempt.AttemptLiveSink;
import com.kafkick.core.observation.attempt.AttemptRecord;
import com.kafkick.infra.mq.config.AttemptConsumerConfig;
import com.kafkick.infra.mq.config.KafkaConsumerGroups;
import com.kafkick.infra.mq.config.KafkaTopicConfig;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * 관제 화면이 읽는 버퍼를 채운다. 그룹은 {@value KafkaConsumerGroups#ATTEMPT_LIVE} 다.
 *
 * <h2>archive 와 그룹이 달라야 한다</h2>
 *
 * 같은 {@code group.id} 를 쓰면 두 컨슈머가 파티션을 <b>나눠</b> 가진다. 화면과 적재가 각각
 * 절반씩만 보게 되는데, 둘 다 "정상 동작" 처럼 보인다 — 화면에는 이벤트가 흐르고 DB 에도
 * 행이 쌓인다. 어긋난다는 사실은 두 원천을 대조해야만 드러나고, 아무도 대조하지 않는다.
 *
 * <h2>여기서 실패를 끝낸다</h2>
 *
 * Redis 가 죽어도 예외를 밖으로 내보내지 않는다. 내보내면 컨테이너가 재시도하고, 재시도가
 * {@code max.poll.interval.ms} 를 넘기면 그룹에서 쫓겨나 리밸런싱이 돈다 — 그 리밸런싱은
 * <b>같은 토픽을 읽는 archive 에는 영향이 없지만</b>, live 컨슈머 자신이 그 뒤로 계속
 * 쫓겨났다 들어오기를 반복한다. 화면 하나의 장애가 소비 계층 전체의 상태가 된다.
 *
 * <p>그래서 삼키고 offset 을 넘긴다. <b>대가는 그 사이의 이벤트가 화면에서 영영 사라지는
 * 것</b>이고, 그것은 {@link DomainMeterNames#ATTEMPT_LIVE_APPEND_FAILURES} 로 드러난다.
 * 이 토픽이 TPS·성공률의 원천이 아니기 때문에 낼 수 있는 대가다.
 */
public class AttemptLiveConsumer {

    private static final Logger log = LoggerFactory.getLogger(AttemptLiveConsumer.class);
    private static final long SUMMARY_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);

    private final AtomicLong nextSummaryAt = new AtomicLong(System.nanoTime());
    private final AttemptLiveSink sink;
    private final StratifiedSampler sampler;
    private final Clock clock;
    private final Counter admitted;
    private final Counter dropped;
    private final Counter appendFailures;

    public AttemptLiveConsumer(
            AttemptLiveSink sink,
            StratifiedSampler sampler,
            Clock clock,
            MeterRegistry meterRegistry
    ) {
        this.sink = Objects.requireNonNull(sink, "sink");
        this.sampler = Objects.requireNonNull(sampler, "sampler");
        this.clock = Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(meterRegistry, "meterRegistry");
        this.admitted = samplingCounter(meterRegistry, DomainMeterNames.SAMPLING_ADMITTED);
        this.dropped = samplingCounter(meterRegistry, DomainMeterNames.SAMPLING_DROPPED);
        this.appendFailures = Counter.builder(DomainMeterNames.ATTEMPT_LIVE_APPEND_FAILURES)
                .description("live 버퍼 쓰기에서 삼킨 실패 수 (offset 은 넘어갔다)")
                .register(meterRegistry);
    }

    @KafkaListener(
            topics = KafkaTopicConfig.ISSUE_ATTEMPT,
            groupId = KafkaConsumerGroups.ATTEMPT_LIVE,
            containerFactory = AttemptConsumerConfig.LIVE_CONTAINER_FACTORY)
    public void consume(ConsumerRecord<String, IssuanceFlowEvent> record, Acknowledgment acknowledgment) {
        try {
            project(record);
        } finally {
            // finally 다. 위에서 무엇이 나든 offset 은 넘어간다 — 안 넘기면 같은 레코드에서
            // 무한 재시도이고, 관측 로그 한 건 때문에 그 파티션의 화면이 통째로 멈춘다.
            //
            // ⚠️ 이건 불변식이지 지금 도달 가능한 방어가 아니다. project() 가 sink 예외를 이미
            //    자기 안에서 잡으므로, 남은 경로(샘플러의 맵 연산 · 정제)는 실제로 던지지
            //    않는다. 그래서 이 finally 를 지워도 깨지는 테스트가 없다(일부러 지워서
            //    확인했다 — 5개 전부 초록이었다). 나중에 이 try 안에 던지는 코드가 들어와도
            //    offset 이 막히지 않게 하려고 이 모양으로 둔 것이고, 지금 무언가를 막고
            //    있다고 읽으면 안 된다. 실제로 검증된 것은 sink 장애 경로이고
            //    AttemptConsumerKafkaIntegrationTest 가 그것을 실제 브로커에서 고정한다.
            acknowledgment.acknowledge();
        }
    }

    private void project(ConsumerRecord<String, IssuanceFlowEvent> record) {
        IssuanceFlowEvent event = record.value();
        if (event == null) {
            // 값이 진짜로 null 인 레코드다 — 역직렬화 실패가 아니다. 실패한 레코드는 컨테이너가
            // 리스너를 부르기 <b>전에</b> 던져서 에러 핸들러로 가므로 여기 오지 않는다(실측:
            // 이 분기에 프로브를 심고 알 수 없는 enum 을 태웠더니 도달 0회였다).
            //
            // 그래도 남겨 둔다. 이 토픽에 null 값이 올 일은 없지만, 오면 아래에서 NPE 가 되고
            // live 는 그것을 계약 위반으로 잘못 집계한다.
            return;
        }
        if (!sampler.sample(event)) {
            dropped.increment();
            return;
        }
        admitted.increment();
        try {
            sink.append(AttemptLiveEntry.from(new AttemptRecord(
                    event, record.topic(), record.partition(), record.offset(), clock.instant())));
        } catch (RuntimeException failure) {
            appendFailures.increment();
            logCauseAtMostOncePerInterval(failure);
        }
    }

    /**
     * 삼킨 실패의 <b>원인</b>을 남긴다. 카운터만으로는 연결 거부·타임아웃·직렬화 오류가
     * 구분되지 않아, 대응이 전혀 다른 셋이 같은 숫자로 보인다.
     *
     * <p>간격을 제한한다. Redis 가 끊긴 구간에서는 이 경로가 초당 수백 번 열리고, 이 저장소는
     * 동기 ConsoleAppender 라 건당 로그가 그대로 컨슈머 처리량이 된다.
     *
     * <p><b>이벤트를 싣지 않는다.</b> 예외의 클래스 이름과 메시지만 남긴다 — 페이로드를 통째로
     * 찍는 것은 이 저장소가 {@code LoggingProducerListener} 를 걷어내면서 이미 금지한 형태다.
     */
    private void logCauseAtMostOncePerInterval(RuntimeException failure) {
        long now = System.nanoTime();
        long due = nextSummaryAt.get();
        if (now - due < 0 || !nextSummaryAt.compareAndSet(due, now + SUMMARY_INTERVAL_NANOS)) {
            return;
        }
        log.warn("live 버퍼 쓰기를 삼켰다. offset 은 넘어간다. cause={}, message={}",
                failure.getClass().getSimpleName(), failure.getMessage());
    }

    private static Counter samplingCounter(MeterRegistry meterRegistry, String decision) {
        return Counter.builder(DomainMeterNames.ATTEMPT_LIVE_SAMPLED)
                .description("층화 샘플링 판정 수 (합은 컨슈머가 받은 건수다)")
                .tag(DomainMeterNames.TAG_SAMPLING_DECISION, decision)
                .register(meterRegistry);
    }
}
