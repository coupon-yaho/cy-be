package com.kafkick.infra.mq.attempt;

import java.time.Clock;
import java.util.Objects;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.attempt.AttemptArchive;
import com.kafkick.core.observation.attempt.AttemptRecord;
import com.kafkick.infra.mq.config.AttemptConsumerConfig;
import com.kafkick.infra.mq.config.KafkaConsumerGroups;
import com.kafkick.infra.mq.config.KafkaTopicConfig;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * {@code issue_attempts} 에 적재한다. 그룹은 {@value KafkaConsumerGroups#ATTEMPT_ARCHIVE} 다.
 *
 * <h2>INSERT 가 끝난 뒤에 commit 한다</h2>
 *
 * 순서가 계약이다. 먼저 commit 하면 그 사이에 프로세스가 죽었을 때 그 구간이 <b>영원히</b>
 * 사라진다 — 재기동은 커밋된 offset 다음부터 읽는다. 반대로 두면 최악의 경우 같은 건을 두 번
 * 넣게 되는데, 그건 유니크 키 둘이 흡수한다. 잃는 쪽과 겹치는 쪽 중에 흡수 장치가 있는 쪽을
 * 고른 것이다.
 *
 * <h2>중복은 실패가 아니다</h2>
 *
 * 리밸런싱 후 재소비는 정상 경로다. 중복을 예외로 취급하면 offset 을 못 넘겨 같은 자리에서
 * 무한 재시도한다 — 유실을 막으려던 장치가 소비를 통째로 멈춘다. {@link AttemptArchive} 가
 * 중복을 흡수하고, 여기서는 그 결과를 세기만 한다.
 *
 * <p><b>적재 실패에서는 offset 을 넘기지 않는다.</b> live 쪽과 반대다. 화면 버퍼는 최근 몇 백
 * 건을 보여 주는 것이 전부라 한 건이 빠져도 그 다음 폴링이 덮지만, 이쪽은 보존 원본이다.
 * DB 가 잠깐 죽었을 때 그 구간을 그냥 넘기면 되돌릴 방법이 없다. 그 대가는 DB 장애가 길어지면
 * 이 컨슈머가 그룹에서 쫓겨나 리밸런싱이 도는 것이고, 그건 시끄러워서 눈에 띈다.
 */
public class AttemptArchiveConsumer {

    private final AttemptArchive archive;
    private final Clock clock;
    private final Counter inserted;
    private final Counter duplicate;

    public AttemptArchiveConsumer(AttemptArchive archive, Clock clock, MeterRegistry meterRegistry) {
        this.archive = Objects.requireNonNull(archive, "archive");
        this.clock = Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(meterRegistry, "meterRegistry");
        this.inserted = outcomeCounter(meterRegistry, DomainMeterNames.ARCHIVE_INSERTED);
        this.duplicate = outcomeCounter(meterRegistry, DomainMeterNames.ARCHIVE_DUPLICATE);
    }

    @KafkaListener(
            topics = KafkaTopicConfig.ISSUE_ATTEMPT,
            groupId = KafkaConsumerGroups.ATTEMPT_ARCHIVE,
            containerFactory = AttemptConsumerConfig.ARCHIVE_CONTAINER_FACTORY)
    public void consume(ConsumerRecord<String, IssuanceFlowEvent> record, Acknowledgment acknowledgment) {
        IssuanceFlowEvent event = record.value();
        if (event == null) {
            // 값이 진짜로 null 인 레코드다 — 역직렬화 실패가 아니다. 실패한 레코드는 컨테이너가
            // 리스너를 부르기 <b>전에</b> 던져서 에러 핸들러로 간다(실측으로 도달 0회를 확인했다).
            //
            // 이쪽에서는 남겨 두는 이유가 더 무겁다. archive 는 리스너 예외를 무한 재시도하므로,
            // null 하나가 NPE 가 되면 그 파티션의 적재가 영원히 멈춘다.
            acknowledgment.acknowledge();
            return;
        }
        boolean stored = archive.append(new AttemptRecord(
                event, record.topic(), record.partition(), record.offset(), clock.instant()));
        // 적재가 던지면 여기까지 못 온다. 그게 의도다 — 위 javadoc 참고.
        (stored ? inserted : duplicate).increment();
        acknowledgment.acknowledge();
    }

    private static Counter outcomeCounter(MeterRegistry meterRegistry, String outcome) {
        return Counter.builder(DomainMeterNames.ATTEMPT_ARCHIVE_OUTCOME)
                .description("issue_attempts 적재 결과 (duplicate 는 재소비의 정상값이다)")
                .tag(DomainMeterNames.TAG_ARCHIVE_OUTCOME, outcome)
                .register(meterRegistry);
    }
}
