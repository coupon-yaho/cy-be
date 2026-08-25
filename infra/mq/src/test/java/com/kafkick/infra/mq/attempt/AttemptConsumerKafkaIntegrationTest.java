package com.kafkick.infra.mq.attempt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import com.kafkick.core.member.Grade;
import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.IssuanceFlowEventFactory;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.observation.attempt.AttemptArchive;
import com.kafkick.core.observation.attempt.AttemptLiveEntry;
import com.kafkick.core.observation.attempt.AttemptLiveSink;
import com.kafkick.core.observation.attempt.AttemptRecord;
import com.kafkick.infra.mq.config.AttemptConsumerConfig;
import com.kafkick.infra.mq.config.KafkaConnectionProperties;
import com.kafkick.infra.mq.config.KafkaConsumerGroups;
import com.kafkick.infra.mq.config.KafkaTopicConfig;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import tools.jackson.databind.json.JsonMapper;

/**
 * 실제 브로커에 태운다. <b>이 티켓의 인수 조건 둘은 여기서만 증명된다.</b>
 *
 * <ul>
 *   <li><b>알 수 없는 enum 한 건이 컨슈머를 멈추지 않고 offset 이 넘어간다.</b> 이 성질은
 *       역직렬화({@code ErrorHandlingDeserializer}) → 에러 핸들러({@code DefaultErrorHandler})
 *       → 컨테이너의 offset 커밋, 세 계층이 맞물려야 성립하고 <b>그중 둘은 우리 코드가
 *       아니다.</b> 대역은 그 맞물림을 아예 실행하지 않는다. 여기서 깨지면 poison message 로
 *       그 파티션의 소비가 영원히 멈추는데, 앱은 정상 기동한 채 로그만 반복된다.</li>
 *   <li><b>두 그룹이 각자 전체를 받는다.</b> {@code AttemptConsumerConfigTest} 는 설정값이
 *       다르다는 것까지만 본다. 파티션을 나눠 갖는지 아닌지는 브로커의 그룹 코디네이터가
 *       정하는 일이라, 실제로 붙여 봐야 안다.</li>
 * </ul>
 */
@Testcontainers(disabledWithoutDocker = true)
class AttemptConsumerKafkaIntegrationTest {

    /**
     * <b>compose.yml 의 브로커와 같은 이미지다.</b> 다른 버전으로 검증하면 여기서 통과한 성질이
     * 배포에서 성립한다는 보장이 없다 — 컨슈머 그룹 프로토콜과 offset 커밋은 브로커 버전이
     * 정하는 동작이라, 이 테스트가 지키려는 것이 정확히 그 계층이다.
     */
    @Container
    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:4.1.0"));

    private static final int PARTITIONS = 2;
    private static final Duration SETTLE = Duration.ofSeconds(20);

    private static final IssuanceFlowEventFactory FACTORY =
            new IssuanceFlowEventFactory(UUID::randomUUID);
    private static final JsonMapper MAPPER = JsonMapper.builder().findAndAddModules().build();

    private static final List<MessageListenerContainer> CONTAINERS = new ArrayList<>();

    /**
     * 클래스에 하나다. 발행마다 새로 만들면 <b>건마다 메타데이터를 다시 받아온다</b> —
     * 그 비용이 테스트 시간의 대부분이었다(실측: 건당 수 초).
     */
    private static KafkaProducer<String, String> producer;

    @BeforeAll
    static void createTopic() throws Exception {
        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic(KafkaTopicConfig.ISSUE_ATTEMPT, PARTITIONS, (short) 1))).all().get();
        }
        producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.ACKS_CONFIG, "all"), new StringSerializer(), new StringSerializer());
    }

    @AfterAll
    static void stopContainers() {
        CONTAINERS.forEach(MessageListenerContainer::stop);
        if (producer != null) {
            producer.close();
        }
    }

    /**
     * 이 티켓의 인수 조건 — <b>알 수 없는 enum 하나가 컨슈머를 멈추지 않고 offset 이 넘어간다.</b>
     *
     * <p>정상 · 오염 · 정상 순서로 넣는다. 오염된 한 건에서 멈추면 세 번째가 영영 안 온다 —
     * 그리고 그 상태는 예외로 드러나지 않고, 로그가 반복될 뿐이다.
     */
    @Test
    void keepsConsumingPastAnUnknownEnumAndAdvancesTheOffset() throws Exception {
        List<AttemptLiveEntry> seen = new CopyOnWriteArrayList<>();
        startLive(seen::add, "poison-live");

        publish(0, MAPPER.writeValueAsString(attempt(101L)));
        // eventType 이 계약에 없는 값이다. IssuanceFlowEvent 의 역직렬화가 여기서 터진다.
        publish(0, MAPPER.writeValueAsString(attempt(102L))
                .replace("\"ISSUE_ATTEMPT\"", "\"WORMHOLE_OPENED\""));
        publish(0, MAPPER.writeValueAsString(attempt(103L)));

        await().atMost(SETTLE).untilAsserted(() ->
                assertThat(seen).extracting(AttemptLiveEntry::memberId)
                        .as("오염된 한 건에서 멈추면 103 이 영영 안 온다")
                        .containsExactly(101L, 103L));

        await().atMost(SETTLE).untilAsserted(() ->
                assertThat(committedOffset("poison-live", 0))
                        .as("격리만 하고 offset 을 안 넘기면 poison message 다")
                        .isEqualTo(endOffset(0)));
    }

    /**
     * 지원하지 않는 {@code schemaVersion} 도 같은 길로 간다.
     *
     * <p>{@link IssuanceFlowEvent} 의 compact constructor 가 {@code schemaVersion != 1} 이면
     * <b>생성 자체를 거부</b>하므로, 구버전 레코드는 역직렬화에서 터진다. core 에는 그것을
     * 격리하는 장치가 없고 그게 이 티켓 몫이다.
     */
    @Test
    void isolatesUnsupportedSchemaVersionsTheSameWay() throws Exception {
        List<AttemptLiveEntry> seen = new CopyOnWriteArrayList<>();
        startLive(seen::add, "schema-live");

        publish(1, MAPPER.writeValueAsString(attempt(201L))
                .replace("\"schemaVersion\":1", "\"schemaVersion\":2"));
        publish(1, MAPPER.writeValueAsString(attempt(202L)));

        await().atMost(SETTLE).untilAsserted(() ->
                assertThat(seen).extracting(AttemptLiveEntry::memberId).containsExactly(202L));
        await().atMost(SETTLE).untilAsserted(() ->
                assertThat(committedOffset("schema-live", 1)).isEqualTo(endOffset(1)));
    }

    /**
     * 그룹이 둘이면 <b>둘 다 전량</b>을 받는다. 같은 그룹이면 파티션을 나눠 절반씩 본다.
     *
     * <p>파티션 둘에 나눠 넣는다 — 파티션이 하나면 같은 그룹이어도 한쪽이 전량을 받아
     * 이 테스트가 아무것도 구분하지 못한다.
     */
    @Test
    void deliversEveryRecordToBothGroups() throws Exception {
        List<AttemptLiveEntry> live = new CopyOnWriteArrayList<>();
        List<AttemptRecord> archive = new CopyOnWriteArrayList<>();
        startLive(live::add, KafkaConsumerGroups.ATTEMPT_LIVE);
        startArchive(archive::add, KafkaConsumerGroups.ATTEMPT_ARCHIVE);

        for (int i = 0; i < 4; i++) {
            publish(i % PARTITIONS, MAPPER.writeValueAsString(attempt(300L + i)));
        }

        await().atMost(SETTLE).untilAsserted(() -> {
            assertThat(live).as("화면이 절반만 보면 그룹이 하나다").hasSize(4);
            assertThat(archive).as("적재가 절반만 받으면 그룹이 하나다").hasSize(4);
        });
        assertThat(live).extracting(AttemptLiveEntry::memberId)
                .containsExactlyInAnyOrder(300L, 301L, 302L, 303L);
    }

    /**
     * <b>Redis 가 죽어도 Kafka 소비는 멈추지 않는다.</b>
     *
     * <p>화면 버퍼의 장애가 offset 을 막으면, 컨테이너가 재시도하다
     * {@code max.poll.interval.ms} 를 넘겨 그룹에서 쫓겨나고 리밸런싱이 반복된다. 화면 하나의
     * 장애가 소비 계층 전체의 상태가 되는 것이다. 대가는 그 사이 이벤트가 화면에서 영영
     * 사라지는 것이고, 그건 {@code app.attempt.live.append.failures} 로 드러난다.
     *
     * <p><b>어느 카운터가 오르는지까지 본다.</b> offset 만 확인하면 sink 예외를 컨슈머가 삼키는
     * 것과 에러 핸들러가 계약 위반으로 격리하는 것이 구분되지 않는다 — 둘 다 offset 은
     * 넘어간다. 뒤쪽이면 Redis 장애가 "계약 위반" 으로 집계되어, 지표를 보고 코드를 의심하게 된다.
     */
    @Test
    void keepsConsumingAndCommittingWhenTheLiveBufferIsDown() throws Exception {
        SimpleMeterRegistry consumerMeters = new SimpleMeterRegistry();
        SimpleMeterRegistry violationMeters = new SimpleMeterRegistry();
        AttemptLiveSink brokenSink = entry -> {
            throw new IllegalStateException("simulated redis outage");
        };
        startLive(brokenSink, "outage-live", consumerMeters, violationMeters);

        publish(0, MAPPER.writeValueAsString(attempt(501L)));
        publish(0, MAPPER.writeValueAsString(attempt(502L)));

        await().atMost(SETTLE).untilAsserted(() ->
                assertThat(counter(consumerMeters, DomainMeterNames.ATTEMPT_LIVE_APPEND_FAILURES))
                        .isEqualTo(2.0));
        await().atMost(SETTLE).untilAsserted(() ->
                assertThat(committedOffset("outage-live", 0))
                        .as("화면 장애가 offset 을 막으면 그룹에서 쫓겨난다").isEqualTo(endOffset(0)));
        assertThat(counter(violationMeters, DomainMeterNames.ATTEMPT_CONTRACT_VIOLATIONS))
                .as("Redis 장애는 계약 위반이 아니다").isZero();
    }

    /**
     * <b>archive 는 DB 실패에서 offset 을 넘기지 않는다.</b> live 와 정반대다.
     *
     * <p>이쪽은 보존 원본이다. DB 가 잠깐 죽었을 때 그 구간을 recoverer 로 넘겨 버리면
     * 되돌릴 방법이 없고, 게다가 그 유실이 {@code contract.violations} 라는 <b>엉뚱한
     * 이름</b>으로 집계된다 — 지표를 보고 이벤트 계약을 의심하게 되는데 원인은 DB 다.
     *
     * <p><b>이 성질은 backoff 하나에 달려 있다.</b> 두 컨슈머에 같은
     * {@code FixedBackOff(0, 0)} 을 주면 archive 도 그냥 건너뛴다 — 그리고 그 상태에서도
     * 다른 네 테스트는 전부 초록이었다(실측). 그래서 이 테스트가 따로 있다.
     */
    @Test
    void doesNotSkipArchiveRecordsWhenTheDatabaseIsDown() throws Exception {
        SimpleMeterRegistry violationMeters = new SimpleMeterRegistry();
        AtomicInteger attempts = new AtomicInteger();
        AttemptArchive brokenArchive = record -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("simulated database outage");
        };
        startArchive(brokenArchive, "outage-archive", Clock.systemUTC(), violationMeters);

        publish(0, MAPPER.writeValueAsString(attempt(601L)));

        // 고정 대기가 아니라 재시도가 실제로 세 번 돈 것을 기다린다. 컨슈머 스케줄링이 늦으면
        // sleep 은 "아직 한 번도 안 돌았는데 offset 이 안 넘어갔다" 를 성공으로 읽는다 —
        // 검사하려던 것이 재시도인데 그 재시도가 없었던 회차에서도 초록이 된다.
        await().atMost(SETTLE).untilAsserted(() ->
                assertThat(attempts.get()).as("재시도가 실제로 돌아야 한다").isGreaterThanOrEqualTo(3));

        assertThat(committedOffset("outage-archive", 0))
                .as("적재 실패를 건너뛰면 보존 원본이 조용히 빈다")
                .isNotEqualTo(endOffset(0));
        assertThat(counter(violationMeters, DomainMeterNames.ATTEMPT_CONTRACT_VIOLATIONS))
                .as("DB 장애는 계약 위반이 아니다").isZero();
    }

    /** {@code ingestedAt} 은 컨슈머가 찍는다 — 프로듀서 시계가 아니다. */
    @Test
    void stampsIngestedAtOnArrival() throws Exception {
        List<AttemptRecord> archive = new CopyOnWriteArrayList<>();
        Instant consumerNow = Instant.parse("2030-01-01T00:00:00Z");
        startArchive(archive::add, "ingested-archive",
                Clock.fixed(consumerNow, ZoneOffset.UTC));

        publish(0, MAPPER.writeValueAsString(attempt(401L)));

        await().atMost(SETTLE).untilAsserted(() -> assertThat(archive).hasSize(1));
        assertThat(archive.get(0).ingestedAt()).isEqualTo(consumerNow);
        assertThat(archive.get(0).event().occurredAt()).isNotEqualTo(consumerNow);
        assertThat(archive.get(0).topic()).isEqualTo(KafkaTopicConfig.ISSUE_ATTEMPT);
    }

    // ── 배선 ────────────────────────────────────────────────────────────────────

    private static void startLive(java.util.function.Consumer<AttemptLiveEntry> onEntry, String groupId) {
        startLive(onEntry::accept, groupId, new SimpleMeterRegistry(), new SimpleMeterRegistry());
    }

    private static void startLive(
            AttemptLiveSink sink,
            String groupId,
            SimpleMeterRegistry consumerMeters,
            SimpleMeterRegistry violationMeters
    ) {
        // 샘플링을 사실상 끈다. 이 테스트가 보는 것은 소비이지 샘플링이 아니다 —
        // 층화 규칙은 StratifiedSamplerTest 가 따로 고정한다.
        StratifiedSampler sampler = new StratifiedSampler(
                new AttemptSamplingProperties(0, Integer.MAX_VALUE, 64), Clock.systemUTC());
        AttemptLiveConsumer consumer = new AttemptLiveConsumer(
                sink, sampler, Clock.systemUTC(), consumerMeters);
        AttemptConsumerConfig config = new AttemptConsumerConfig();
        start(config.attemptLiveListenerContainerFactory(
                        config.attemptLiveConsumerFactory(connection(), MAPPER),
                        new AttemptContractViolationCounter(violationMeters)),
                groupId, consumer::consume);
    }

    private static void startArchive(
            java.util.function.Consumer<AttemptRecord> onRecord, String groupId) {
        startArchive(onRecord, groupId, Clock.systemUTC());
    }

    private static void startArchive(
            java.util.function.Consumer<AttemptRecord> onRecord, String groupId, Clock clock) {
        startArchive(record -> {
            onRecord.accept(record);
            return true;
        }, groupId, clock, new SimpleMeterRegistry());
    }

    private static void startArchive(
            AttemptArchive archive, String groupId, Clock clock, SimpleMeterRegistry violationMeters) {
        AttemptArchiveConsumer consumer =
                new AttemptArchiveConsumer(archive, clock, new SimpleMeterRegistry());
        AttemptConsumerConfig config = new AttemptConsumerConfig();
        start(config.attemptArchiveListenerContainerFactory(
                        config.attemptArchiveConsumerFactory(connection(), MAPPER),
                        new AttemptContractViolationCounter(violationMeters)),
                groupId, consumer::consume);
    }

    /**
     * 리스너를 애노테이션 처리 없이 직접 세운다.
     *
     * <p>{@code @KafkaListener} 배선 자체는 {@code AttemptConsumerConfigTest} 가 애노테이션을
     * 읽어 고정한다. 여기서 스프링 컨텍스트를 띄우면 이 모듈이 부팅 클래스를 갖게 되는데,
     * 그건 infra 모듈이 가질 물건이 아니다. 이 테스트가 보는 것은 <b>브로커와 맞물리는
     * 부분</b>이고, 그 부분은 컨테이너 팩토리 아래에 있다.
     *
     * <p>{@code groupId} 를 컨테이너에 다시 지정하는 이유 — 테스트마다 그룹을 갈라야 앞
     * 테스트가 커밋해 둔 offset 이 다음 테스트의 시작 위치를 바꾸지 않는다.
     */
    private static void start(
            ConcurrentKafkaListenerContainerFactory<String, IssuanceFlowEvent> factory,
            String groupId,
            AcknowledgingMessageListener<String, IssuanceFlowEvent> listener
    ) {
        ConcurrentMessageListenerContainer<String, IssuanceFlowEvent> container =
                factory.createContainer(KafkaTopicConfig.ISSUE_ATTEMPT);
        container.getContainerProperties().setGroupId(groupId);
        container.getContainerProperties().setMessageListener(listener);
        container.setConcurrency(1);
        CONTAINERS.add(container);
        container.start();
        // 파티션 배정 전에 발행하면 auto.offset.reset=latest 라 그 건들을 통째로 건너뛴다.
        // 이 대기가 없으면 테스트가 "소비가 안 됐다" 로 간헐 실패하고, 원인이 코드로 보인다.
        await().atMost(SETTLE).until(() -> container.isRunning()
                && container.getAssignedPartitions() != null
                && !container.getAssignedPartitions().isEmpty());
    }



    private static KafkaConnectionProperties connection() {
        return new KafkaConnectionProperties(KAFKA.getBootstrapServers());
    }


    private static void publish(int partition, String payload) throws Exception {
        producer.send(new ProducerRecord<>(
                KafkaTopicConfig.ISSUE_ATTEMPT, partition, "key", payload)).get();
    }

    /**
     * 그 파티션의 끝 오프셋. <b>커밋된 값을 절대 숫자로 단언하면 안 된다</b> —
     * 같은 파티션을 쓰는 다른 테스트가 먼저 돌면 시작점이 달라진다. JUnit 은 메서드 실행
     * 순서를 보장하지 않으므로 그 단언은 순서에 기대는 것이고, 실제로 그렇게 깨졌다
     * (2 를 기대했는데 8 이었다). "끝까지 따라잡았는가" 가 이 테스트들이 실제로 묻는 것이다.
     */
    private static long endOffset(int partition) throws Exception {
        TopicPartition topicPartition = new TopicPartition(KafkaTopicConfig.ISSUE_ATTEMPT, partition);
        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            return admin.listOffsets(Map.of(topicPartition, OffsetSpec.latest()))
                    .partitionResult(topicPartition).get().offset();
        }
    }

    private static long committedOffset(String groupId, int partition) throws Exception {
        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            Map<TopicPartition, OffsetAndMetadata> offsets =
                    admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
            OffsetAndMetadata committed = offsets.get(
                    new TopicPartition(KafkaTopicConfig.ISSUE_ATTEMPT, partition));
            return committed == null ? -1L : committed.offset();
        }
    }

    private static double counter(SimpleMeterRegistry registry, String name) {
        return registry.find(name).counters().stream()
                .mapToDouble(io.micrometer.core.instrument.Counter::count).sum();
    }

    private static IssuanceFlowEvent attempt(long memberId) {
        return FACTORY.issueAttempt(new IssuanceFlowEvent.Ctx(
                "request-" + memberId, memberId, 201L, Grade.GOLD, false,
                Instant.parse("2026-08-25T00:00:00Z"), EngineVersion.V3, ReleaseStage.V3,
                QueueMode.ADAPTIVE, null, "api-1"));
    }
}
