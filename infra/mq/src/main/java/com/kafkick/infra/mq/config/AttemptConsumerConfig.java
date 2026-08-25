package com.kafkick.infra.mq.config;

import java.time.Clock;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.attempt.AttemptArchive;
import com.kafkick.core.observation.attempt.AttemptLiveSink;
import com.kafkick.infra.mq.attempt.AttemptArchiveConsumer;
import com.kafkick.infra.mq.attempt.AttemptContractViolationCounter;
import com.kafkick.infra.mq.attempt.AttemptLiveConsumer;
import com.kafkick.infra.mq.attempt.AttemptSamplingProperties;
import com.kafkick.infra.mq.attempt.StratifiedSampler;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import tools.jackson.databind.json.JsonMapper;

/**
 * attempt 컨슈머 둘. <b>그룹이 둘인 것이 이 설정의 존재 이유다.</b>
 *
 * <h2>왜 팩토리를 둘 두는가</h2>
 *
 * {@code group.id} 는 {@code ConsumerFactory} 의 설정 맵에 들어간다. 팩토리 하나를 공유하고
 * {@code @KafkaListener(groupId=...)} 로만 가르는 방법도 있지만, 그러면 {@code auto.offset.reset}
 * 같은 나머지 값이 두 그룹에 대해 같아진다 — 지금은 둘 다 {@code latest} 라 우연히 맞지만,
 * {@link KafkaConsumerGroups} 는 그 값을 <b>그룹별 정책</b>으로 선언해 두었다. 팩토리를 나눠
 * {@link KafkaConsumerGroups#consumerConfig(String, String)} 를 각각 통과시키면, 정책이 바뀔 때
 * 코드 구조가 아니라 그 표 하나만 고치면 된다.
 *
 * <h2>{@code @EnableKafka} 가 여기 있어야 한다</h2>
 *
 * 이 저장소는 Boot 의 Kafka 자동설정을 쓰지 않는다({@code kafka.yml.example} 첫 줄). 그 말은
 * {@code KafkaListenerAnnotationBeanPostProcessor} 를 아무도 등록하지 않는다는 뜻이고, 그러면
 * {@code @KafkaListener} 는 <b>그냥 애노테이션</b>이다 — 컨테이너가 안 만들어지고 컨슈머가
 * 한 건도 안 읽는데, 앱은 정상 기동하고 로그에도 아무것도 안 남는다. 화면과 DB 가 함께 비고
 * 원인은 어디에도 없다.
 *
 * <p>{@code @EnableKafka} 는 기본 컨테이너 팩토리 이름({@code kafkaListenerContainerFactory})을
 * 요구하지만, 두 리스너가 {@code containerFactory} 를 명시하므로 그 기본값은 필요 없다.
 *
 * <h2>수동 커밋이다</h2>
 *
 * {@code AckMode.MANUAL} 이고 리스너가 {@code Acknowledgment} 를 받는다. 자동 커밋이면 처리
 * 전에 offset 이 넘어가서, archive 의 "INSERT 뒤에 commit" 계약을 지킬 방법이 없다.
 *
 * <h2>역직렬화 실패는 격리하고 offset 을 넘긴다</h2>
 *
 * {@link IssuanceFlowEvent} 는 {@code schemaVersion != 1} 이면 <b>생성 자체를 거부한다.</b>
 * 구버전 레코드나 모르는 enum 값은 역직렬화에서 터지는데, 그대로 두면 컨테이너가 같은
 * 레코드를 무한 재시도한다(poison message) — 그 파티션의 소비가 영원히 멈춘다.
 *
 * <p>{@link ErrorHandlingDeserializer} 로 감싸 예외를 헤더로 옮기고, {@link DefaultErrorHandler}
 * 에 <b>재시도 없는</b> backoff 와 카운터 recoverer 를 준다. DLT 로 보내지 않는 이유는
 * {@link AttemptContractViolationCounter} javadoc 에 적었다 — 요약하면 그 토픽이 선언되어 있지
 * 않아 브로커가 RF1 으로 만들어 버린다.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty("kafka.enabled")
@EnableKafka
@EnableConfigurationProperties({KafkaConnectionProperties.class, AttemptSamplingProperties.class})
public class AttemptConsumerConfig {

    public static final String LIVE_CONTAINER_FACTORY = "attemptLiveListenerContainerFactory";
    public static final String ARCHIVE_CONTAINER_FACTORY = "attemptArchiveListenerContainerFactory";

    /**
     * 파티션이 6 이라 6 을 넘기면 남는 스레드가 논다. 3 을 고른 것은 브로커 3 대에 균등하게
     * 떨어지면서 인스턴스 하나가 파티션 절반을 잡기 때문이다.
     */
    static final int CONCURRENCY = 3;

    /** archive 재시도 간격. DB 가 돌아올 때까지 기다리는 자리라 촘촘할 이유가 없다. */
    public static final long ARCHIVE_RETRY_INTERVAL_MS = 1_000L;

    private static final Logger log = LoggerFactory.getLogger(AttemptConsumerConfig.class);

    @Bean
    public ConsumerFactory<String, IssuanceFlowEvent> attemptLiveConsumerFactory(
            KafkaConnectionProperties properties, JsonMapper jsonMapper) {
        return consumerFactory(KafkaConsumerGroups.ATTEMPT_LIVE, properties, jsonMapper);
    }

    @Bean
    public ConsumerFactory<String, IssuanceFlowEvent> attemptArchiveConsumerFactory(
            KafkaConnectionProperties properties, JsonMapper jsonMapper) {
        return consumerFactory(KafkaConsumerGroups.ATTEMPT_ARCHIVE, properties, jsonMapper);
    }

    @Bean(LIVE_CONTAINER_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<String, IssuanceFlowEvent> attemptLiveListenerContainerFactory(
            @Qualifier("attemptLiveConsumerFactory") ConsumerFactory<String, IssuanceFlowEvent> factory,
            AttemptContractViolationCounter violations) {
        return containerFactory(factory, violations, SKIP_IMMEDIATELY);
    }

    @Bean(ARCHIVE_CONTAINER_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<String, IssuanceFlowEvent> attemptArchiveListenerContainerFactory(
            @Qualifier("attemptArchiveConsumerFactory") ConsumerFactory<String, IssuanceFlowEvent> factory,
            AttemptContractViolationCounter violations) {
        return containerFactory(factory, violations, retryForever());
    }

    /**
     * 레지스트리가 없어도 세운다 — 관측 설비의 부재가 소비 계층의 기동 실패가 되면 안 된다.
     * 다만 조용히 넘어가지는 않는다({@code AttemptProducerConfig} 와 같은 판단이다).
     */
    @Bean
    public AttemptContractViolationCounter attemptContractViolationCounter(
            ObjectProvider<MeterRegistry> meterRegistries) {
        return new AttemptContractViolationCounter(requireMeterRegistry(meterRegistries));
    }

    /**
     * 레지스트리가 없어도 세운다 — 관측 설비의 부재가 소비 계층의 기동 실패가 되면 안 된다.
     * 다만 <b>조용히</b> 넘어가지는 않는다.
     *
     * <p>이 문을 세 빈이 함께 지나게 한 것은 리뷰에서 잡힌 결함이다. 처음에는 계약 위반
     * 카운터에만 로그가 있고 두 컨슈머는 말없이 고아 레지스트리로 떨어졌다 — 그런데 이 티켓의
     * 대시보드가 실제로 기대는 미터({@code sampled} · {@code append.failures} ·
     * {@code archive.outcome})는 <b>그 두 빈이 소유한다.</b> 시계열이 통째로 비는데 그 이유를
     * 말해 주는 로그는 하필 안 도는 빈에 있었다.
     */
    private static MeterRegistry requireMeterRegistry(ObjectProvider<MeterRegistry> meterRegistries) {
        return meterRegistries.getIfAvailable(() -> {
            log.warn("MeterRegistry 가 없다 — attempt 컨슈머 계층의 미터가 노출되지 않는다");
            return new SimpleMeterRegistry();
        });
    }

    /**
     * 샘플러는 <b>인스턴스당 하나다.</b> 리스너 스레드 3 개가 같은 것을 공유한다.
     *
     * <p>스레드마다 두면 초당 한도가 스레드 수만큼 곱해진다 — 설정에 100 을 적었는데 실제로는
     * 300 이 나가고, 그 사실이 어디에도 안 적힌다.
     *
     * <p><b>인스턴스 사이에서는 곱해진다.</b> api 가 4 대면 실제 Redis 쓰기는 최대 4 배다.
     * 분산 한도를 두려면 Redis 를 봐야 하는데, 그건 발급 경로가 아니라 컨슈머라 해도 관측이
     * 관측을 위해 왕복을 하나 더 사는 것이다. 대신 통과 <b>판정</b>을 지표로 낸다.
     *
     * <p>⚠️ {@code app.attempt.live.sampled{decision="admitted"}} 는 <b>판정</b> 수이지 버퍼에
     * 실제로 들어간 수가 아니다. 그 둘은 Redis 가 흔들리는 구간에서 갈라진다 — 실제 유입은
     * {@code admitted - app.attempt.live.append.failures} 다. 설정값을 튜닝할 때 앞의 값만 보면
     * 장애 구간에서 "통과량은 정상인데 화면만 비었다" 로 읽고 한도를 반대 방향으로 잡는다.
     */
    @Bean
    public StratifiedSampler attemptStratifiedSampler(
            AttemptSamplingProperties properties, ObjectProvider<Clock> clocks) {
        log.info("attempt live 샘플링 — 층별 최소 {}/s · 전체 상한 {}/s · 층 상한 {} (최악 {}/s, 인스턴스당)",
                properties.resolvedMinPerStratumPerSecond(), properties.resolvedMaxPerSecond(),
                properties.resolvedMaxStrata(), properties.worstCasePerSecond());
        return new StratifiedSampler(properties, clocks.getIfAvailable(Clock::systemUTC));
    }

    @Bean
    public AttemptLiveConsumer attemptLiveConsumer(
            AttemptLiveSink sink,
            StratifiedSampler sampler,
            ObjectProvider<Clock> clocks,
            ObjectProvider<MeterRegistry> meterRegistries) {
        return new AttemptLiveConsumer(sink, sampler, clocks.getIfAvailable(Clock::systemUTC),
                requireMeterRegistry(meterRegistries));
    }

    @Bean
    public AttemptArchiveConsumer attemptArchiveConsumer(
            AttemptArchive archive,
            ObjectProvider<Clock> clocks,
            ObjectProvider<MeterRegistry> meterRegistries) {
        return new AttemptArchiveConsumer(archive, clocks.getIfAvailable(Clock::systemUTC),
                requireMeterRegistry(meterRegistries));
    }

    private static ConsumerFactory<String, IssuanceFlowEvent> consumerFactory(
            String groupId, KafkaConnectionProperties properties, JsonMapper jsonMapper) {
        Objects.requireNonNull(jsonMapper, "jsonMapper");
        Map<String, Object> config = KafkaConsumerGroups.consumerConfig(
                groupId, KafkaProducerSupport.requireBootstrapServers(properties));
        return new DefaultKafkaConsumerFactory<>(config,
                new ErrorHandlingDeserializer<>(new StringDeserializer()),
                new ErrorHandlingDeserializer<>(valueDeserializer(jsonMapper)));
    }

    /**
     * 타입 헤더를 <b>안 본다.</b> 프로듀서가 {@code setAddTypeInfo(false)} 로 보내므로 헤더가
     * 아예 없고, 헤더를 보게 두면 그 부재를 오류로 읽는다. 대상 타입은 여기서 고정한다.
     */
    private static JacksonJsonDeserializer<IssuanceFlowEvent> valueDeserializer(JsonMapper jsonMapper) {
        return new JacksonJsonDeserializer<>(IssuanceFlowEvent.class, jsonMapper, false);
    }

    private static ConcurrentKafkaListenerContainerFactory<String, IssuanceFlowEvent> containerFactory(
            ConsumerFactory<String, IssuanceFlowEvent> consumerFactory,
            AttemptContractViolationCounter violations,
            BackOff backOff) {
        ConcurrentKafkaListenerContainerFactory<String, IssuanceFlowEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(CONCURRENCY);
        factory.getContainerProperties().setAckMode(AckMode.MANUAL);
        factory.setCommonErrorHandler(errorHandler(violations, backOff));
        return factory;
    }

    /**
     * 격리 핸들러. <b>backoff 가 두 컨슈머에서 달라야 한다.</b>
     *
     * <h2>역직렬화 실패에는 backoff 가 아무 영향이 없다 — 실측</h2>
     *
     * 처음에는 두 컨슈머에 {@code FixedBackOff(0, 0)} 하나를 주고 "재시도하지 않는다" 고
     * 적었다. 그런데 {@code DefaultErrorHandler} 는 {@code DeserializationException} 을
     * <b>재시도 불가</b>로 이미 분류하고 있어서, backoff 를 무한 재시도로 바꿔 돌려도 동작이
     * 한 글자도 안 바뀐다(실제로 바꿔서 돌렸고 실제 브로커 테스트 5개가 그대로 초록이었다).
     * 그 주석은 지켜지는 것이 없는 규칙이었다.
     *
     * <h2>그래서 backoff 가 실제로 정하는 것은 리스너가 던진 예외다</h2>
     *
     * 그리고 두 컨슈머는 거기서 정반대다.
     *
     * <ul>
     *   <li><b>live</b> — {@link #SKIP_IMMEDIATELY}. 화면 버퍼는 최근 몇 백 건을 보여 주는 것이
     *       전부라 한 건이 빠져도 다음 폴링이 덮는다. 여기서 붙잡으면 화면 하나의 장애가
     *       컨슈머 그룹 전체의 리밸런싱이 된다.</li>
     *   <li><b>archive</b> — {@link #retryForever()}. 이쪽은 <b>보존 원본</b>이다. DB 가 잠깐
     *       죽었을 때 그 구간을 recoverer 로 넘겨 버리면 되돌릴 방법이 없고, 게다가 그
     *       유실이 {@code contract.violations} 라는 <b>엉뚱한 이름</b>으로 집계된다 — 지표를
     *       보고 이벤트 계약을 의심하게 된다. 실제 원인은 DB 인데.</li>
     * </ul>
     *
     * <p>archive 쪽 대가는 DB 장애가 길어지면 그 컨슈머가 {@code max.poll.interval.ms} 를 넘겨
     * 그룹에서 쫓겨나고 리밸런싱이 반복되는 것이다. 그건 시끄러워서 눈에 띈다 — 조용한 유실보다
     * 낫다는 판단이고, 이 트레이드오프가 이 티켓에서 가장 되돌려지기 쉬운 결정이다.
     */
    private static CommonErrorHandler errorHandler(
            AttemptContractViolationCounter violations, BackOff backOff) {
        return new DefaultErrorHandler(
                (record, exception) -> violations.record(
                        exception, record.topic(), record.partition(), record.offset()),
                backOff);
    }

    /** live 용. 리스너가 던지면 그 건을 버리고 바로 넘어간다. */
    private static final BackOff SKIP_IMMEDIATELY = new FixedBackOff(0L, 0L);

    /**
     * archive 용. 리스너가 던지는 한 계속 재시도한다.
     *
     * <p>{@code FixedBackOff} 는 새 인스턴스여야 한다 — 상태를 갖지는 않지만, 두 팩토리가
     * 같은 객체를 공유하면 나중에 상태를 갖는 구현으로 바꿀 때 조용히 얽힌다.
     */
    private static BackOff retryForever() {
        return new FixedBackOff(ARCHIVE_RETRY_INTERVAL_MS, Long.MAX_VALUE);
    }
}
