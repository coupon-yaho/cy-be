package com.kafkick.infra.mq.attempt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.LoggingProducerListener;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import com.kafkick.core.member.Grade;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.IssuanceFlowEventFactory;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.infra.mq.config.AttemptProducerConfig;
import com.kafkick.infra.mq.config.KafkaTopicConfig;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * 이 티켓의 완료 조건 — attempt 발행이 실패해도 발급이 성공해야 한다. 그래서 실패를
 * <b>일부러 주입해서</b> 확인한다.
 *
 * <p><b>실패 주입을 두 갈래로 한다.</b> {@code max.block.ms} 를 넘긴 실패는 예외가 아니라
 * 콜백으로 온다는 것을 실측으로 확인했기 때문이다(브로커 없는 프로듀서에 붙여 보니
 * {@code send()} 는 정상 반환하고 콜백이 호출 스레드에서 돌았다). 그래서 예외를 던지는
 * mock 만 쓰면 <b>현실에 없는 경로만 검증하게 된다</b> — 진짜 경로는
 * {@link MockProducer} 로 콜백을 태워서 본다.
 */
class AttemptEventPublisherTest {

    private final ListAppender<ILoggingEvent> errorLogs = new ListAppender<>();

    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final AttemptFailureCounter failures = new AttemptFailureCounter(meterRegistry);

    @BeforeEach
    void captureFrameworkErrorLogs() {
        errorLogs.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        errorLogs.start();
        ((Logger) LoggerFactory.getLogger(LoggingProducerListener.class)).addAppender(errorLogs);
    }

    @AfterEach
    void detachAppender() {
        ((Logger) LoggerFactory.getLogger(LoggingProducerListener.class)).detachAppender(errorLogs);
    }

    /**
     * <b>브로커가 없을 때 실제로 도는 경로다.</b> {@code send()} 반환 시점에 future 가 이미
     * 실패해 있으면 {@code KafkaTemplate} 이 그 자리에서 {@code get()} 을 불러
     * {@code KafkaException("Send failed")} 로 감싸 다시 던진다.
     *
     * <p>여기서 <b>합계가 1.0 인지</b>가 핵심이다. 예전에는 콜백과 catch 가 각각 세서 2.0 이
     * 나왔고, 원인도 timeout 1 + other 1 로 갈렸다.
     */
    @Test
    @DisplayName("현실 경로 — 이미 실패한 발행 1건이 카운터에 정확히 1건으로 잡힌다")
    void anAlreadyFailedSendIsCountedExactlyOnce() {
        MockProducer<String, Object> producer = mockProducer(true);
        producer.sendException = new TimeoutException("메타데이터 미확보 — max.block.ms 초과");
        KafkaTemplate<String, Object> template =
                new AttemptProducerConfig().attemptKafkaTemplate(factoryOf(producer));
        AttemptEventPublisher publisher = new AttemptEventPublisher(template, failures);

        assertThatCode(() -> publisher.record(event())).doesNotThrowAnyException();

        assertThat(failureCount())
                .as("두 경로에서 세면 실패율이 2배로 보이고 원인이 반씩 갈린다")
                .isEqualTo(1.0);
        assertThat(reasonCount("timeout"))
                .as("KafkaException 으로 감싸여 와도 원인 체인을 보면 timeout 이다")
                .isEqualTo(1.0);
        assertThat(reasonCount("other")).isZero();
    }

    @Test
    @DisplayName("지연 콜백 경로 — 나중에 도착하는 실패도 정확히 1건으로 잡힌다")
    void aDelayedFailureIsAlsoCountedExactlyOnce() {
        MockProducer<String, Object> producer = mockProducer(false);
        KafkaTemplate<String, Object> template =
                new AttemptProducerConfig().attemptKafkaTemplate(factoryOf(producer));
        AttemptEventPublisher publisher = new AttemptEventPublisher(template, failures);

        assertThatCode(() -> publisher.record(event())).doesNotThrowAnyException();
        assertThat(failureCount()).as("아직 실패가 오지 않았다").isZero();

        producer.errorNext(new TimeoutException("브로커 응답 없음"));

        assertThat(failureCount()).isEqualTo(1.0);
        assertThat(reasonCount("timeout")).isEqualTo(1.0);
    }

    /**
     * <b>진짜 브로커 부재를 재현한다.</b> {@link MockProducer} 는 실패를 <em>우리가 심어 준다</em> —
     * {@code max.block.ms} 만료가 실제로 어떤 타입의 예외를 어느 스레드에서 만드는지는 검증하지
     * 못한다. 그래서 여기서는 실제 프로듀서를 죽은 주소에 물려
     * {@code waitOnMetadata} 가 스스로 만든 실패를 본다.
     *
     * <p>{@code max.block.ms=50} 이라 브로커도 컨테이너도 필요 없다. <b>기다리지도 않는다</b> —
     * 이 실패는 {@code record()} 가 돌아오기 전에 전부 도착한다(20회 반복으로 확인했고, 뒤에
     * 500ms 를 더 기다려도 값이 늘지 않았다). {@code sleep} 이나 폴링으로 대기하면 CI 지연에
     * 흔들릴 뿐 아니라, <b>뒤늦게 도착하는 중복을 놓치는</b> 가드가 된다.
     */
    @Test
    @DisplayName("브로커 없는 실제 프로듀서 — 발행 1회가 카운터 1건이다")
    void aRealProducerWithoutABrokerCountsExactlyOnce() throws IOException {
        Map<String, Object> config = new HashMap<>();
        // 9094 는 이 저장소가 정한 브로커 기본 포트다(kafka.yml.example). 거기에 "비어 있다" 를
        // 전제하면 로컬에 브로커를 띄워 둔 사람에게만 깨지고, 그때 진짜 토픽에 레코드가 들어간다.
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, closedAddress());
        config.put(ProducerConfig.ACKS_CONFIG, "0");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 50);
        config.put(ProducerConfig.RETRIES_CONFIG, 0);
        DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(
                config, new StringSerializer(), (topic, data) -> new byte[] { 1 });

        try {
            KafkaTemplate<String, Object> template =
                    new AttemptProducerConfig().attemptKafkaTemplate(factory);
            AttemptEventPublisher publisher = new AttemptEventPublisher(template, failures);

            assertThatCode(() -> publisher.record(event())).doesNotThrowAnyException();

            assertThat(failures.total())
                    .as("콜백과 catch 가 각각 세면 여기가 2 가 된다 — 실제로 그렇게 만들어 봤다")
                    .isEqualTo(1L);
            assertThat(reasonCount("timeout")).isEqualTo(1.0);
            assertThat(reasonCount("other")).isZero();
            assertThat(errorLogs.list)
                    .as("KafkaTemplate 기본 LoggingProducerListener 는 실패마다 ERROR 에 키와"
                            + " 페이로드를 실어 호출 스레드에서 찍는다 — max.block.ms 로 번 시간을"
                            + " 로그 I/O 가 되돌리고 회원 식별자까지 흘린다")
                    .isEmpty();
        } finally {
            factory.destroy();
        }
    }

    /**
     * 위 가드가 <b>진짜로 도는지</b> 확인한다. 로거 이름이 바뀌거나 spring-kafka 가 로깅 자리를
     * 옮기면 "ERROR 0건" 단언은 공허하게 통과한다 — 그때 이 테스트가 먼저 빨개진다.
     */
    @Test
    @DisplayName("기본 로거를 붙이면 실패 1건이 ERROR 1건으로 잡힌다 — 가드가 실제로 돈다")
    void theLogGuardActuallyObservesTheFrameworkLogger() {
        MockProducer<String, Object> producer = mockProducer(false);
        KafkaTemplate<String, Object> template =
                new AttemptProducerConfig().attemptKafkaTemplate(factoryOf(producer));
        template.setProducerListener(new LoggingProducerListener<>());
        AttemptEventPublisher publisher = new AttemptEventPublisher(template, failures);

        publisher.record(event());
        // 콜백으로 오는 실패여야 리스너가 불린다. send() 가 그냥 던지는 경로는 콜백을 안 탄다.
        producer.errorNext(new TimeoutException("브로커 응답 없음"));

        assertThat(errorLogs.list)
                .as("이게 비어 있으면 위 가드는 아무것도 안 보고 있다는 뜻이다")
                .hasSize(1);
    }

    @Test
    @DisplayName("호출 스레드로 던져지는 실패도 발급 경로로 새지 않는다")
    void synchronousFailureNeverEscapesToTheIssuancePath() {
        KafkaTemplate<String, Object> template = mockTemplate();
        when(template.send(anyString(), anyString(), any()))
                .thenThrow(new org.springframework.kafka.KafkaException("직렬화 실패"));
        AttemptEventPublisher publisher = new AttemptEventPublisher(template, failures);

        assertThatCode(() -> publisher.record(event())).doesNotThrowAnyException();
        assertThat(failureCount()).isEqualTo(1.0);
    }

    /**
     * JVM 이 죽어 가는 상황까지 숨기면 안 된다. 관측 실패 1건으로 기록하고 발급을 계속하면
     * 이미 불건전한 프로세스가 요청을 계속 받는다.
     */
    @Test
    @DisplayName("Error 는 삼키지 않고 그대로 올려보낸다")
    void errorsAreNotSwallowed() {
        KafkaTemplate<String, Object> template = mockTemplate();
        when(template.send(anyString(), anyString(), any()))
                .thenThrow(new OutOfMemoryError("힙 고갈"));
        AttemptEventPublisher publisher = new AttemptEventPublisher(template, failures);

        assertThatThrownBy(() -> publisher.record(event())).isInstanceOf(OutOfMemoryError.class);
        assertThat(failureCount()).as("Error 를 실패 1건으로 기록해 은폐하면 안 된다").isZero();
    }

    @Test
    @DisplayName("정상 발행은 attempt 토픽으로 memberId 키를 달고 나간다")
    void publishesToTheAttemptTopicKeyedByMemberId() {
        KafkaTemplate<String, Object> template = mockTemplate();
        AttemptEventPublisher publisher = new AttemptEventPublisher(template, failures);
        IssuanceFlowEvent event = event();

        publisher.record(event);

        verify(template).send(KafkaTopicConfig.ISSUE_ATTEMPT, "7", event);
        assertThat(failureCount()).isZero();
    }

    private double failureCount() {
        return meterRegistry.find(AttemptFailureCounter.FAILURE_COUNTER).counters().stream()
                .mapToDouble(counter -> counter.count())
                .sum();
    }

    private double reasonCount(String reason) {
        return meterRegistry.get(AttemptFailureCounter.FAILURE_COUNTER)
                .tag("reason", reason)
                .counter()
                .count();
    }

    /** 0번 포트로 열었다 바로 닫으면 그 포트는 사실상 비어 있다. */
    private static String closedAddress() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return "127.0.0.1:" + socket.getLocalPort();
        }
    }

    private static MockProducer<String, Object> mockProducer(boolean autoComplete) {
        return new MockProducer<>(Cluster.empty(), autoComplete, null,
                new StringSerializer(), (topic, data) -> new byte[0]);
    }

    private static ProducerFactory<String, Object> factoryOf(Producer<String, Object> producer) {
        return new ProducerFactory<>() {
            @Override
            public Producer<String, Object> createProducer() {
                return producer;
            }
        };
    }

    static IssuanceFlowEvent event() {
        IssuanceFlowEventFactory factory = new IssuanceFlowEventFactory(
                () -> UUID.fromString("11111111-1111-1111-1111-111111111111"));
        return factory.issued(context(), 101L, "ISSUANCE0000001");
    }

    static IssuanceFlowEvent.Ctx context() {
        return new IssuanceFlowEvent.Ctx(
                "request-1",
                7L,
                42L,
                Grade.WELCOME,
                false,
                Instant.parse("2026-08-20T00:00:00Z"),
                EngineVersion.V3,
                ReleaseStage.V3,
                QueueMode.OFF,
                null,
                "api-1");
    }

    @SuppressWarnings("unchecked")
    private static KafkaTemplate<String, Object> mockTemplate() {
        return mock(KafkaTemplate.class);
    }
}
