package com.kafkick.infra.mq.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.LoggingProducerListener;

/**
 * 이 티켓의 존재 이유를 지키는 테스트다. 프로듀서가 하나로 합쳐지면 관측 로그 전송 실패가
 * 발급 예외로 올라온다.
 */
class ProducerIsolationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(JacksonAutoConfiguration.class))
            .withUserConfiguration(AttemptProducerConfig.class, PersistProducerConfig.class)
            .withPropertyValues("kafka.enabled=true", "kafka.bootstrap-servers=localhost:9094");

    @Test
    @DisplayName("프로듀서 팩토리와 템플릿이 각각 둘이고 서로 다른 인스턴스다")
    void attemptAndPersistNeverShareAProducer() {
        runner.run(context -> {
            assertThat(context.getBeansOfType(ProducerFactory.class))
                    .as("등급이 둘이므로 팩토리도 둘이다 — 버퍼를 공유하면 분리가 무효다")
                    .hasSize(2);
            assertThat(context.getBeansOfType(KafkaTemplate.class))
                    .as("attempt · persist · DLT 발행용 셋. DLT 는 persist 팩토리를 재사용한다")
                    .hasSize(3);

            assertThat(context.getBean("attemptProducerFactory"))
                    .as("팩토리를 공유하면 버퍼도 공유다. 느린 persist 가 attempt 발행을 막는다")
                    .isNotSameAs(context.getBean("persistProducerFactory"));
            assertThat(template(context, "attemptKafkaTemplate").getProducerFactory())
                    .isNotSameAs(template(context, "persistKafkaTemplate").getProducerFactory());
        });
    }

    @Test
    @DisplayName("attempt 는 acks=0 · 멱등성 off · 재시도 없음 · max.block.ms 50")
    void attemptProducerNeverBlocksTheIssuancePath() {
        runner.run(context -> {
            Map<String, Object> config = configOf(context, "attemptProducerFactory");

            assertThat(config).containsEntry(ProducerConfig.ACKS_CONFIG, "0");
            assertThat(config).containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
            assertThat(config)
                    .as("멱등성이 꺼진 채 재시도하면 같은 파티션 안에서 순서가 뒤집힌다")
                    .containsEntry(ProducerConfig.RETRIES_CONFIG, 0);
            assertThat((int) config.get(ProducerConfig.MAX_BLOCK_MS_CONFIG))
                    .as("기본값 60초를 두면 버퍼가 찰 때 발급이 60초 멈추고, 0 이면 재기동 직후 전량 유실이다")
                    .isPositive()
                    .isLessThanOrEqualTo(100);
        });
    }

    /**
     * {@code max.block.ms} 가 지키는 것은 <b>메타데이터가 없거나 버퍼가 만석일 때</b>뿐이다.
     * 메타데이터를 이미 받아 둔 상태에서 브로커가 죽으면 {@code send()} 는 즉시 성공하고
     * 레코드는 accumulator 에 앉는다 — 그 콜백은 {@code delivery.timeout.ms} 가 만료돼야 온다.
     *
     * <p>기본값 120초를 두면 그동안 실패 카운터가 0 이라 "발행이 잘 되고 있다" 로 읽히고,
     * 기본 버퍼 32MiB 가 발급 API 힙에 고인다. 부하 중 수치를 재는 저장소에서 그 GC 압력은
     * 측정 자체를 오염시킨다.
     */
    @Test
    @DisplayName("attempt 는 브로커가 죽어도 2초 안에 실패가 보이고 버퍼가 8MiB 를 넘지 않는다")
    void attemptSurfacesFailuresFastAndCapsItsBuffer() {
        runner.run(context -> {
            Map<String, Object> config = configOf(context, "attemptProducerFactory");

            assertThat((int) config.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG))
                    .as("기본값 120초면 브로커 사망을 2분 뒤에 안다 — 그 사이 지표는 0 이다")
                    .isLessThanOrEqualTo(2_000);
            assertThat((long) config.get(ProducerConfig.BUFFER_MEMORY_CONFIG))
                    .as("기본 32MiB 가 발급 API 힙에 고이면 부하 측정이 GC 에 오염된다")
                    .isLessThanOrEqualTo(8L * 1024 * 1024);
            assertThat((int) config.get(ProducerConfig.MAX_BLOCK_MS_CONFIG)
                    + (int) config.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG))
                    .as("막는 시간과 실패를 아는 시간의 합이 persist 의 한 번 시도보다 길면 등급이 뒤집힌다")
                    .isLessThanOrEqualTo(3_000);
        });
    }

    /**
     * 여기서 "미설정" 을 단언하면 나중에 상한을 넣는 수정을 테스트가 막는다. 지키려는 것은
     * <b>값의 부재</b>가 아니라 <b>버려지지 않으면서도 상한이 있다</b>는 성질이다.
     */
    @Test
    @DisplayName("persist 는 acks=all · 멱등성 on 이고 상한이 셋 다 있다")
    void persistProducerNeverDropsAnIssuanceAndStillHasACeiling() {
        runner.run(context -> {
            Map<String, Object> config = configOf(context, "persistProducerFactory");

            assertThat(config).containsEntry(ProducerConfig.ACKS_CONFIG, "all");
            assertThat(config).containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

            int maxBlock = (int) config.get(ProducerConfig.MAX_BLOCK_MS_CONFIG);
            int request = (int) config.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            int delivery = (int) config.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);

            assertThat(maxBlock)
                    .as("버퍼가 찼다고 즉시 던지면 그 발급 건이 사라진다 — 기다리되 60초는 아니다")
                    .isGreaterThan(100);
            assertThat(maxBlock + delivery)
                    .as("호출부가 결과를 기다리면 이 합만큼 락이 잡힌다. 기본값 조합은 180초다")
                    .isLessThanOrEqualTo(30_000);
            int linger = (int) config.get(ProducerConfig.LINGER_MS_CONFIG);
            assertThat(delivery)
                    .as("클라이언트가 거부하는 조건은 delivery >= linger + request 다. linger 를"
                            + " 기본값에 맡기면 이 부등식을 테스트가 검사할 수 없다")
                    .isGreaterThanOrEqualTo(linger + request);
        });
    }

    @Test
    @DisplayName("두 프로듀서가 메트릭에서 구분된다 — client.id 가 다르다")
    void producersAreDistinguishableInMetrics() {
        runner.run(context -> {
            Object attempt = configOf(context, "attemptProducerFactory").get(ProducerConfig.CLIENT_ID_CONFIG);
            Object persist = configOf(context, "persistProducerFactory").get(ProducerConfig.CLIENT_ID_CONFIG);

            assertThat(attempt).isNotNull();
            assertThat(persist)
                    .as("기본값이면 producer-1 · producer-2 라 생성 순서에 따라 뒤바뀐다")
                    .isNotNull()
                    .isNotEqualTo(attempt);
        });
    }

    /**
     * 관측 설비가 없다고 발급 API 가 못 뜨면 안 된다. 이 저장소는 관측 풀이 죽어도 헬스를
     * UP 으로 유지하는 쪽을 이미 선택했다.
     */
    @Test
    @DisplayName("MeterRegistry 가 없어도 프로듀서 계층이 올라온다")
    void observabilityAbsenceDoesNotBreakStartup() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBeansOfType(io.micrometer.core.instrument.MeterRegistry.class)).isEmpty();
            assertThat(context.containsBean("attemptFailureCounter")).isTrue();
        });
    }

    /**
     * DLT 는 "잃지 않는 것" 이 목적이다. attempt 템플릿을 집으면 {@code acks=0} 이라 격리본이
     * 브로커 확인 없이 날아가고 실패해도 카운터가 안 오른다 — 이름을 못박아 그 선택지를 없앤다.
     */
    @Test
    @DisplayName("DLT 발행 템플릿은 persist 등급을 쓴다")
    void deadLetterPublishingUsesTheDurableProducer() {
        runner.run(context -> {
            assertThat(context.containsBean("deadLetterKafkaTemplate")).isTrue();
            assertThat(template(context, "deadLetterKafkaTemplate").getProducerFactory())
                    .as("attempt 팩토리를 집으면 격리본이 fire-and-forget 이 된다")
                    .isSameAs(context.getBean("persistProducerFactory"));
            assertThat(includesContents(template(context, "deadLetterKafkaTemplate")))
                    .as("DLT 로 가는 레코드는 원본 그대로다 — persist 에서 가린 페이로드가 여기서"
                            + " 부활하면 안 된다")
                    .isFalse();
            assertThat(includesContents(template(context, "persistKafkaTemplate"))).isFalse();
        });
    }

    @Test
    @DisplayName("스위치를 끄면 프로듀서가 하나도 올라오지 않는다")
    void producersAreOptIn() {
        runner.withPropertyValues("kafka.enabled=false").run(context ->
                assertThat(context.getBeansOfType(ProducerFactory.class)).isEmpty());
    }

    /**
     * 접속 정보 검증을 프로퍼티 레코드에 {@code @NotEmpty} 로 걸면 스캔이 스위치와 무관하게
     * 검증을 돌린다. 그러면 <b>Kafka 를 쓰지도 않는 v1·v2 배포</b>가 빈
     * {@code KAFKA_BOOTSTRAP_SERVERS} 하나로 기동에 실패한다. 검증을 조건 안쪽으로 옮긴 이유다.
     *
     * <p><b>{@link ScannedProperties} 없이는 이 테스트가 틀린 이유로 통과한다.</b> 조건부 설정의
     * {@code @EnableConfigurationProperties} 에만 기대면 스위치가 꺼졌을 때 프로퍼티 빈 자체가
     * 등록되지 않아서, 검증이 어디 있든 아무 일도 안 일어난다. 실제 앱은
     * {@code @ConfigurationPropertiesScan} 이 <b>무조건</b> 등록한다 — 그 상태를 만들어야 한다.
     */
    @Test
    @DisplayName("스위치가 꺼져 있으면 접속 정보가 비어 있어도 기동한다")
    void aDisabledModuleIsNotBrokenByEmptyKafkaSettings() {
        scanned()
                .withPropertyValues("kafka.enabled=false", "kafka.bootstrap-servers=")
                .run(context -> assertThat(context).hasNotFailed());
    }

    /** 반대로 켰는데 비어 있으면 첫 발급이 아니라 <b>기동에서</b> 죽어야 한다. */
    @Test
    @DisplayName("스위치를 켰는데 접속 정보가 비면 기동에서 죽는다")
    void anEnabledModuleFailsFastWithoutBootstrapServers() {
        scanned()
                .withPropertyValues("kafka.enabled=true", "kafka.bootstrap-servers=")
                .run(context -> assertThat(context)
                        .hasFailed()
                        .getFailure()
                        .rootCause()
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("kafka.bootstrap-servers"));
    }

    private ApplicationContextRunner scanned() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(JacksonAutoConfiguration.class))
                .withUserConfiguration(ScannedProperties.class,
                        AttemptProducerConfig.class, PersistProducerConfig.class);
    }

    /** {@code ApiApplication} 의 {@code @ConfigurationPropertiesScan} 을 흉내 낸다. */
    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(KafkaConnectionProperties.class)
    static class ScannedProperties {
    }

    /**
     * {@code LoggingProducerListener} 는 {@code includeContents} 게터가 없어 필드로 확인한다.
     * 브리틀하지만, 이 값이 true 로 돌아가면 실패마다 키(=식별자)와 본문이 로그로 나간다.
     *
     * <p>{@code NoSuchFieldException} 으로 깨지면 spring-kafka 업그레이드로 내부 필드명이 바뀐
     * 것이다. 그때는 게터가 생겼는지 먼저 보고, 없으면 커스텀 {@code ProducerListener} 로 갈아탄다.
     */
    private static boolean includesContents(KafkaTemplate<String, Object> template) throws Exception {
        java.lang.reflect.Field listenerField =
                KafkaTemplate.class.getDeclaredField("producerListener");
        listenerField.setAccessible(true);
        Object listener = listenerField.get(template);
        assertThat(listener).isInstanceOf(LoggingProducerListener.class);
        java.lang.reflect.Field contents =
                LoggingProducerListener.class.getDeclaredField("includeContents");
        contents.setAccessible(true);
        return (boolean) contents.get(listener);
    }

    @SuppressWarnings("unchecked")
    private static KafkaTemplate<String, Object> template(
            org.springframework.context.ApplicationContext context, String name) {
        return context.getBean(name, KafkaTemplate.class);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> configOf(
            org.springframework.context.ApplicationContext context, String name) {
        return context.getBean(name, ProducerFactory.class).getConfigurationProperties();
    }
}
