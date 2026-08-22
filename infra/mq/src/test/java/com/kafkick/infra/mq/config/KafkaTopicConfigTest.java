package com.kafkick.infra.mq.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * 토픽 선언의 값 자체가 계약이다. 파티션 수는 나중에 줄일 수 없고, ISR 하한이 빠지면
 * acks=all 이 "리더 한 대에만 썼다" 로 조용히 약해진다.
 */
@ExtendWith(OutputCaptureExtension.class)
class KafkaTopicConfigTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(JacksonAutoConfiguration.class))
            .withUserConfiguration(KafkaTopicConfig.class)
            .withPropertyValues("kafka.bootstrap-servers=localhost:9094");

    @Test
    @DisplayName("스위치를 끄면 토픽 선언이 통째로 올라오지 않는다")
    void topicsAreOptIn() {
        runner.withPropertyValues("kafka.enabled=false").run(context ->
                assertThat(context.getBeansOfType(NewTopic.class)).isEmpty());
    }

    @Test
    @DisplayName("토픽 3 + DLT 2 를 선언한다")
    void declaresThreeTopicsAndTwoDeadLetterTopics() {
        enabled().run(context -> {
            assertThat(topics(context).values())
                    .extracting(NewTopic::name)
                    .containsExactlyInAnyOrderElementsOf(KafkaTopicConfig.allTopics());
            assertThat(context).hasSingleBean(KafkaAdmin.class);
        });
    }

    @Test
    @DisplayName("모든 토픽이 파티션 6이다 — DLT 도 같다")
    void everyTopicHasSixPartitions() {
        enabled().run(context ->
                assertThat(topics(context).values())
                        .allSatisfy(topic -> assertThat(topic.numPartitions())
                                .as("%s 의 파티션 수", topic.name())
                                .isEqualTo(6)));
    }

    /**
     * DLT 파티션이 원본보다 적으면 {@code DeadLetterPublishingRecoverer} 가 같은 번호의
     * 파티션으로 보내다 실패한다. poison message 를 치우려다 컨슈머가 다시 멈춘다.
     */
    @Test
    @DisplayName("DLT 파티션 수가 원본 토픽보다 작지 않다")
    void deadLetterTopicsAreNotNarrowerThanTheirSource() {
        enabled().run(context -> {
            Map<String, NewTopic> byName = byName(context);
            assertThat(byName.get(KafkaTopicConfig.ISSUE_PERSIST_DLT).numPartitions())
                    .isGreaterThanOrEqualTo(byName.get(KafkaTopicConfig.ISSUE_PERSIST).numPartitions());
            assertThat(byName.get(KafkaTopicConfig.NOTIFY_DLT).numPartitions())
                    .isGreaterThanOrEqualTo(byName.get(KafkaTopicConfig.NOTIFY).numPartitions());
        });
    }

    @Test
    @DisplayName("내구성 토픽은 RF3 · ISR2 이고 attempt 만 RF2 · ISR 없음")
    void durabilityGradeMatchesTheAcksOfEachTopic() {
        enabled().run(context -> {
            Map<String, NewTopic> byName = byName(context);

            for (String durable : java.util.List.of(
                    KafkaTopicConfig.ISSUE_PERSIST, KafkaTopicConfig.NOTIFY,
                    KafkaTopicConfig.ISSUE_PERSIST_DLT, KafkaTopicConfig.NOTIFY_DLT)) {
                assertThat(byName.get(durable).replicationFactor()).as("%s 의 RF", durable).isEqualTo((short) 3);
                assertThat(byName.get(durable).configs())
                        .as("%s 의 ISR 하한 — 없으면 acks=all 이 리더 한 대짜리가 된다", durable)
                        .containsEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2");
            }

            NewTopic attempt = byName.get(KafkaTopicConfig.ISSUE_ATTEMPT);
            assertThat(attempt.replicationFactor()).isEqualTo((short) 2);
            assertThat(attempt.configs() == null ? java.util.Map.<String, String>of() : attempt.configs())
                    .as("acks=0 은 리더 응답도 안 기다린다. ISR 하한을 적으면 지켜지는 것처럼 읽히기만 한다")
                    .doesNotContainKey(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
        });
    }

    @Test
    @DisplayName("attempt 는 12시간 · 파티션당 2GiB 로 잘린다 — 아무도 안 읽는 로그가 디스크를 채우면 persist 가 먼저 죽는다")
    void attemptIsCappedByBothTimeAndSize() {
        enabled().run(context -> {
            NewTopic attempt = byName(context).get(KafkaTopicConfig.ISSUE_ATTEMPT);

            assertThat(attempt.configs())
                    .containsEntry(TopicConfig.RETENTION_MS_CONFIG,
                            Long.toString(java.time.Duration.ofHours(12).toMillis()));
            assertThat(Long.parseLong(attempt.configs().get(TopicConfig.RETENTION_BYTES_CONFIG)))
                    .as("파티션당이다. 6을 곱한 값이 토픽 총량이 된다")
                    .isEqualTo(2L * 1024 * 1024 * 1024);
        });
    }

    /** DLT 는 더 길게 잡으므로 여기서 빼고 {@code deadLetterTopicsOutliveTheirSource} 가 본다. */
    @Test
    @DisplayName("내구성 토픽도 보존을 브로커 기본값에 맡기지 않는다")
    void durableTopicsDeclareTheirOwnRetention() {
        enabled().run(context -> {
            Map<String, NewTopic> byName = byName(context);
            for (String durable : java.util.List.of(
                    KafkaTopicConfig.ISSUE_PERSIST, KafkaTopicConfig.NOTIFY)) {
                assertThat(byName.get(durable).configs())
                        .as("%s 의 보존", durable)
                        .containsEntry(TopicConfig.RETENTION_MS_CONFIG,
                                Long.toString(java.time.Duration.ofDays(7).toMillis()));
            }
        });
    }

    /**
     * 격리본이 원본과 같이 만료되면 DLT 에 넣은 의미가 없다 — 원인을 보러 갔을 때 이미 지워져 있다.
     */
    @Test
    @DisplayName("DLT 보존이 원본보다 짧지 않다")
    void deadLetterTopicsOutliveTheirSource() {
        enabled().run(context -> {
            Map<String, NewTopic> byName = byName(context);
            long persist = retentionOf(byName.get(KafkaTopicConfig.ISSUE_PERSIST));

            assertThat(retentionOf(byName.get(KafkaTopicConfig.ISSUE_PERSIST_DLT)))
                    .as("원본이 만료될 때 증거도 같이 사라지면 DLT 가 하는 일이 없다")
                    .isGreaterThan(persist);
            assertThat(retentionOf(byName.get(KafkaTopicConfig.NOTIFY_DLT))).isGreaterThan(persist);
        });
    }

    /**
     * Spring 의 기본 목적지 해석기는 원본 이름 + {@code .DLT} 다. attempt 컨슈머에 공용 에러
     * 핸들러를 달면 <b>선언하지 않은 토픽이 브로커에서 RF1 으로 생기고</b>, 그 토픽은
     * {@code allTopics()} 밖이라 선언 검증도 영원히 못 본다.
     */
    @Test
    @DisplayName("attempt 에는 DLT 를 두지 않는다 — 결정을 코드가 들고 있다")
    void attemptHasNoDeadLetterTopicOnPurpose() {
        assertThat(KafkaTopicConfig.allTopics())
                .doesNotContain(KafkaTopicConfig.ISSUE_ATTEMPT + KafkaTopicConfig.DLT_SUFFIX);
        assertThat(KafkaTopicConfig.TOPICS_WITHOUT_DLT)
                .as("이유를 아는 곳이 없으면 다음 사람이 그냥 붙인다")
                .containsExactly(KafkaTopicConfig.ISSUE_ATTEMPT);
    }

    /**
     * 이 테스트가 지키는 것은 <b>{@code KafkaAdmin.setAutoCreate(false)}</b> 다 — 그게 켜져
     * 있으면 refresh 중에 브로커를 찾다가 컨텍스트당 15초가 걸렸다(실측). 프로비저너 자체의
     * 비차단성은 {@code KafkaTopicProvisionerTest} 가 본다.
     */
    @Test
    @DisplayName("토픽 생성이 컨텍스트 refresh 에 끼어들지 않는다 — autoCreate 가 꺼져 있다")
    void topicCreationIsNotPartOfContextRefresh() {
        long startedAt = System.nanoTime();
        enabled().run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(KafkaAdmin.class);
            assertThat(context.getBeansOfType(org.springframework.boot.ApplicationRunner.class))
                    .as("스위치를 끄면 프로비저너도 없다")
                    .isEmpty();
        });
        Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

        // 값이 아니라 성질을 잰다. autoCreate 를 켜면 여기서 브로커를 찾다가 15초가 걸렸다(실측).
        assertThat(elapsed)
                .as("이 값이 초 단위로 나오면 setAutoCreate(false) 가 빠진 것이다."
                        + " CI 부하로 깨졌다면 예산을 올리지 말고 성질 단언으로 바꿔라 —"
                        + " 예산을 키우는 순간 이 테스트는 아무것도 안 지킨다")
                .isLessThan(Duration.ofSeconds(5));
    }

    /**
     * 지표를 프로비저너 빈 안에서 등록하면 <b>가장 위험한 설정에서만 시계열이 사라진다</b> —
     * 토픽을 아무도 안 만드는 회차가 정확히 "반영됐는지" 를 봐야 하는 회차다.
     */
    @Test
    @DisplayName("프로비저닝을 꺼도 반영 여부 지표는 남는다")
    void provisioningMetersSurviveWhenProvisioningIsOff() {
        enabled().run(context -> {
            assertThat(context.getBeansOfType(KafkaTopicProvisioner.class))
                    .as("이 회차는 토픽을 만들지 않는다")
                    .isEmpty();
            assertThat(context).hasSingleBean(io.micrometer.core.instrument.binder.MeterBinder.class);
        });
    }

    /**
     * 이미 있는 토픽의 설정을 우리 선언에 맞추는 스위치. <b>게터가 없어 필드로 확인한다.</b>
     *
     * <p>기본값 false 면 기존 토픽의 {@code min.insync.replicas}·보존을 검사도 수정도 안 한다.
     * 그러면 브로커가 자동 생성한 토픽이 ISR 하한 1 로 남고, {@code acks=all} 이 "복제본 한 대에만
     * 썼는데 성공" 이 된다 — 조용해서 더 나쁘다. 그리고 이 스위치는 실수로 빠뜨리기 쉽다:
     * 실제로 이 티켓에서 "켰다" 고 보고해 놓고 코드에 없던 적이 있다.
     */
    @Test
    @DisplayName("기존 토픽의 설정도 선언에 맞춘다")
    void existingTopicConfigsAreBroughtInLine() {
        enabled().run(context -> {
            KafkaAdmin admin = context.getBean(KafkaAdmin.class);

            assertThat((boolean) requiredField(KafkaAdmin.class, admin, "modifyTopicConfigs"))
                    .as("꺼져 있으면 ISR 하한과 보존이 브로커 기본값으로 남고 아무도 모른다."
                            + " NoSuchFieldException 으로 깨졌다면 spring-kafka 업그레이드로 내부"
                            + " 필드명이 바뀐 것이다 — 그때는 게터가 생겼는지 먼저 확인할 것")
                    .isTrue();
        });
    }

    /**
     * 프로비저닝을 끈 회차는 지표가 {@code N_A} 를 내는데 그건 정상값처럼 읽힌다. 기동 로그 한 줄이
     * 그 회차가 조용하지 않게 하는 최소 장치다.
     */
    @Test
    @DisplayName("프로비저닝을 끄면 기동에 경고가 실제로 나간다")
    void turningProvisioningOffIsNotSilent(CapturedOutput output) {
        enabled().run(context -> assertThat(output)
                .as("빈이 있는 것과 경고가 나가는 것은 다르다 — 본문을 debug 로 낮춰도 빈은 그대로다")
                .contains("kafka.provision-topics=false"));
    }

    @Test
    @DisplayName("프로비저닝을 켠 회차에는 그 경고가 없다")
    void provisioningEnabledStaysQuiet() {
        runner.withPropertyValues("kafka.enabled=true")
                .run(context -> assertThat(context.containsBean("kafkaTopicProvisioningDisabledNotice"))
                        .isFalse());
    }

    /**
     * 값을 <b>적지 않았을 때</b>를 본다. 이 경로가 없으면 {@code matchIfMissing = false} 로
     * 바꾸는 실수나 속성 이름 오타를 아무도 못 잡고, 그 환경에서는 토픽이 영원히 안 만들어진다.
     */
    @Test
    @DisplayName("provision-topics 를 적지 않으면 프로비저너가 켜져 있다")
    void provisionerIsOnWhenThePropertyIsAbsent() {
        runner.withPropertyValues("kafka.enabled=true").run(context ->
                assertThat(context).hasSingleBean(KafkaTopicProvisioner.class));
    }

    /** 선언값을 보는 테스트가 있지도 않은 브로커에 접속을 시도할 이유가 없다. */
    private ApplicationContextRunner enabled() {
        return runner.withPropertyValues("kafka.enabled=true", "kafka.provision-topics=false");
    }

    private static Map<String, NewTopic> topics(org.springframework.context.ApplicationContext context) {
        return context.getBeansOfType(NewTopic.class);
    }

    /**
     * 필드가 사라지면 {@code NoSuchFieldException} 만 남아 이 테스트가 무엇을 지키려 했는지가
     * 실패 로그에서 사라진다 — 그러면 "라이브러리 업그레이드로 깨진 테스트" 로 취급돼 단언째로
     * 지워진다. 실제로 이 티켓이 그 사고를 겪었다.
     */
    private static Object requiredField(Class<?> owner, Object target, String name) {
        try {
            java.lang.reflect.Field field = owner.getDeclaredField(name);
            field.setAccessible(true);
            return field.get(target);
        } catch (NoSuchFieldException missing) {
            throw new AssertionError(owner.getSimpleName() + "." + name + " 이 사라졌다."
                    + " spring-kafka 업그레이드로 필드가 바뀐 것이다. 이 단언을 지우지 말고 같은 값을"
                    + " 확인할 새 방법을 찾아라 — 기본값으로 돌아가면 ISR 하한이 브로커 기본값(1)로"
                    + " 남고 acks=all 이 무력해진다", missing);
        } catch (IllegalAccessException denied) {
            throw new AssertionError(denied);
        }
    }

    private static long retentionOf(NewTopic topic) {
        return Long.parseLong(topic.configs().get(TopicConfig.RETENTION_MS_CONFIG));
    }

    private static Map<String, NewTopic> byName(org.springframework.context.ApplicationContext context) {
        return topics(context).values().stream()
                .collect(java.util.stream.Collectors.toMap(NewTopic::name, topic -> topic));
    }
}
