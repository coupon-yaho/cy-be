package com.kafkick.infra.mq.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.MicrometerConsumerListener;

import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.infra.mq.attempt.AttemptArchiveConsumer;
import com.kafkick.infra.mq.attempt.AttemptLiveConsumer;

import tools.jackson.databind.json.JsonMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * 이 티켓의 1번 인수 조건 — <b>두 컨슈머의 {@code group.id} 가 달라야 한다.</b>
 *
 * <p>같으면 둘이 파티션을 나눠 가져 화면과 적재가 각각 절반씩만 본다. 그런데 <b>둘 다 정상처럼
 * 보인다</b> — 화면에는 이벤트가 흐르고 DB 에도 행이 쌓인다. 어긋난다는 사실은 두 원천을
 * 대조해야만 드러나고, 대조하는 코드는 어디에도 없다. 그래서 여기서 고정한다.
 */
class AttemptConsumerConfigTest {

    private static final KafkaConnectionProperties PROPERTIES =
            new KafkaConnectionProperties("localhost:9094");

    @Test
    void givesTheTwoConsumersDifferentGroupIds() {
        AttemptConsumerConfig config = new AttemptConsumerConfig();

        String live = groupIdOf(config.attemptLiveConsumerFactory(PROPERTIES, jsonMapper()));
        String archive = groupIdOf(config.attemptArchiveConsumerFactory(PROPERTIES, jsonMapper()));

        assertThat(live).isEqualTo(KafkaConsumerGroups.ATTEMPT_LIVE);
        assertThat(archive).isEqualTo(KafkaConsumerGroups.ATTEMPT_ARCHIVE);
        assertThat(live).as("같으면 파티션을 나눠 가져 각자 절반만 본다").isNotEqualTo(archive);
    }

    /**
     * 리스너 애노테이션이 <b>자기 그룹의</b> 팩토리를 가리키는지.
     *
     * <p>팩토리를 둘 만들어도 리스너가 둘 다 같은 것을 가리키면 그룹이 하나가 된다. 위 테스트는
     * 팩토리만 보므로 그 배선 오류를 못 잡는다 — 빈은 둘 다 만들어지고 하나는 그냥 안 쓰인다.
     */
    @Test
    void pointsEachListenerAtItsOwnContainerFactory() {
        assertThat(listenerOf(AttemptLiveConsumer.class).containerFactory())
                .isEqualTo(AttemptConsumerConfig.LIVE_CONTAINER_FACTORY);
        assertThat(listenerOf(AttemptLiveConsumer.class).groupId())
                .isEqualTo(KafkaConsumerGroups.ATTEMPT_LIVE);

        assertThat(listenerOf(AttemptArchiveConsumer.class).containerFactory())
                .isEqualTo(AttemptConsumerConfig.ARCHIVE_CONTAINER_FACTORY);
        assertThat(listenerOf(AttemptArchiveConsumer.class).groupId())
                .isEqualTo(KafkaConsumerGroups.ATTEMPT_ARCHIVE);
    }

    /** 둘 다 attempt 토픽 하나만 읽는다. persist 를 잘못 섞으면 정합성 토픽이 관측에 끌려온다. */
    @Test
    void readsOnlyTheAttemptTopic() {
        assertThat(listenerOf(AttemptLiveConsumer.class).topics())
                .containsExactly(KafkaTopicConfig.ISSUE_ATTEMPT);
        assertThat(listenerOf(AttemptArchiveConsumer.class).topics())
                .containsExactly(KafkaTopicConfig.ISSUE_ATTEMPT);
    }

    /**
     * offset 정책은 {@link KafkaConsumerGroups} 가 소유한다. 팩토리가 그 문을 지나는지 본다.
     *
     * <p>둘 다 {@code latest} 여야 한다. {@code earliest} 로 두면 컨슈머가 잠깐 내려갔다 올 때마다
     * 밀린 전량을 DB 로 밀어 넣는다 — 초당 수천 건이 쌓이는 토픽이라, 관측이 측정 대상을 죽인다.
     */
    @Test
    void carriesTheGroupOffsetPolicyFromTheOwningTable() {
        AttemptConsumerConfig config = new AttemptConsumerConfig();

        assertThat(configOf(config.attemptLiveConsumerFactory(PROPERTIES, jsonMapper())))
                .containsEntry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
                .containsEntry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
                .containsEntry(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        assertThat(configOf(config.attemptArchiveConsumerFactory(PROPERTIES, jsonMapper())))
                .containsEntry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    }

    @Test
    void bindsNativeKafkaConsumerLagMetricsToBothFactories() {
        AttemptConsumerConfig config = new AttemptConsumerConfig();
        ConsumerFactory<String, IssuanceFlowEvent> live =
                config.attemptLiveConsumerFactory(PROPERTIES, jsonMapper());
        ConsumerFactory<String, IssuanceFlowEvent> archive =
                config.attemptArchiveConsumerFactory(PROPERTIES, jsonMapper());
        StaticListableBeanFactory beans = new StaticListableBeanFactory();
        beans.addBean("meterRegistry", new SimpleMeterRegistry());

        config.attemptConsumerMetricsBinder(
                live, archive, beans.getBeanProvider(MeterRegistry.class))
                .afterSingletonsInstantiated();

        assertThat(live.getListeners()).singleElement().isInstanceOf(MicrometerConsumerListener.class);
        assertThat(archive.getListeners()).singleElement().isInstanceOf(MicrometerConsumerListener.class);
    }

    private static String groupIdOf(ConsumerFactory<String, IssuanceFlowEvent> factory) {
        return String.valueOf(configOf(factory).get(ConsumerConfig.GROUP_ID_CONFIG));
    }

    private static Map<String, Object> configOf(ConsumerFactory<String, IssuanceFlowEvent> factory) {
        return factory.getConfigurationProperties();
    }

    private static KafkaListener listenerOf(Class<?> consumer) {
        List<Method> annotated = Arrays.stream(consumer.getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(KafkaListener.class))
                .toList();
        assertThat(annotated).as("%s 에 @KafkaListener 가 정확히 하나", consumer.getSimpleName())
                .hasSize(1);
        return annotated.get(0).getAnnotation(KafkaListener.class);
    }

    private static JsonMapper jsonMapper() {
        return JsonMapper.builder().findAndAddModules().build();
    }
}
