package com.kafkick.infra.mq.config;

import java.time.Clock;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.util.backoff.BackOff;

import com.kafkick.core.notification.NotificationSender;
import com.kafkick.core.notification.event.NotificationRequestedEvent;
import com.kafkick.infra.mq.notification.NotificationResultMeter;
import com.kafkick.infra.mq.notification.NotificationSenderModeGauge;
import com.kafkick.infra.mq.notification.NotificationTerminalFailureException;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import tools.jackson.databind.json.JsonMapper;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty("kafka.enabled")
@EnableKafka
public class NotificationConsumerConfig {
    static final int CONCURRENCY = 3;

    @Bean
    public ConsumerFactory<String, NotificationRequestedEvent> notificationConsumerFactory(
            KafkaConnectionProperties properties, JsonMapper jsonMapper) {
        Objects.requireNonNull(jsonMapper, "jsonMapper");
        Map<String, Object> config = KafkaConsumerGroups.consumerConfig(
                KafkaConsumerGroups.NOTIFY_DISPATCH,
                KafkaProducerSupport.requireBootstrapServers(properties));
        return new DefaultKafkaConsumerFactory<>(config,
                new ErrorHandlingDeserializer<>(new StringDeserializer()),
                new ErrorHandlingDeserializer<>(new JacksonJsonDeserializer<>(
                        NotificationRequestedEvent.class, jsonMapper, false)));
    }

    @Bean("notificationListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, NotificationRequestedEvent>
            notificationListenerContainerFactory(
                    @Qualifier("notificationConsumerFactory")
                    ConsumerFactory<String, NotificationRequestedEvent> consumerFactory,
                    @Qualifier("deadLetterKafkaTemplate") KafkaTemplate<String, Object> dltTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, NotificationRequestedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(CONCURRENCY);
        factory.getContainerProperties().setAckMode(AckMode.MANUAL);

        DefaultErrorHandler handler = new DefaultErrorHandler(
                new DeadLetterPublishingRecoverer(dltTemplate), retryBackOff());
        handler.addNotRetryableExceptions(NotificationTerminalFailureException.class);
        factory.setCommonErrorHandler(handler);
        return factory;
    }

    @Bean
    public NotificationResultMeter notificationResultMeter(
            ObjectProvider<MeterRegistry> meterRegistries) {
        return new NotificationResultMeter(
                meterRegistries.getIfAvailable(SimpleMeterRegistry::new));
    }

    /**
     * <b>{@link NotificationResultMeter} 의 짝이다.</b> 그쪽이 세는 성공은 <b>목 발송기가
     * 선 환경에서도 오른다</b> — 아무 데도 안 보내고 예외도 안 던지기 때문이다. 이 게이지가
     * 없으면 그 성공이 진짜인지 볼 방법이 지표에 없다.
     *
     * <p><b>발송기 빈을 받아 실제로 무엇이 섰는지 본다.</b> 프로퍼티를 읽으면 프로퍼티와
     * 조건부 배선이 갈렸을 때 지표가 배선이 아니라 <b>의도</b>를 말한다.
     */
    @Bean
    public NotificationSenderModeGauge notificationSenderModeGauge(
            NotificationSender notificationSender,
            ObjectProvider<MeterRegistry> meterRegistries) {
        return new NotificationSenderModeGauge(notificationSender,
                meterRegistries.getIfAvailable(SimpleMeterRegistry::new));
    }

    static BackOff retryBackOff() {
        return new SequenceBackOff(1_000L, 5_000L, 20_000L);
    }
}
