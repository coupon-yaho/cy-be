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

import com.kafkick.core.notification.event.NotificationRequestedEvent;
import com.kafkick.infra.mq.notification.NotificationResultMeter;
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

    static BackOff retryBackOff() {
        return new SequenceBackOff(1_000L, 5_000L, 20_000L);
    }
}
