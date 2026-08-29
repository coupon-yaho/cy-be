package com.kafkick.infra.mq.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DefaultErrorHandler;

import tools.jackson.databind.json.JsonMapper;

class NotificationConsumerConfigTest {
    private final NotificationConsumerConfig config = new NotificationConsumerConfig();

    @Test
    void dispatchFactoryUsesEarliestManualAckAndBoundedConcurrency() {
        var consumerFactory = config.notificationConsumerFactory(
                new KafkaConnectionProperties("broker:9092"), JsonMapper.builder().build());
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, Object> dltTemplate = org.mockito.Mockito.mock(KafkaTemplate.class);

        var listenerFactory = config.notificationListenerContainerFactory(
                consumerFactory, dltTemplate);
        var container = listenerFactory.createContainer(KafkaTopicConfig.NOTIFY);

        assertThat(consumerFactory.getConfigurationProperties())
                .containsEntry(ConsumerConfig.GROUP_ID_CONFIG, KafkaConsumerGroups.NOTIFY_DISPATCH)
                .containsEntry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConsumerGroups.EARLIEST)
                .containsEntry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        assertThat(container.getContainerProperties().getAckMode()).isEqualTo(AckMode.MANUAL);
        assertThat(container.getConcurrency()).isPositive()
                .isLessThanOrEqualTo(KafkaTopicConfig.PARTITIONS);
        assertThat(container.getCommonErrorHandler()).isInstanceOf(DefaultErrorHandler.class);
    }
}
