package com.kafkick.infra.mq.notification;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.notification.event.NotificationRequestedEvent;
import com.kafkick.infra.mq.config.KafkaTopicConfig;

@ExtendWith(MockitoExtension.class)
class KafkaNotificationRequestedEventPublisherTest {
    @Mock KafkaTemplate<String, Object> template;

    @Test
    void publishesDurablyWithMemberPartitionKey() {
        NotificationRequestedEvent event = new NotificationRequestedEvent(
                41L, 20L, 10L, 1, AttemptTrigger.INITIAL,
                Instant.parse("2026-08-29T00:00:00Z"));
        when(template.send(KafkaTopicConfig.NOTIFY, "20", event))
                .thenReturn(CompletableFuture.completedFuture(null));

        new KafkaNotificationRequestedEventPublisher(template).publish(event);

        verify(template).send(KafkaTopicConfig.NOTIFY, "20", event);
    }
}
