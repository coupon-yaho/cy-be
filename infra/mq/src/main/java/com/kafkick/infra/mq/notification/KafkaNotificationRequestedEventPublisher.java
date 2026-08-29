package com.kafkick.infra.mq.notification;

import java.util.Objects;
import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.kafkick.core.notification.event.NotificationRequestedEvent;
import com.kafkick.core.notification.event.NotificationRequestedEventPublisher;
import com.kafkick.infra.mq.config.KafkaTopicConfig;

@Component
@ConditionalOnProperty("kafka.enabled")
public class KafkaNotificationRequestedEventPublisher implements NotificationRequestedEventPublisher {

    private final KafkaTemplate<String, Object> template;

    public KafkaNotificationRequestedEventPublisher(
            @Qualifier("persistKafkaTemplate") KafkaTemplate<String, Object> template
    ) {
        this.template = Objects.requireNonNull(template, "template");
    }

    @Override
    public void publish(NotificationRequestedEvent event) {
        Objects.requireNonNull(event, "event");
        try {
            template.send(KafkaTopicConfig.NOTIFY, Long.toString(event.memberId()), event).get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("알림 요청 발행 대기가 중단되었습니다.", interrupted);
        } catch (ExecutionException failure) {
            throw new IllegalStateException("알림 요청 발행에 실패했습니다.", failure.getCause());
        }
    }
}
