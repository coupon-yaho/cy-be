package com.kafkick.infra.mq.notification;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.kafkick.core.notification.FailureOutcome;
import com.kafkick.core.notification.NotificationDeliveryDecision;
import com.kafkick.core.notification.NotificationDeliveryService;
import com.kafkick.core.notification.NotificationSendException;
import com.kafkick.core.notification.NotificationSender;
import com.kafkick.core.notification.domain.NotifyFailureReason;
import com.kafkick.core.notification.event.NotificationRequestedEvent;
import com.kafkick.infra.mq.config.KafkaConsumerGroups;
import com.kafkick.infra.mq.config.KafkaTopicConfig;

@Component
@ConditionalOnProperty("kafka.enabled")
public class NotificationDispatchConsumer {

    private final NotificationDeliveryService deliveries;
    private final NotificationSender sender;
    private final NotificationResultMeter meter;
    private final Clock clock;

    public NotificationDispatchConsumer(NotificationDeliveryService deliveries,
            NotificationSender sender, NotificationResultMeter meter, Clock clock) {
        this.deliveries = Objects.requireNonNull(deliveries, "deliveries");
        this.sender = Objects.requireNonNull(sender, "sender");
        this.meter = Objects.requireNonNull(meter, "meter");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @KafkaListener(topics = KafkaTopicConfig.NOTIFY,
            groupId = KafkaConsumerGroups.NOTIFY_DISPATCH,
            containerFactory = "notificationListenerContainerFactory")
    public void consume(ConsumerRecord<String, NotificationRequestedEvent> record,
            Acknowledgment acknowledgment) {
        Objects.requireNonNull(record, "record");
        Objects.requireNonNull(acknowledgment, "acknowledgment");
        Instant startedAt = clock.instant();
        NotificationDeliveryDecision decision = deliveries.prepare(record.value(), startedAt);
        if (decision.action() == NotificationDeliveryDecision.Action.ACKNOWLEDGE) {
            acknowledgment.acknowledge();
            return;
        }

        try {
            sender.send(decision.notification(), decision.idempotencyKey());
        } catch (NotificationSendException failure) {
            settleFailure(decision, failure.reason(), failure, startedAt, acknowledgment);
            return;
        } catch (RuntimeException failure) {
            settleFailure(decision, NotifyFailureReason.UNKNOWN, failure, startedAt, acknowledgment);
            return;
        }

        boolean winner = deliveries.completeSuccess(
                decision, startedAt, clock.instant());
        if (winner) {
            meter.success();
        }
        acknowledgment.acknowledge();
    }

    private void settleFailure(NotificationDeliveryDecision decision, NotifyFailureReason reason,
            RuntimeException cause, Instant startedAt, Acknowledgment acknowledgment) {
        FailureOutcome outcome = deliveries.completeFailure(
                decision, reason, startedAt, clock.instant());
        if (outcome == FailureOutcome.DUPLICATE) {
            acknowledgment.acknowledge();
            return;
        }
        if (outcome == FailureOutcome.TERMINAL) {
            meter.failure();
            throw new NotificationTerminalFailureException(cause);
        }
        throw new NotificationRetryableException(cause);
    }
}
