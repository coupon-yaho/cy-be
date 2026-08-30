package com.kafkick.core.notification;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.notification.event.NotificationRequestedEvent;

class NotificationRequestedEventTest {
    @Test
    void rejectsInvalidIdentifiersAndAutoTrigger() {
        Instant at = Instant.parse("2026-08-27T00:00:00Z");
        assertThatThrownBy(() -> new NotificationRequestedEvent(0L, 1L, 1L, 1,
                AttemptTrigger.INITIAL, at)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new NotificationRequestedEvent(1L, 1L, 1L, 1,
                AttemptTrigger.AUTO, at)).isInstanceOf(IllegalArgumentException.class);
    }
}
