package com.kafkick.infra.mq.notification;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;

class NotificationRelayPropertiesTest {
    @Test
    void defaultsMatchRelayContract() {
        NotificationRelayProperties properties = new NotificationRelayProperties();

        assertThat(properties.getLease()).isEqualTo(Duration.ofSeconds(30));
        assertThat(properties.getFixedDelayMs()).isEqualTo(100L);
    }
}
