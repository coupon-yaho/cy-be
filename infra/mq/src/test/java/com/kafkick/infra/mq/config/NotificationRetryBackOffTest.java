package com.kafkick.infra.mq.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.util.backoff.BackOffExecution;

class NotificationRetryBackOffTest {
    @Test
    void exposesExactlyThreeRetryIntervals() {
        BackOffExecution execution = NotificationConsumerConfig.retryBackOff().start();

        assertThat(execution.nextBackOff()).isEqualTo(1_000L);
        assertThat(execution.nextBackOff()).isEqualTo(5_000L);
        assertThat(execution.nextBackOff()).isEqualTo(20_000L);
        assertThat(execution.nextBackOff()).isEqualTo(BackOffExecution.STOP);
    }
}
