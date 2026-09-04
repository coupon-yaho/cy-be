package com.kafkick.infra.mq.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.Test;

class NotificationRelayPropertiesTest {
    @Test
    void defaultsMatchRelayContract() {
        NotificationRelayProperties properties = new NotificationRelayProperties();

        assertThat(properties.getLease()).isEqualTo(Duration.ofSeconds(30));
        assertThat(properties.getFixedDelayMs()).isEqualTo(100L);
        assertThat(properties.getBackoffBase()).isEqualTo(Duration.ofMillis(200));
        assertThat(properties.getBackoffCap()).isEqualTo(Duration.ofSeconds(20));
    }

    /**
     * 저장소의 지연 변환기가 365일 위에서 던지는데 그것은 <b>첫 실패가 났을 때</b> 터진다.
     * 설정이 틀린 사실을 기동 시점에 알아야 한다.
     */
    @Test
    void rejectsBackoffBeyondWhatTheAdapterCanPersist() {
        NotificationRelayProperties properties = new NotificationRelayProperties();

        assertThatThrownBy(() -> properties.setBackoffCap(Duration.ofDays(366)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("365일");
    }

    @Test
    void rejectsNonPositiveBackoff() {
        NotificationRelayProperties properties = new NotificationRelayProperties();

        assertThatThrownBy(() -> properties.setBackoffBase(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> properties.setBackoffCap(Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
