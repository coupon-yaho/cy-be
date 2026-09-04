package com.kafkick.core.notification.retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class NotificationRetryBackOffPropertiesTest {

    @Test
    void defaultsMatchTheDocumentedSchedule() {
        NotificationRetryBackOffProperties properties = new NotificationRetryBackOffProperties();

        assertThat(properties.getBase()).isEqualTo(Duration.ofMillis(200));
        assertThat(properties.getCap()).isEqualTo(Duration.ofSeconds(20));
    }

    /**
     * 저장소의 지연 변환기가 365일 위에서 던지는데 그것은 <b>첫 실패가 났을 때</b> 터진다.
     * 설정이 틀린 사실을 기동 시점에 알아야 한다.
     */
    @Test
    void rejectsBackoffBeyondWhatTheAdapterCanPersist() {
        NotificationRetryBackOffProperties properties = new NotificationRetryBackOffProperties();

        assertThatThrownBy(() -> properties.setCap(Duration.ofDays(366)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("365일");
    }

    @Test
    void rejectsNonPositiveBackoff() {
        NotificationRetryBackOffProperties properties = new NotificationRetryBackOffProperties();

        assertThatThrownBy(() -> properties.setBase(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> properties.setCap(Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>정책이 한 벌인지는 배선으로 봐야 한다.</b> 두 경로가 각자 {@code new} 로 만들면
     * 값이 같아도 <b>한 벌이 아니다</b> — 갈라지는 순간 아무도 모른다. 그래서 설정이
     * <b>빈 하나</b>를 만들고 양쪽이 그것을 주입받는다.
     */
    @Test
    @DisplayName("설정이 만드는 백오프가 속성 그대로다 — 이 빈 하나를 두 경로가 나눠 쓴다")
    void theConfigBuildsTheBackOffFromTheseProperties() {
        NotificationRetryBackOffProperties properties = new NotificationRetryBackOffProperties();
        properties.setBase(Duration.ofMillis(5));
        properties.setCap(Duration.ofMillis(5));

        FullJitterBackOff backOff =
                new NotificationRetryBackOffConfig().notificationRetryBackOff(properties);

        // base=cap=5ms 면 attempt 와 무관하게 상한이 5ms 다 — 범위로 못 박을 수 있다.
        assertThat(backOff.nextDelay(1)).isBetween(Duration.ZERO, Duration.ofMillis(5));
        assertThat(backOff.nextDelay(9)).isBetween(Duration.ZERO, Duration.ofMillis(5));
    }
}
