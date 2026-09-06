package com.kafkick.infra.mq.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * <b>이 게이지가 없으면 성공 지표가 거짓말을 하는지 볼 방법이 없다.</b>
 *
 * <p>목 발송기는 안 보내면서 예외도 안 던지므로 {@code app.notify.sent{result=success}} 가
 * 정상적으로 오르고, outbox 도 소비자도 정상으로 보인다. 그 상태와 진짜 발송을 <b>지표에서</b>
 * 가르는 것이 이 게이지 하나다.
 */
class NotificationSenderModeGaugeTest {

    private static double gauge(MeterRegistry registry) {
        return registry.get(NotificationSenderModeGauge.GAUGE).gauge().value();
    }

    private static HttpNotificationSender liveSender() {
        return new HttpNotificationSender("http://notify.test/send",
                Duration.ofMillis(30), Duration.ofMillis(60));
    }

    @Test
    @DisplayName("목이 서면 0 — 성공 지표를 운영 판단에 쓰면 안 되는 상태")
    void reportsZeroWhenTheMockSenderIsInPlace() {
        MeterRegistry registry = new SimpleMeterRegistry();

        NotificationSenderModeGauge mode =
                new NotificationSenderModeGauge(new MockNotificationSender(), registry);

        assertThat(mode.isLive()).isFalse();
        assertThat(gauge(registry)).isEqualTo(0);
    }

    @Test
    @DisplayName("실물이 서면 1")
    void reportsOneWhenTheRealSenderIsInPlace() {
        MeterRegistry registry = new SimpleMeterRegistry();

        NotificationSenderModeGauge mode =
                new NotificationSenderModeGauge(liveSender(), registry);

        assertThat(mode.isLive()).isTrue();
        assertThat(gauge(registry)).isEqualTo(1);
    }

    /**
     * <b>스위치가 아니라 선 빈을 본다.</b> 프로퍼티를 읽으면 프로퍼티와 조건부 배선이
     * 갈렸을 때 지표가 <b>배선이 아니라 의도</b>를 말한다 — 이 게이지가 잡으려는 것이
     * 정확히 그 갈라짐이라, 그때 거짓말하면 존재 이유가 없어진다.
     *
     * <p>그래서 "목이 아니면 실물" 로 판정한다. 나중에 발송기가 하나 더 늘어도 그것은
     * <b>실물</b>이므로 이 판정이 그대로 맞다 — 반대로 "Http 면 1" 로 적으면 새 발송기가
     * 조용히 0 이 되어 정상 운영에 알림이 울린다.
     */
    @Test
    @DisplayName("목이 아닌 발송기는 무엇이든 실물로 센다")
    void anySenderThatIsNotTheMockCountsAsLive() {
        MeterRegistry registry = new SimpleMeterRegistry();

        NotificationSenderModeGauge mode = new NotificationSenderModeGauge(
                (notification, idempotencyKey) -> {
                }, registry);

        assertThat(mode.isLive())
                .as("새 발송기가 늘었을 때 조용히 0 이 되면 정상 운영에 알림이 울린다")
                .isTrue();
        assertThat(gauge(registry)).isEqualTo(1);
    }

    /** 지표 등록기가 없는 컨텍스트에서도 판정 자체는 서야 한다. */
    @Test
    @DisplayName("MeterRegistry 가 없어도 판정은 선다")
    void decidesWithoutAMeterRegistry() {
        assertThat(new NotificationSenderModeGauge(new MockNotificationSender(), null).isLive())
                .isFalse();
        assertThat(new NotificationSenderModeGauge(liveSender(), null).isLive()).isTrue();
    }

    /** 발송기가 없으면 판정할 것이 없다 — 조용히 실물로 세면 안 된다. */
    @Test
    @DisplayName("발송기가 null 이면 그 자리에서 멈춘다")
    void refusesAMissingSender() {
        assertThatThrownBy(() -> new NotificationSenderModeGauge(null, new SimpleMeterRegistry()))
                .isInstanceOf(NullPointerException.class);
    }
}
