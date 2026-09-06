package com.kafkick.infra.mq.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.notification.NotificationOutboxRepository;
import com.kafkick.core.observation.DomainMeterNames;

/**
 * 백로그 게이지가 <b>세는 값과 못 세는 상태</b>를 어떻게 다루는지 못 박는다.
 *
 * <p>이 값이 틀리는 방향은 전부 <b>더 평온해 보이는</b> 쪽이라 아무도 모른다 —
 * 못 셌는데 0 을 넣으면 "다 나갔다" 로, 실패에 0 으로 덮으면 "방금 다 비웠다" 로 읽힌다.
 * 그래서 <b>못 센 상태를 값으로 표시</b>하고, 실패에는 <b>직전 값을 유지</b>한다.
 */
@ExtendWith(MockitoExtension.class)
class NotificationOutboxBacklogGaugeTest {

    @Mock NotificationOutboxRepository outboxes;

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    private NotificationOutboxBacklogGauge gauge() {
        return new NotificationOutboxBacklogGauge(outboxes, registry);
    }

    private double gaugeValue() {
        return registry.find(DomainMeterNames.OUTBOX_BACKLOG).gauge().value();
    }

    /**
     * <b>한 번도 못 센 상태와 "백로그 0" 을 가른다.</b> 둘을 0 으로 합치면
     * <b>DB 를 못 읽는 상황이 가장 평온해 보인다</b> — 알림이 그 둘을 구분해야 한다.
     */
    @Test
    @DisplayName("아직 한 번도 못 세면 -1 이다 — 0 이 아니다")
    void startsAtMinusOneBeforeTheFirstCount() {
        gauge();

        assertThat(gaugeValue())
                .as("0 이면 '다 나갔다' 로 읽혀 못 보는 상태가 가장 평온해 보인다")
                .isEqualTo(-1);
    }

    @Test
    @DisplayName("센 값이 게이지에 들어간다")
    void publishesTheCountedBacklog() {
        when(outboxes.countBacklog()).thenReturn(42L);
        NotificationOutboxBacklogGauge gauge = gauge();

        gauge.refresh();

        assertThat(gaugeValue()).isEqualTo(42);
    }

    /**
     * <b>못 셌다고 0 을 넣지 않는다.</b> 넣으면 화면이 "방금 다 비웠다" 로 읽는다 —
     * 직전 값이 멈춰 있으면 사람이 <b>값이 안 움직이는 것</b>을 보고 의심할 수 있다.
     */
    @Test
    @DisplayName("조회가 실패하면 직전 값을 유지한다 — 0 으로 덮지 않는다")
    void keepsTheLastValueWhenTheQueryFails() {
        when(outboxes.countBacklog())
                .thenReturn(7L)
                .thenThrow(new IllegalStateException("connection lost"));
        NotificationOutboxBacklogGauge gauge = gauge();

        gauge.refresh();
        gauge.refresh();

        assertThat(gaugeValue())
                .as("0 으로 덮으면 사고가 '다 비웠다' 로 보인다")
                .isEqualTo(7);
    }

    /**
     * <b>계속 실패하면 못 보는 상태로 내린다.</b>
     *
     * <p>직전 값을 무한히 붙들면 그 값이 거짓말이 된다 — 화면에는 백로그가 멈춘 것처럼
     * 보이는데 아무도 안 세고 있고, 알림은 {@code -1} 도 부재도 아니라 <b>영원히
     * 조용하다.</b> 리뷰가 짚었다.
     */
    @Test
    @DisplayName("연속 실패가 이어지면 -1 로 내려 알림이 잡게 한다")
    void givesUpAfterRepeatedFailures() {
        when(outboxes.countBacklog())
                .thenReturn(7L)
                .thenThrow(new IllegalStateException("connection lost"));
        NotificationOutboxBacklogGauge gauge = gauge();

        gauge.refresh();
        for (int attempt = 0; attempt < 5; attempt++) {
            gauge.refresh();
        }

        assertThat(gaugeValue())
                .as("멈춘 값이 계속 보이면 아무 알림도 안 뜬다")
                .isEqualTo(-1);
    }

    /** 한 번이라도 성공하면 실패 셈이 돌아간다 — 간헐 실패가 쌓여 포기하면 안 된다. */
    @Test
    @DisplayName("성공이 끼면 실패 셈이 0 으로 돌아간다")
    void resetsTheFailureStreakOnSuccess() {
        when(outboxes.countBacklog())
                .thenThrow(new IllegalStateException("blip"))
                .thenThrow(new IllegalStateException("blip"))
                .thenThrow(new IllegalStateException("blip"))
                .thenReturn(9L)
                .thenThrow(new IllegalStateException("blip"))
                .thenThrow(new IllegalStateException("blip"));
        NotificationOutboxBacklogGauge gauge = gauge();

        for (int attempt = 0; attempt < 6; attempt++) {
            gauge.refresh();
        }

        assertThat(gaugeValue())
                .as("간헐 실패가 쌓여 포기하면 멀쩡한 값이 사라진다")
                .isEqualTo(9);
    }

    /**
     * <b>예외를 밖으로 안 던진다.</b> 던지면 스케줄러가 계속 다시 부르며 로그만 쌓이는데,
     * 이 값이 늦는 것은 사고가 아니다.
     */
    @Test
    @DisplayName("조회 실패가 스케줄러로 새어 나가지 않는다")
    void doesNotPropagateTheFailure() {
        when(outboxes.countBacklog()).thenThrow(new IllegalStateException("connection lost"));
        NotificationOutboxBacklogGauge gauge = gauge();

        gauge.refresh();

        assertThat(gaugeValue()).isEqualTo(-1);
    }
}
