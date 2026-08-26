package com.kafkick.core.coupon.service;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupon.port.CouponRoundLifecyclePort;
import com.kafkick.core.coupon.port.CouponRoundScheduleLockPort;
import com.kafkick.core.coupon.service.result.CouponRoundLifecycleResult;
import com.kafkick.core.observation.CampaignClosedEvent;
import com.kafkick.core.observation.CampaignClosedEventPublisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponRoundLifecycleServiceTest {

    @Mock
    private CouponRoundLifecyclePort lifecyclePort;

    @Mock
    private CouponRoundScheduleLockPort scheduleLockPort;

    @Mock
    private CampaignClosedEventPublisher campaignClosedEventPublisher;

    @Test
    @DisplayName("같은 기준 시각으로 종료된 OPEN과 놓친 SCHEDULED를 닫은 뒤 대상 회차를 연다")
    void synchronizeCouponRoundLifecycleInSafeOrder() {
        Instant asOf = Instant.parse("2026-08-25T03:00:00Z");
        CouponRoundLifecycleService service =
                new CouponRoundLifecycleService(
                        lifecyclePort,
                        scheduleLockPort,
                        campaignClosedEventPublisher
                );
        when(lifecyclePort.closeOpenRounds(asOf)).thenReturn(List.of(11L, 12L));
        when(lifecyclePort.closeMissedScheduledRounds(asOf)).thenReturn(2);
        when(lifecyclePort.openScheduledRounds(asOf)).thenReturn(1);

        CouponRoundLifecycleResult result = service.synchronize(asOf);

        assertThat(result).isEqualTo(
                new CouponRoundLifecycleResult(2, 2, 1)
        );
        InOrder order = inOrder(
                scheduleLockPort,
                lifecyclePort,
                campaignClosedEventPublisher
        );
        order.verify(scheduleLockPort).lock();
        order.verify(lifecyclePort).closeOpenRounds(asOf);
        order.verify(lifecyclePort).closeMissedScheduledRounds(asOf);
        order.verify(lifecyclePort).openScheduledRounds(asOf);
        order.verify(campaignClosedEventPublisher).publishAfterCommit(
                new CampaignClosedEvent(11L, asOf)
        );
        order.verify(campaignClosedEventPublisher).publishAfterCommit(
                new CampaignClosedEvent(12L, asOf)
        );
    }

    @Test
    @DisplayName("종료된 OPEN 회차가 없으면 종료 이벤트를 등록하지 않는다")
    void doNotRegisterEventWithoutClosedOpenRound() {
        Instant asOf = Instant.parse("2026-08-25T03:00:00Z");
        CouponRoundLifecycleService service =
                new CouponRoundLifecycleService(
                        lifecyclePort,
                        scheduleLockPort,
                        campaignClosedEventPublisher
                );
        when(lifecyclePort.closeOpenRounds(asOf)).thenReturn(List.of());

        CouponRoundLifecycleResult result = service.synchronize(asOf);

        assertThat(result.closedOpenCount()).isZero();
        verifyNoInteractions(campaignClosedEventPublisher);
    }

    @Test
    @DisplayName("회차 상태 동기화 기준 시각이 없으면 거부한다")
    void rejectMissingLifecycleAsOf() {
        CouponRoundLifecycleService service =
                new CouponRoundLifecycleService(
                        lifecyclePort,
                        scheduleLockPort,
                        campaignClosedEventPublisher
                );

        assertThatThrownBy(() -> service.synchronize(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("쿠폰 회차 상태 전환 기준 시각은 필수입니다.");
    }
}
