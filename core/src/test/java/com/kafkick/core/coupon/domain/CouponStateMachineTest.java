// 런타임과 검증 배치가 공유하는 합법 전이표와 종단 상태를 검증합니다.
package com.kafkick.core.coupon.domain;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CouponStateMachineTest {

    @Test
    @DisplayName("정의된 다섯 이벤트를 합법적인 상태로 전이한다")
    void transitionAllowedEvents() {
        assertThat(CouponStateMachine.transition(
                null,
                IssuanceEventType.ISSUE,
                false
        )).isEqualTo(IssuanceStatus.ISSUED);
        assertThat(CouponStateMachine.transition(
                IssuanceStatus.ISSUED,
                IssuanceEventType.USE,
                false
        )).isEqualTo(IssuanceStatus.USED);
        assertThat(CouponStateMachine.transition(
                IssuanceStatus.USED,
                IssuanceEventType.CANCEL_USE,
                false
        )).isEqualTo(IssuanceStatus.ISSUED);
        assertThat(CouponStateMachine.transition(
                IssuanceStatus.ISSUED,
                IssuanceEventType.CANCEL,
                false
        )).isEqualTo(IssuanceStatus.CANCELLED);
        assertThat(CouponStateMachine.transition(
                IssuanceStatus.ISSUED,
                IssuanceEventType.EXPIRE,
                true
        )).isEqualTo(IssuanceStatus.EXPIRED);
    }

    @Test
    @DisplayName("만료된 USED 쿠폰의 사용 취소는 EXPIRED로 전이한다")
    void cancelExpiredUseToExpired() {
        assertThat(CouponStateMachine.transition(
                IssuanceStatus.USED,
                IssuanceEventType.CANCEL_USE,
                true
        )).isEqualTo(IssuanceStatus.EXPIRED);
    }

    @Test
    @DisplayName("종단 상태에서는 다른 상태로 전이할 수 없다")
    void rejectTransitionFromTerminalStatus() {
        assertThatThrownBy(() -> CouponStateMachine.transition(
                IssuanceStatus.CANCELLED,
                IssuanceEventType.USE,
                false
        ))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("허용되지 않은 쿠폰 상태 전이입니다.");
    }
}
