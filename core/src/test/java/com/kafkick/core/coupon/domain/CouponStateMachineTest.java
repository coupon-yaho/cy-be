package com.kafkick.core.coupon.domain;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.coupon.exception.CouponInvalidTransitionException;
import com.kafkick.core.coupon.exception.CouponIssueErrorCode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// 런타임과 검증 배치가 공유하는 합법 전이표와 종단 상태를 검증합니다.

class CouponStateMachineTest {

    private static final Instant EXPIRES_AT =
            Instant.parse("2026-08-25T05:00:00Z");
    private static final Instant BEFORE_EXPIRATION =
            Instant.parse("2026-08-25T04:59:59Z");
    private static final Instant AFTER_EXPIRATION =
            Instant.parse("2026-08-25T05:00:01Z");

    @Test
    @DisplayName("정의된 다섯 이벤트를 합법적인 상태로 전이한다")
    void transitionAllowedEvents() {
        assertThat(CouponStateMachine.transition(
                null,
                IssuanceEventType.ISSUE,
                EXPIRES_AT,
                BEFORE_EXPIRATION
        )).isEqualTo(IssuanceStatus.ISSUED);
        assertThat(CouponStateMachine.transition(
                IssuanceStatus.ISSUED,
                IssuanceEventType.USE,
                EXPIRES_AT,
                BEFORE_EXPIRATION
        )).isEqualTo(IssuanceStatus.USED);
        assertThat(CouponStateMachine.transition(
                IssuanceStatus.USED,
                IssuanceEventType.CANCEL_USE,
                EXPIRES_AT,
                BEFORE_EXPIRATION
        )).isEqualTo(IssuanceStatus.ISSUED);
        assertThat(CouponStateMachine.transition(
                IssuanceStatus.ISSUED,
                IssuanceEventType.CANCEL,
                EXPIRES_AT,
                BEFORE_EXPIRATION
        )).isEqualTo(IssuanceStatus.CANCELLED);
        assertThat(CouponStateMachine.transition(
                IssuanceStatus.ISSUED,
                IssuanceEventType.EXPIRE,
                EXPIRES_AT,
                AFTER_EXPIRATION
        )).isEqualTo(IssuanceStatus.EXPIRED);
    }

    @Test
    @DisplayName("만료된 USED 쿠폰의 사용 취소는 EXPIRED로 전이한다")
    void cancelExpiredUseToExpired() {
        assertThat(CouponStateMachine.transition(
                IssuanceStatus.USED,
                IssuanceEventType.CANCEL_USE,
                EXPIRES_AT,
                AFTER_EXPIRATION
        )).isEqualTo(IssuanceStatus.EXPIRED);
    }

    @Test
    @DisplayName("저장된 이력의 합법 상태 전이를 만료 시각 없이 판정한다")
    void recognizesLegalPersistedTransitions() {
        assertThat(CouponStateMachine.isLegal(
                null, IssuanceEventType.ISSUE, IssuanceStatus.ISSUED)).isTrue();
        assertThat(CouponStateMachine.isLegal(
                IssuanceStatus.ISSUED, IssuanceEventType.USE, IssuanceStatus.USED)).isTrue();
        assertThat(CouponStateMachine.isLegal(
                IssuanceStatus.USED, IssuanceEventType.CANCEL_USE, IssuanceStatus.ISSUED)).isTrue();
        assertThat(CouponStateMachine.isLegal(
                IssuanceStatus.USED, IssuanceEventType.CANCEL_USE, IssuanceStatus.EXPIRED)).isTrue();
        assertThat(CouponStateMachine.isLegal(
                IssuanceStatus.ISSUED, IssuanceEventType.CANCEL, IssuanceStatus.CANCELLED)).isTrue();
        assertThat(CouponStateMachine.isLegal(
                IssuanceStatus.ISSUED, IssuanceEventType.EXPIRE, IssuanceStatus.EXPIRED)).isTrue();
    }

    @Test
    @DisplayName("저장된 이력의 불법 상태 전이를 거부한다")
    void rejectsIllegalPersistedTransitions() {
        assertThat(CouponStateMachine.isLegal(
                IssuanceStatus.USED, IssuanceEventType.USE, IssuanceStatus.ISSUED)).isFalse();
        assertThat(CouponStateMachine.isLegal(
                IssuanceStatus.CANCELLED, IssuanceEventType.USE, IssuanceStatus.USED)).isFalse();
        assertThat(CouponStateMachine.isLegal(
                IssuanceStatus.ISSUED, null, IssuanceStatus.USED)).isFalse();
    }

    @Test
    @DisplayName("종단 상태에서는 다른 상태로 전이할 수 없다")
    void rejectTransitionFromTerminalStatus() {
        assertThatThrownBy(() -> CouponStateMachine.transition(
                IssuanceStatus.CANCELLED,
                IssuanceEventType.USE,
                EXPIRES_AT,
                BEFORE_EXPIRATION
        ))
                .isInstanceOfSatisfying(
                        CouponInvalidTransitionException.class,
                        exception -> assertThat(exception.getErrorCode())
                                .isEqualTo(
                                        CouponIssueErrorCode.INVALID_TRANSITION
                                )
                );
    }

    @Test
    @DisplayName("사용 취소의 만료 판정 시각이 없으면 거부한다")
    void rejectCancelUseWithoutExpirationTime() {
        assertThatThrownBy(() -> CouponStateMachine.transition(
                IssuanceStatus.USED,
                IssuanceEventType.CANCEL_USE,
                null,
                BEFORE_EXPIRATION
        ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("만료 판정 시각은 필수입니다.");
    }

    @Test
    @DisplayName("만료 시각이 지나기 전에는 EXPIRE 전이를 거부한다")
    void rejectExpireBeforeExpiration() {
        assertThatThrownBy(() -> CouponStateMachine.transition(
                IssuanceStatus.ISSUED,
                IssuanceEventType.EXPIRE,
                EXPIRES_AT,
                BEFORE_EXPIRATION
        ))
                .isInstanceOfSatisfying(
                        CouponInvalidTransitionException.class,
                        exception -> assertThat(exception.getErrorCode())
                                .isEqualTo(
                                        CouponIssueErrorCode.INVALID_TRANSITION
                                )
                );
    }
}
