// 런타임과 검증 배치가 공유할 수 있도록 발급건의 합법적인 상태 전이를 한곳에 정의합니다.
package com.kafkick.core.coupon.domain;

import java.time.Instant;

import com.kafkick.core.coupon.exception.CouponInvalidTransitionException;

public final class CouponStateMachine {

    private CouponStateMachine() {
    }

    public static IssuanceStatus transition(
            IssuanceStatus currentStatus,
            IssuanceEventType eventType,
            Instant expiresAt,
            Instant at
    ) {
        if (eventType == null) {
            throw new IllegalArgumentException(
                    "쿠폰 상태 전이 이벤트는 필수입니다."
            );
        }

        return switch (eventType) {
            case ISSUE -> require(
                    currentStatus == null,
                    IssuanceStatus.ISSUED
            );
            case USE -> require(
                    currentStatus == IssuanceStatus.ISSUED,
                    IssuanceStatus.USED
            );
            case CANCEL_USE -> cancelUse(
                    currentStatus,
                    expiresAt,
                    at
            );
            case CANCEL -> require(
                    currentStatus == IssuanceStatus.ISSUED,
                    IssuanceStatus.CANCELLED
            );
            case EXPIRE -> expire(
                    currentStatus,
                    expiresAt,
                    at
            );
        };
    }

    private static IssuanceStatus expire(
            IssuanceStatus currentStatus,
            Instant expiresAt,
            Instant at
    ) {
        if (currentStatus != IssuanceStatus.ISSUED
                || !isExpired(expiresAt, at)) {
            throw new CouponInvalidTransitionException();
        }
        return IssuanceStatus.EXPIRED;
    }

    private static IssuanceStatus cancelUse(
            IssuanceStatus currentStatus,
            Instant expiresAt,
            Instant at
    ) {
        if (currentStatus != IssuanceStatus.USED) {
            throw new CouponInvalidTransitionException();
        }
        return isExpired(expiresAt, at)
                ? IssuanceStatus.EXPIRED
                : IssuanceStatus.ISSUED;
    }

    private static boolean isExpired(Instant expiresAt, Instant at) {
        if (expiresAt == null || at == null) {
            throw new IllegalArgumentException(
                    "만료 판정 시각은 필수입니다."
            );
        }
        return at.isAfter(expiresAt);
    }

    private static IssuanceStatus require(
            boolean allowed,
            IssuanceStatus nextStatus
    ) {
        if (!allowed) {
            throw new CouponInvalidTransitionException();
        }
        return nextStatus;
    }
}
