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

    /** 저장된 이력의 상태 삼중항이 런타임 전이 규칙에 포함되는지 판정합니다. */
    public static boolean isLegal(
            IssuanceStatus currentStatus,
            IssuanceEventType eventType,
            IssuanceStatus nextStatus
    ) {
        if (eventType == null) {
            return false;
        }
        return switch (eventType) {
            case ISSUE -> currentStatus == null
                    && nextStatus == IssuanceStatus.ISSUED;
            case USE -> currentStatus == IssuanceStatus.ISSUED
                    && nextStatus == IssuanceStatus.USED;
            case CANCEL_USE -> currentStatus == IssuanceStatus.USED
                    && (nextStatus == IssuanceStatus.ISSUED
                    || nextStatus == IssuanceStatus.EXPIRED);
            case CANCEL -> currentStatus == IssuanceStatus.ISSUED
                    && nextStatus == IssuanceStatus.CANCELLED;
            case EXPIRE -> currentStatus == IssuanceStatus.ISSUED
                    && nextStatus == IssuanceStatus.EXPIRED;
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
