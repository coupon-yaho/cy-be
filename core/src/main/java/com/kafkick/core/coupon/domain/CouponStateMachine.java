// 런타임과 검증 배치가 공유할 수 있도록 발급건의 합법적인 상태 전이를 한곳에 정의합니다.
package com.kafkick.core.coupon.domain;

public final class CouponStateMachine {

    private CouponStateMachine() {
    }

    public static IssuanceStatus transition(
            IssuanceStatus currentStatus,
            IssuanceEventType eventType,
            boolean expired
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
            case CANCEL_USE -> require(
                    currentStatus == IssuanceStatus.USED,
                    expired
                            ? IssuanceStatus.EXPIRED
                            : IssuanceStatus.ISSUED
            );
            case CANCEL -> require(
                    currentStatus == IssuanceStatus.ISSUED,
                    IssuanceStatus.CANCELLED
            );
            case EXPIRE -> require(
                    currentStatus == IssuanceStatus.ISSUED,
                    IssuanceStatus.EXPIRED
            );
        };
    }

    private static IssuanceStatus require(
            boolean allowed,
            IssuanceStatus nextStatus
    ) {
        if (!allowed) {
            throw new IllegalStateException(
                    "허용되지 않은 쿠폰 상태 전이입니다."
            );
        }
        return nextStatus;
    }
}
