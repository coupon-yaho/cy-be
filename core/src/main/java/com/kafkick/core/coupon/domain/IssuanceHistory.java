// 발급건의 상태 전이를 재현할 수 있도록 ISSUE 이력을 표현합니다.
package com.kafkick.core.coupon.domain;

import java.time.Instant;

public record IssuanceHistory(
        Long id,
        Long issuanceId,
        IssuanceEventType eventType,
        IssuanceStatus fromStatus,
        IssuanceStatus toStatus,
        String reason,
        String requestId,
        Instant createdAt
) {

    public IssuanceHistory {
        validateId(id, "발급 이력 ID", true);
        validateId(issuanceId, "발급 ID", false);
        if (eventType == null || toStatus == null) {
            throw new IllegalArgumentException(
                    "발급 이벤트와 변경 상태는 필수입니다."
            );
        }
        if (reason != null && reason.length() > 120) {
            throw new IllegalArgumentException(
                    "발급 이력 사유는 120자 이하여야 합니다."
            );
        }
        if (requestId != null && requestId.length() > 36) {
            throw new IllegalArgumentException(
                    "요청 ID는 36자 이하여야 합니다."
            );
        }
        if (createdAt == null) {
            throw new IllegalArgumentException(
                    "발급 이력 생성 시각은 필수입니다."
            );
        }
    }

    public static IssuanceHistory issue(
            Long issuanceId,
            String requestId,
            Instant createdAt
    ) {
        return new IssuanceHistory(
                null,
                issuanceId,
                IssuanceEventType.ISSUE,
                null,
                CouponStateMachine.transition(
                        null,
                        IssuanceEventType.ISSUE,
                        null,
                        createdAt
                ),
                null,
                requestId,
                createdAt
        );
    }

    public static IssuanceHistory use(
            Long issuanceId,
            IssuanceStatus fromStatus,
            Instant expiresAt,
            String idempotencyKey,
            Instant createdAt
    ) {
        return new IssuanceHistory(
                null,
                issuanceId,
                IssuanceEventType.USE,
                fromStatus,
                CouponStateMachine.transition(
                        fromStatus,
                        IssuanceEventType.USE,
                        expiresAt,
                        createdAt
                ),
                null,
                idempotencyKey,
                createdAt
        );
    }

    private static void validateId(
            Long value,
            String fieldName,
            boolean nullable
    ) {
        if ((!nullable && value == null) || (value != null && value <= 0)) {
            throw new IllegalArgumentException(
                    fieldName + "는 0보다 커야 합니다."
            );
        }
    }
}
