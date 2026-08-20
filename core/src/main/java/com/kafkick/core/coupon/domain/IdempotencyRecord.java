package com.kafkick.core.coupon.domain;

import java.time.Instant;

public record IdempotencyRecord(
        String key,
        Long memberId,
        Long issuanceId,
        String requestHash,
        IdempotencyStatus status,
        String responseBody,
        Instant createdAt
) {

    public IdempotencyRecord {
        if (key == null || key.isBlank() || key.length() > 36) {
            throw new IllegalArgumentException(
                    "멱등키는 36자 이하여야 합니다."
            );
        }
        if (requestHash == null || requestHash.length() != 64) {
            throw new IllegalArgumentException(
                    "요청 해시는 64자리여야 합니다."
            );
        }
        if (status == null || createdAt == null) {
            throw new IllegalArgumentException(
                    "멱등 상태와 생성 시각은 필수입니다."
            );
        }
        if (status == IdempotencyStatus.DONE
                && (memberId == null
                || issuanceId == null
                || responseBody == null)) {
            throw new IllegalArgumentException(
                    "완료된 멱등 레코드에는 처리 결과가 필요합니다."
            );
        }
    }
}
