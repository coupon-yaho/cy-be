// 쿠폰 발급 취소 후 상태와 취소 시각을 반환합니다.
package com.kafkick.core.coupon.service;

import java.time.Instant;

import com.kafkick.core.coupon.domain.IssuanceStatus;

public record CouponCancelResult(
        Long issuanceId,
        IssuanceStatus status,
        Instant canceledAt
) {
}
