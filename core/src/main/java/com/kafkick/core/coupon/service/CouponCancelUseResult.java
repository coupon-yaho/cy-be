// 쿠폰 사용 취소 후 상태와 취소된 주문 실적을 반환합니다.
package com.kafkick.core.coupon.service;

import java.time.Instant;

import com.kafkick.core.coupon.domain.IssuanceStatus;

public record CouponCancelUseResult(
        Long issuanceId,
        IssuanceStatus status,
        Long orderId,
        int discountAmount,
        Instant canceledAt
) {
}
