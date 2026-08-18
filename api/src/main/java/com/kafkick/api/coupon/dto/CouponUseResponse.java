// 쿠폰 사용으로 확정된 상태와 실제 할인 실적을 반환합니다.
package com.kafkick.api.coupon.dto;

import java.time.Instant;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.service.CouponUseResult;

public record CouponUseResponse(
        Long issuanceId,
        IssuanceStatus status,
        Long orderId,
        int discountAmount,
        Instant usedAt
) {

    public static CouponUseResponse from(CouponUseResult result) {
        return new CouponUseResponse(
                result.issuanceId(),
                result.status(),
                result.orderId(),
                result.discountAmount(),
                result.usedAt()
        );
    }
}
