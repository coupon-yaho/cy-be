// 쿠폰 사용 취소 후 상태와 취소된 주문 실적을 반환합니다.
package com.kafkick.api.coupon.dto;

import java.time.Instant;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.service.CouponCancelUseResult;

public record CouponCancelUseResponse(
        Long issuanceId,
        IssuanceStatus status,
        Long orderId,
        int discountAmount,
        Instant canceledAt
) {

    public static CouponCancelUseResponse from(
            CouponCancelUseResult result
    ) {
        return new CouponCancelUseResponse(
                result.issuanceId(),
                result.status(),
                result.orderId(),
                result.discountAmount(),
                result.canceledAt()
        );
    }
}
