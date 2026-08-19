// 쿠폰 발급 취소 결과를 외부 응답 규격으로 제공합니다.
package com.kafkick.api.coupon.dto;

import java.time.Instant;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.service.CouponCancelResult;

public record CouponCancelResponse(
        Long issuanceId,
        IssuanceStatus status,
        Instant canceledAt
) {

    public static CouponCancelResponse from(CouponCancelResult result) {
        return new CouponCancelResponse(
                result.issuanceId(),
                result.status(),
                result.canceledAt()
        );
    }
}
