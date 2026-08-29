package com.kafkick.api.coupon.dto.response;

import java.time.Instant;

import com.kafkick.core.coupon.v2.query.CouponDefinition;
import com.kafkick.core.coupontemplate.domain.CouponPolicyType;

/** 목록에 표시할 정적 쿠폰 정의. 재고·발급 여부는 POST가 판정한다. */
public record V2CouponDefinitionResponse(
        long couponRoundId,
        long brandId,
        String name,
        CouponPolicyType policyType,
        Integer discountRate,
        Integer maxDiscountAmount,
        Integer discountAmount,
        int validDays,
        Instant openAt,
        Instant closeAt
) {
    public static V2CouponDefinitionResponse from(CouponDefinition definition) {
        return new V2CouponDefinitionResponse(definition.couponRoundId(), definition.brandId(),
                definition.name(), definition.policyType(), definition.discountRate(),
                definition.maxDiscountAmount(), definition.discountAmount(), definition.validDays(),
                definition.openAt(), definition.closeAt());
    }
}
