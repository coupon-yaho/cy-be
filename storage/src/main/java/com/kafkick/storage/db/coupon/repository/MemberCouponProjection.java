package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;

import com.kafkick.core.coupon.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.IssuanceStatus;

public interface MemberCouponProjection {

    Long getIssuanceId();

    Long getCouponRoundId();

    String getCode();

    IssuanceStatus getStatus();

    String getName();

    CouponPolicyType getPolicyType();

    Integer getDiscountRate();

    Integer getMaxDiscountAmount();

    Integer getDiscountAmount();

    Instant getIssuedAt();

    Instant getExpiresAt();
}
