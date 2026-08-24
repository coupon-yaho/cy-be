package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;

public interface IssuableCouponRoundProjection {

    Long getCouponRoundId();

    Long getBrandId();

    String getName();

    String getPolicyType();

    Integer getDiscountRate();

    Integer getMaxDiscountAmount();

    Integer getDiscountAmount();

    Integer getValidDays();

    Instant getOpenAt();

    Instant getCloseAt();

    Integer getRemainingQuantity();
}
