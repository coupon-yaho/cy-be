package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;

public interface CouponDefinitionProjection {

    Long getCouponRoundId();

    Long getBrandId();

    String getName();

    String getPolicyType();

    Integer getDiscountRate();

    Integer getMaxDiscountAmount();

    Integer getDiscountAmount();

    int getValidDays();

    Instant getOpenAt();

    Instant getCloseAt();

    String getStatus();
}
