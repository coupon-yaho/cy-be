package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;

public interface CouponRoundDetailProjection {

    Long getCouponRoundId();

    Long getTemplateId();

    Long getBrandId();

    String getName();

    String getPolicyType();

    Integer getDiscountRate();

    Integer getMaxDiscountAmount();

    Integer getDiscountAmount();

    Integer getValidDays();

    Integer getEligibleGradesMask();

    Instant getOpenAt();

    Instant getCloseAt();

    String getStatus();

    Integer getTotalQuantity();

    Integer getRemainingQuantity();
}
