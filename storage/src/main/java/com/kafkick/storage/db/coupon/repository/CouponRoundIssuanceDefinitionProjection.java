package com.kafkick.storage.db.coupon.repository;

public interface CouponRoundIssuanceDefinitionProjection {

    Long getCouponRoundId();

    Integer getValidDays();

    String getEngineVersion();
}
