package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;

/** 발급 사전검증 한 쿼리가 돌려주는 회차 정책과 기존 발급 여부입니다. */
public interface CouponIssuePolicyProjection {

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

    Instant getGeneratedAt();

    /** MySQL 의 {@code EXISTS} 는 0/1 정수로 온다. Boolean 으로 선언하면 변환기가 없어 실패한다. */
    Long getAlreadyIssued();
}
