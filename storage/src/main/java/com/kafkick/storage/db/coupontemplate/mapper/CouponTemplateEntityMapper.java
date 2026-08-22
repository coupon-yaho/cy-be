package com.kafkick.storage.db.coupontemplate.mapper;

import com.kafkick.core.coupontemplate.domain.CouponTemplate;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.storage.db.coupontemplate.entity.CouponTemplateEntity;

public final class CouponTemplateEntityMapper {

    private CouponTemplateEntityMapper() {
    }

    public static CouponTemplateEntity toEntity(CouponTemplate couponTemplate) {
        return new CouponTemplateEntity(
                couponTemplate.id(),
                couponTemplate.brandId(),
                couponTemplate.name(),
                couponTemplate.policyType(),
                couponTemplate.discountRate(),
                couponTemplate.maxDiscountAmount(),
                couponTemplate.discountAmount(),
                couponTemplate.validDays(),
                (byte) couponTemplate.nthWeek(),
                couponTemplate.dayOfWeek(),
                couponTemplate.startTime(),
                couponTemplate.durationHours(),
                couponTemplate.stockPerOccurrence(),
                (byte) couponTemplate.eligibleGradesMask(),
                couponTemplate.active()
        );
    }

    public static CouponTemplate toDomain(CouponTemplateEntity entity) {
        return CouponTemplate.restore(
                entity.getId(),
                entity.getBrandId(),
                entity.getName(),
                entity.getPolicyType(),
                entity.getDiscountRate(),
                entity.getMaxDiscountAmount(),
                entity.getDiscountAmount(),
                entity.getValidDays(),
                entity.getNthWeek().intValue(),
                entity.getDayOfWeek(),
                entity.getStartTime(),
                entity.getDurationHours(),
                entity.getStockPerOccurrence(),
                MembershipGrade.fromMask(
                        entity.getEligibleGradesMask().intValue()
                ),
                entity.isActive()
        );
    }
}
