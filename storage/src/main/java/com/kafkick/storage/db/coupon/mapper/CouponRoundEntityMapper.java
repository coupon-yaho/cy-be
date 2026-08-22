package com.kafkick.storage.db.coupon.mapper;

import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.storage.db.coupon.entity.CouponRoundEntity;

public final class CouponRoundEntityMapper {

    private CouponRoundEntityMapper() {
    }

    public static CouponRoundEntity toEntity(CouponRound couponRound) {
        return new CouponRoundEntity(
                couponRound.id(),
                couponRound.templateId(),
                couponRound.brandId(),
                couponRound.name(),
                couponRound.policyType(),
                couponRound.discountRate(),
                couponRound.maxDiscountAmount(),
                couponRound.discountAmount(),
                couponRound.validDays(),
                (byte) couponRound.eligibleGradesMask(),
                couponRound.openAt(),
                couponRound.closeAt(),
                couponRound.status(),
                couponRound.generatedAt()
        );
    }

    public static CouponRound toDomain(CouponRoundEntity entity) {
        return CouponRound.restore(
                entity.getId(),
                entity.getTemplateId(),
                entity.getBrandId(),
                entity.getName(),
                entity.getPolicyType(),
                entity.getDiscountRate(),
                entity.getMaxDiscountAmount(),
                entity.getDiscountAmount(),
                entity.getValidDays(),
                MembershipGrade.fromMask(entity.getEligibleGradesMask()),
                entity.getOpenAt(),
                entity.getCloseAt(),
                entity.getStatus(),
                entity.getGeneratedAt()
        );
    }
}
