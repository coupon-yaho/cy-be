// 기존 쿠폰 템플릿을 조회하고 검증된 값으로 전체 수정합니다.
package com.kafkick.core.coupon.service;

import com.kafkick.core.coupon.domain.CouponTemplate;
import com.kafkick.core.coupon.exception.CouponTemplateErrorCode;
import com.kafkick.core.coupon.port.CouponTemplateRepository;
import com.kafkick.core.support.exception.BusinessException;

public class CouponTemplateUpdateService {

    private final CouponTemplateRepository couponTemplateRepository;

    public CouponTemplateUpdateService(
            CouponTemplateRepository couponTemplateRepository
    ) {
        this.couponTemplateRepository = couponTemplateRepository;
    }

    public CouponTemplate update(
            Long couponTemplateId,
            CouponTemplateUpdateCommand command
    ) {
        CouponTemplate couponTemplate = couponTemplateRepository
                .findById(couponTemplateId)
                .orElseThrow(() -> new BusinessException(
                        CouponTemplateErrorCode.COUPON_TEMPLATE_NOT_FOUND,
                        "couponTemplateId=" + couponTemplateId
                ));

        try {
            CouponTemplate updatedCouponTemplate = couponTemplate.update(
                    command.brandId(),
                    command.name(),
                    command.policyType(),
                    command.discountRate(),
                    command.maxDiscountAmount(),
                    command.discountAmount(),
                    command.validDays(),
                    command.nthWeek(),
                    command.dayOfWeek(),
                    command.startTime(),
                    command.durationHours(),
                    command.stockPerOccurrence(),
                    command.eligibleGrades()
            );

            return couponTemplateRepository.save(updatedCouponTemplate);
        } catch (IllegalArgumentException exception) {
            throw new BusinessException(
                    CouponTemplateErrorCode.INVALID_COUPON_TEMPLATE,
                    exception.getMessage(),
                    exception
            );
        }
    }
}
