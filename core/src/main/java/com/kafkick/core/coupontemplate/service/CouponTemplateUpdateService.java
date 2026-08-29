package com.kafkick.core.coupontemplate.service;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupontemplate.domain.CouponTemplate;
import com.kafkick.core.coupontemplate.exception.CouponTemplateErrorCode;
import com.kafkick.core.coupontemplate.port.CouponTemplateRepository;
import com.kafkick.core.coupontemplate.service.command.CouponTemplateUpdateCommand;
import com.kafkick.core.support.exception.BusinessException;

@Service
public class CouponTemplateUpdateService {

    private final CouponTemplateRepository couponTemplateRepository;

    public CouponTemplateUpdateService(
            CouponTemplateRepository couponTemplateRepository
    ) {
        this.couponTemplateRepository = couponTemplateRepository;
    }

    @Transactional
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
