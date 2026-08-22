package com.kafkick.core.coupontemplate.service;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupontemplate.domain.CouponTemplate;
import com.kafkick.core.coupontemplate.exception.CouponTemplateErrorCode;
import com.kafkick.core.coupontemplate.port.CouponTemplateRepository;
import com.kafkick.core.coupontemplate.service.command.CouponTemplateCreateCommand;
import com.kafkick.core.support.exception.BusinessException;

@Service
public class CouponTemplateCreateService {

    private final CouponTemplateRepository couponTemplateRepository;

    public CouponTemplateCreateService(
            CouponTemplateRepository couponTemplateRepository
    ) {
        this.couponTemplateRepository = couponTemplateRepository;
    }

    @Transactional
    public CouponTemplate create(
            CouponTemplateCreateCommand command
    ) {
        CouponTemplate couponTemplate = createCouponTemplate(command);
        return couponTemplateRepository.save(couponTemplate);
    }

    private CouponTemplate createCouponTemplate(
            CouponTemplateCreateCommand command
    ) {
        try {
            return CouponTemplate.create(
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
        } catch (IllegalArgumentException exception) {
            throw new BusinessException(
                    CouponTemplateErrorCode.INVALID_COUPON_TEMPLATE,
                    exception.getMessage(),
                    exception
            );
        }
    }
}
