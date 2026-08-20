package com.kafkick.core.coupon.service;

import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.CouponTemplate;
import com.kafkick.core.coupon.exception.CouponTemplateErrorCode;
import com.kafkick.core.coupon.port.CouponTemplateRepository;
import com.kafkick.core.support.exception.BusinessException;

public class CouponTemplateActivationService {

    private final CouponTemplateRepository couponTemplateRepository;

    public CouponTemplateActivationService(
            CouponTemplateRepository couponTemplateRepository
    ) {
        this.couponTemplateRepository = couponTemplateRepository;
    }

    @Transactional
    public CouponTemplate changeActivation(
            Long couponTemplateId,
            CouponTemplateActivationCommand command
    ) {
        CouponTemplate couponTemplate = couponTemplateRepository
                .findById(couponTemplateId)
                .orElseThrow(() -> new BusinessException(
                        CouponTemplateErrorCode.COUPON_TEMPLATE_NOT_FOUND,
                        "couponTemplateId=" + couponTemplateId
                ));

        if (couponTemplate.active() == command.active()) {
            return couponTemplate;
        }

        return couponTemplateRepository.save(
                couponTemplate.changeActivation(command.active())
        );
    }
}
