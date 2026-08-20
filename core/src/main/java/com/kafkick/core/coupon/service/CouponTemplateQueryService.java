package com.kafkick.core.coupon.service;

import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.CouponTemplate;
import com.kafkick.core.coupon.exception.CouponTemplateErrorCode;
import com.kafkick.core.coupon.query.CouponTemplatePage;
import com.kafkick.core.coupon.port.CouponTemplateRepository;
import com.kafkick.core.support.exception.BusinessException;

public class CouponTemplateQueryService {

    private final CouponTemplateRepository couponTemplateRepository;

    public CouponTemplateQueryService(
            CouponTemplateRepository couponTemplateRepository
    ) {
        this.couponTemplateRepository = couponTemplateRepository;
    }

    @Transactional(readOnly = true)
    public CouponTemplate findById(Long couponTemplateId) {
        return couponTemplateRepository.findById(couponTemplateId)
                .orElseThrow(() -> new BusinessException(
                        CouponTemplateErrorCode.COUPON_TEMPLATE_NOT_FOUND,
                        "couponTemplateId=" + couponTemplateId
                ));
    }

    @Transactional(readOnly = true)
    public CouponTemplatePage findPage(int page, int size) {
        return couponTemplateRepository.findPageByIdAsc(page, size);
    }
}
