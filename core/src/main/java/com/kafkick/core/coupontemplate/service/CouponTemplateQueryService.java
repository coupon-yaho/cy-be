package com.kafkick.core.coupontemplate.service;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupontemplate.domain.CouponTemplate;
import com.kafkick.core.coupontemplate.exception.CouponTemplateErrorCode;
import com.kafkick.core.coupontemplate.query.CouponTemplatePage;
import com.kafkick.core.coupontemplate.port.CouponTemplateRepository;
import com.kafkick.core.support.exception.BusinessException;

@Service
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
