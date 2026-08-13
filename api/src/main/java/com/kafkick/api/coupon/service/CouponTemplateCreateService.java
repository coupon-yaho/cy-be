// 쿠폰 생성 요청을 검증하고 도메인 오류를 공통 비즈니스 예외로 변환한 뒤 저장합니다.
package com.kafkick.api.coupon.service;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.api.coupon.dto.CouponTemplateCreateRequest;
import com.kafkick.api.coupon.dto.CouponTemplateCreateResponse;
import com.kafkick.core.coupon.CouponTemplate;
import com.kafkick.core.coupon.CouponTemplateRepository;
import com.kafkick.core.coupon.exception.CouponErrorCode;
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
    public CouponTemplateCreateResponse create(
            CouponTemplateCreateRequest request
    ) {
        CouponTemplate couponTemplate = createCouponTemplate(request);
        CouponTemplate savedCouponTemplate =
                couponTemplateRepository.save(couponTemplate);

        return CouponTemplateCreateResponse.from(savedCouponTemplate);
    }

    private CouponTemplate createCouponTemplate(
            CouponTemplateCreateRequest request
    ) {
        try {
            return request.toDomain();
        } catch (IllegalArgumentException exception) {
            throw new BusinessException(
                    CouponErrorCode.INVALID_COUPON_TEMPLATE,
                    exception.getMessage(),
                    exception
            );
        }
    }
}
