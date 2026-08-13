// 쿠폰 생성 요청을 도메인 모델로 변환하고 Repository를 통해 저장합니다.
package com.kafkick.api.coupon.service;

import com.kafkick.api.coupon.dto.CouponTemplateCreateRequest;
import com.kafkick.api.coupon.dto.CouponTemplateCreateResponse;
import com.kafkick.core.coupon.CouponTemplate;
import com.kafkick.core.coupon.CouponTemplateRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class CouponTemplateCreateService {

    private final CouponTemplateRepository couponTemplateRepository;

    public CouponTemplateCreateService(CouponTemplateRepository couponTemplateRepository) {
        this.couponTemplateRepository = couponTemplateRepository;
    }

    @Transactional
    public CouponTemplateCreateResponse create(CouponTemplateCreateRequest request) {
        CouponTemplate couponTemplate = request.toDomain();
        CouponTemplate savedCouponTemplate = couponTemplateRepository.save(couponTemplate);

        return CouponTemplateCreateResponse.from(savedCouponTemplate);
    }
}
