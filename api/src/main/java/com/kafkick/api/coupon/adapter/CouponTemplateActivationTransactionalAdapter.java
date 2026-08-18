// 쿠폰 템플릿 활성화 상태 변경의 트랜잭션 경계를 API 어댑터에서 관리합니다.
package com.kafkick.api.coupon.adapter;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.CouponTemplate;
import com.kafkick.core.coupon.service.CouponTemplateActivationCommand;
import com.kafkick.core.coupon.service.CouponTemplateActivationService;

@Component
public class CouponTemplateActivationTransactionalAdapter {

    private final CouponTemplateActivationService
            couponTemplateActivationService;

    public CouponTemplateActivationTransactionalAdapter(
            CouponTemplateActivationService couponTemplateActivationService
    ) {
        this.couponTemplateActivationService =
                couponTemplateActivationService;
    }

    @Transactional
    public CouponTemplate changeActivation(
            Long couponTemplateId,
            CouponTemplateActivationCommand command
    ) {
        return couponTemplateActivationService.changeActivation(
                couponTemplateId,
                command
        );
    }
}
