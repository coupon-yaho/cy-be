// 쿠폰 템플릿 수정 유즈케이스의 트랜잭션 경계를 API 어댑터에서 관리합니다.
package com.kafkick.api.coupon.adapter;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.CouponTemplate;
import com.kafkick.core.coupon.service.CouponTemplateUpdateCommand;
import com.kafkick.core.coupon.service.CouponTemplateUpdateService;

@Component
public class CouponTemplateUpdateTransactionalAdapter {

    private final CouponTemplateUpdateService couponTemplateUpdateService;

    public CouponTemplateUpdateTransactionalAdapter(
            CouponTemplateUpdateService couponTemplateUpdateService
    ) {
        this.couponTemplateUpdateService = couponTemplateUpdateService;
    }

    @Transactional
    public CouponTemplate update(
            Long couponTemplateId,
            CouponTemplateUpdateCommand command
    ) {
        return couponTemplateUpdateService.update(couponTemplateId, command);
    }
}
