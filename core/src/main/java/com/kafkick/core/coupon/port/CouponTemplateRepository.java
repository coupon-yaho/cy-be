// CouponTemplate 도메인 모델의 저장 계약을 정의합니다.
package com.kafkick.core.coupon.port;

import com.kafkick.core.coupon.domain.CouponTemplate;

public interface CouponTemplateRepository {

    CouponTemplate save(CouponTemplate couponTemplate);
}
