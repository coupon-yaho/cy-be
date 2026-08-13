// CouponTemplate 도메인 모델의 저장 계약을 정의합니다.
package com.kafkick.core.coupon;

public interface CouponTemplateRepository {

    CouponTemplate save(CouponTemplate couponTemplate);
}