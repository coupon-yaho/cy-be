// 쿠폰 템플릿 저장 및 조회 계약을 정의합니다.
package com.kafkick.core.coupon.port;

import java.util.List;
import java.util.Optional;

import com.kafkick.core.coupon.domain.CouponTemplate;

public interface CouponTemplateRepository {

    CouponTemplate save(CouponTemplate couponTemplate);

    Optional<CouponTemplate> findById(Long id);

    List<CouponTemplate> findAllActiveByIdAsc();

    CouponTemplatePage findPageByIdAsc(int page, int size);
}
