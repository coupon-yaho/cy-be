package com.kafkick.core.coupon.port;

import java.util.List;
import java.util.Optional;

import com.kafkick.core.coupon.domain.CouponTemplate;
import com.kafkick.core.coupon.query.CouponTemplatePage;

public interface CouponTemplateRepository {

    CouponTemplate save(CouponTemplate couponTemplate);

    Optional<CouponTemplate> findById(Long id);

    List<CouponTemplate> findAllActiveByIdAsc();

    CouponTemplatePage findPageByIdAsc(int page, int size);
}
