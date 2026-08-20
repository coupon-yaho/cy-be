package com.kafkick.api.coupon.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kafkick.core.coupon.port.CouponTemplateRepository;
import com.kafkick.core.coupon.service.CouponTemplateActivationService;
import com.kafkick.core.coupon.service.CouponTemplateCreateService;
import com.kafkick.core.coupon.service.CouponTemplateQueryService;
import com.kafkick.core.coupon.service.CouponTemplateUpdateService;

@Configuration
public class CouponTemplateUseCaseConfiguration {

    @Bean
    public CouponTemplateCreateService couponTemplateCreateService(
            CouponTemplateRepository couponTemplateRepository
    ) {
        return new CouponTemplateCreateService(couponTemplateRepository);
    }

    @Bean
    public CouponTemplateQueryService couponTemplateQueryService(
            CouponTemplateRepository couponTemplateRepository
    ) {
        return new CouponTemplateQueryService(couponTemplateRepository);
    }

    @Bean
    public CouponTemplateUpdateService couponTemplateUpdateService(
            CouponTemplateRepository couponTemplateRepository
    ) {
        return new CouponTemplateUpdateService(couponTemplateRepository);
    }

    @Bean
    public CouponTemplateActivationService couponTemplateActivationService(
            CouponTemplateRepository couponTemplateRepository
    ) {
        return new CouponTemplateActivationService(couponTemplateRepository);
    }
}
