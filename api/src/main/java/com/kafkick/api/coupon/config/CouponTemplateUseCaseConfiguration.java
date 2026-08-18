// 기술 독립적인 쿠폰 템플릿 수정 유즈케이스를 API 애플리케이션 빈으로 구성합니다.
package com.kafkick.api.coupon.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kafkick.core.coupon.port.CouponTemplateRepository;
import com.kafkick.core.coupon.service.CouponTemplateActivationService;
import com.kafkick.core.coupon.service.CouponTemplateUpdateService;

@Configuration(proxyBeanMethods = false)
public class CouponTemplateUseCaseConfiguration {

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
