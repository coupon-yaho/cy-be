// 배치 스케줄러가 기술 독립적인 쿠폰 회차 생성 유즈케이스를 사용하도록 구성합니다.
package com.kafkick.batch.coupon.config;

import java.time.ZoneId;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.core.coupon.port.CouponTemplateRepository;
import com.kafkick.core.coupon.service.CouponRoundGenerationService;

@Configuration(proxyBeanMethods = false)
public class CouponRoundUseCaseConfiguration {

    private static final ZoneId BRAND_DAY_ZONE = ZoneId.of("Asia/Seoul");

    @Bean
    public CouponRoundGenerationService couponRoundGenerationService(
            CouponTemplateRepository couponTemplateRepository,
            CouponRoundRepository couponRoundRepository
    ) {
        return new CouponRoundGenerationService(
                couponTemplateRepository,
                couponRoundRepository,
                BRAND_DAY_ZONE
        );
    }
}
