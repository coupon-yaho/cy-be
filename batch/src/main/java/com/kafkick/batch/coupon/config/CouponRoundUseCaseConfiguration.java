// 배치 스케줄러가 기술 독립적인 쿠폰 회차 생성 유즈케이스를 사용하도록 구성합니다.
package com.kafkick.batch.coupon.config;

import java.time.ZoneId;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.core.coupon.port.CouponTemplateRepository;
import com.kafkick.core.coupon.service.CouponRoundGenerationService;

@Configuration(proxyBeanMethods = false)
public class CouponRoundUseCaseConfiguration {

    @Bean
    public CouponRoundGenerationService couponRoundGenerationService(
            CouponTemplateRepository couponTemplateRepository,
            CouponRoundRepository couponRoundRepository,
            @Value("${coupon.round-generation.schedule-zone:Asia/Seoul}")
            String scheduleZone,
            @Value("${coupon.round-generation.max-days:30}")
            int maxGenerationDays
    ) {
        return new CouponRoundGenerationService(
                couponTemplateRepository,
                couponRoundRepository,
                ZoneId.of(scheduleZone),
                maxGenerationDays
        );
    }
}
