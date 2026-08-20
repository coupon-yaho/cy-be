package com.kafkick.batch.coupon.config;

import java.time.ZoneId;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.core.coupon.port.CouponTemplateRepository;
import com.kafkick.core.coupon.service.CouponRoundGenerationService;
import com.kafkick.core.coupon.service.CouponRoundCreationService;

@Configuration
public class CouponRoundUseCaseConfiguration {

    @Bean
    public CouponRoundCreationService couponRoundCreationService(
            CouponRoundRepository couponRoundRepository
    ) {
        return new CouponRoundCreationService(couponRoundRepository);
    }

    @Bean
    public CouponRoundGenerationService couponRoundGenerationService(
            CouponTemplateRepository couponTemplateRepository,
            CouponRoundCreationService couponRoundCreationService,
            @Value("${coupon.round-generation.schedule-zone:Asia/Seoul}")
            String scheduleZone,
            @Value("${coupon.round-generation.max-days:30}")
            int maxGenerationDays
    ) {
        return new CouponRoundGenerationService(
                couponTemplateRepository,
                couponRoundCreationService,
                ZoneId.of(scheduleZone),
                maxGenerationDays
        );
    }
}
