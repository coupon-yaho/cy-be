package com.kafkick.batch.coupon.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.kafkick.batch.coupon.expiration.CouponExpirationProperties;
import com.kafkick.core.coupon.port.CouponStockRepository;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.service.CouponExpirationService;

@Configuration
@EnableScheduling
@EnableConfigurationProperties(CouponExpirationProperties.class)
public class CouponExpirationConfiguration {

    @Bean
    public CouponExpirationService couponExpirationService(
            IssuanceRepository issuanceRepository,
            IssuanceHistoryRepository issuanceHistoryRepository,
            CouponStockRepository couponStockRepository
    ) {
        return new CouponExpirationService(
                issuanceRepository,
                issuanceHistoryRepository,
                couponStockRepository
        );
    }
}
