package com.kafkick.api.coupon.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kafkick.api.coupon.adapter.SecureRandomCouponCodeGenerator;
import com.kafkick.core.coupon.port.CouponCodeGenerator;
import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.core.coupon.port.CouponStockRepository;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.service.CouponIssueService;

@Configuration
public class CouponIssueUseCaseConfiguration {

    @Bean
    public CouponCodeGenerator couponCodeGenerator() {
        return new SecureRandomCouponCodeGenerator();
    }

    @Bean
    public CouponIssueService couponIssueService(
            CouponRoundRepository couponRoundRepository,
            IssuanceRepository issuanceRepository,
            CouponStockRepository couponStockRepository,
            IssuanceHistoryRepository issuanceHistoryRepository,
            CouponCodeGenerator couponCodeGenerator
    ) {
        return new CouponIssueService(
                couponRoundRepository,
                issuanceRepository,
                couponStockRepository,
                issuanceHistoryRepository,
                couponCodeGenerator
        );
    }
}
