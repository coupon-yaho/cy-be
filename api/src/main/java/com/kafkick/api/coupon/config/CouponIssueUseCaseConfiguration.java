// 기술 독립적인 쿠폰 발급 유즈케이스를 API 애플리케이션 빈으로 구성합니다.
package com.kafkick.api.coupon.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kafkick.core.coupon.port.CouponCodeGenerator;
import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.core.coupon.port.CouponStockRepository;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.service.CouponIssueService;

@Configuration(proxyBeanMethods = false)
public class CouponIssueUseCaseConfiguration {

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
