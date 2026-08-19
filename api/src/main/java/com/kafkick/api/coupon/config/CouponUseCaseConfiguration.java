// 기술 독립적인 쿠폰 사용 유즈케이스를 API 애플리케이션 빈으로 구성합니다.
package com.kafkick.api.coupon.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.core.coupon.port.CouponStockRepository;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.port.IssuanceUsageRepository;
import com.kafkick.core.coupon.service.CouponUseService;
import com.kafkick.core.coupon.service.CouponCancelUseService;
import com.kafkick.core.coupon.service.CouponCancelService;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(CouponIdempotencyProperties.class)
public class CouponUseCaseConfiguration {

    @Bean
    public CouponUseService couponUseService(
            IssuanceRepository issuanceRepository,
            CouponRoundRepository couponRoundRepository,
            IssuanceUsageRepository issuanceUsageRepository,
            IssuanceHistoryRepository issuanceHistoryRepository
    ) {
        return new CouponUseService(
                issuanceRepository,
                couponRoundRepository,
                issuanceUsageRepository,
                issuanceHistoryRepository
        );
    }

    @Bean
    public CouponCancelUseService couponCancelUseService(
            IssuanceRepository issuanceRepository,
            IssuanceUsageRepository issuanceUsageRepository,
            IssuanceHistoryRepository issuanceHistoryRepository,
            CouponStockRepository couponStockRepository
    ) {
        return new CouponCancelUseService(
                issuanceRepository,
                issuanceUsageRepository,
                issuanceHistoryRepository,
                couponStockRepository
        );
    }

    @Bean
    public CouponCancelService couponCancelService(
            IssuanceRepository issuanceRepository,
            IssuanceHistoryRepository issuanceHistoryRepository,
            CouponStockRepository couponStockRepository
    ) {
        return new CouponCancelService(
                issuanceRepository,
                issuanceHistoryRepository,
                couponStockRepository
        );
    }
}
