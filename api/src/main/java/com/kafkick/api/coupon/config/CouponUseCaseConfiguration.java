package com.kafkick.api.coupon.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.core.coupon.port.CouponStockRepository;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.port.IssuanceUsageRepository;
import com.kafkick.core.coupon.port.IdempotencyRepository;
import com.kafkick.core.coupon.service.IdempotencyClaimService;
import com.kafkick.core.coupon.service.IdempotencyExecutionService;
import com.kafkick.core.coupon.service.IdempotencyPolicy;
import com.kafkick.core.coupon.service.IdempotentOperationService;
import com.kafkick.core.coupon.service.CouponOperationExecutionService;
import com.kafkick.core.coupon.service.CouponUseService;
import com.kafkick.core.coupon.service.CouponCancelUseService;
import com.kafkick.core.coupon.service.CouponCancelService;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.api.coupon.adapter.CouponCancelResponseCodec;
import com.kafkick.api.coupon.adapter.CouponCancelUseResponseCodec;
import com.kafkick.api.coupon.adapter.CouponUseResponseCodec;

@Configuration
@EnableConfigurationProperties(CouponIdempotencyProperties.class)
public class CouponUseCaseConfiguration {

    @Bean
    public IdempotencyClaimService idempotencyClaimService(
            IdempotencyRepository idempotencyRepository
    ) {
        return new IdempotencyClaimService(idempotencyRepository);
    }

    @Bean
    public IdempotencyExecutionService idempotencyExecutionService(
            IdempotencyClaimService claimService,
            TimeProvider timeProvider,
            CouponIdempotencyProperties properties
    ) {
        return new IdempotencyExecutionService(
                claimService,
                timeProvider,
                new IdempotencyPolicy(
                        properties.waitTimeout(),
                        properties.pollInterval(),
                        properties.staleAfter()
                )
        );
    }

    @Bean
    public IdempotentOperationService idempotentOperationService(
            IdempotencyRepository idempotencyRepository
    ) {
        return new IdempotentOperationService(idempotencyRepository);
    }

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

    @Bean
    public CouponOperationExecutionService couponOperationExecutionService(
            IdempotencyExecutionService idempotencyExecutionService,
            IdempotentOperationService idempotentOperationService,
            CouponUseService couponUseService,
            CouponCancelUseService couponCancelUseService,
            CouponCancelService couponCancelService,
            CouponUseResponseCodec useResponseCodec,
            CouponCancelUseResponseCodec cancelUseResponseCodec,
            CouponCancelResponseCodec cancelResponseCodec
    ) {
        return new CouponOperationExecutionService(
                idempotencyExecutionService,
                idempotentOperationService,
                couponUseService,
                couponCancelUseService,
                couponCancelService,
                useResponseCodec,
                cancelUseResponseCodec,
                cancelResponseCodec
        );
    }
}
