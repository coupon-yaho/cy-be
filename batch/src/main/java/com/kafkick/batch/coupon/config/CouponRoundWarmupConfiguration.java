package com.kafkick.batch.coupon.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.batch.coupon.v2.CouponRoundWarmupRunner;
import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.core.coupon.v2.port.IssuanceWarmupPort;
import com.kafkick.core.support.TimeProvider;

/**
 * 워밍업 배선.
 *
 * <p><b>두 포트를 조건 없이 요구한다.</b> {@code @ConditionalOnBean} 은 사용자 설정에서는 평가
 * 순서가 보장되지 않아, 자동설정이 늦게 오면 <b>조용히 빠진 채</b> 기동이 성공한다 — 그러면
 * 워밍업이 없다는 사실이 회차를 올리려는 순간에야 404 로 드러난다. batch 는 Redis 를 항상 물고
 * 있으므로(build.gradle 의 data-redis, application.yml 이 게이트 자동설정을 빼지 않는다) 없으면
 * 그건 배선이 깨진 것이고, 기동에서 시끄럽게 죽는 편이 맞다.
 */
@Configuration(proxyBeanMethods = false)
public class CouponRoundWarmupConfiguration {

    @Bean
    public CouponRoundWarmupRunner couponRoundWarmupRunner(
            JdbcTemplate jdbcTemplate,
            PlatformTransactionManager transactionManager,
            IssuanceGatePort gate,
            IssuanceWarmupPort warmupPort,
            TimeProvider timeProvider) {
        return new CouponRoundWarmupRunner(
                jdbcTemplate, new TransactionTemplate(transactionManager),
                gate, warmupPort, timeProvider);
    }
}
