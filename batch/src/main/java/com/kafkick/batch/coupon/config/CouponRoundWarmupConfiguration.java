package com.kafkick.batch.coupon.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.batch.coupon.v2.CouponRoundGateJdbc;
import com.kafkick.batch.coupon.v2.CouponRoundRebuildProperties;
import com.kafkick.batch.coupon.v2.CouponRoundRebuildRunner;
import com.kafkick.batch.coupon.v2.CouponRoundWarmupRunner;
import com.kafkick.batch.coupon.v2.RoundGateWriteGuard;
import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.core.coupon.v2.port.IssuanceWarmupPort;
import com.kafkick.core.support.TimeProvider;

/**
 * 게이트를 여는 두 경로의 배선 — 워밍업(처음 여는 회차)과 재구성(이미 열린 회차).
 *
 * <p><b>두 포트를 조건 없이 요구한다.</b> {@code @ConditionalOnBean} 은 사용자 설정에서는 평가
 * 순서가 보장되지 않아, 자동설정이 늦게 오면 <b>조용히 빠진 채</b> 기동이 성공한다 — 그러면
 * 워밍업이 없다는 사실이 회차를 올리려는 순간에야 404 로 드러난다. batch 는 Redis 를 항상 물고
 * 있으므로(build.gradle 의 data-redis, application.yml 이 게이트 자동설정을 빼지 않는다) 없으면
 * 그건 배선이 깨진 것이고, 기동에서 시끄럽게 죽는 편이 맞다.
 *
 * <p><b>{@link RoundGateWriteGuard} 는 빈 하나다.</b> 두 러너가 같은 인스턴스를 받아야 서로의
 * 겹침까지 막는다 — 각자 만들면 워밍업이 도는 회차를 재구성이 통과해 들어와, 늦은 쪽이 먼저
 * 열린 게이트 뒤에서 {@code issued} 를 지운다(07 의 그림 그대로다).
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(CouponRoundRebuildProperties.class)
public class CouponRoundWarmupConfiguration {

    @Bean
    public CouponRoundGateJdbc couponRoundGateJdbc(
            JdbcTemplate jdbcTemplate, PlatformTransactionManager transactionManager) {
        return new CouponRoundGateJdbc(jdbcTemplate, new TransactionTemplate(transactionManager));
    }

    @Bean
    public RoundGateWriteGuard roundGateWriteGuard() {
        return new RoundGateWriteGuard();
    }

    @Bean
    public CouponRoundWarmupRunner couponRoundWarmupRunner(
            CouponRoundGateJdbc roundJdbc,
            RoundGateWriteGuard guard,
            IssuanceGatePort gate,
            IssuanceWarmupPort warmupPort,
            TimeProvider timeProvider) {
        return new CouponRoundWarmupRunner(roundJdbc, guard, gate, warmupPort, timeProvider);
    }

    @Bean
    public CouponRoundRebuildRunner couponRoundRebuildRunner(
            CouponRoundGateJdbc roundJdbc,
            RoundGateWriteGuard guard,
            IssuanceGatePort gate,
            IssuanceWarmupPort warmupPort,
            TimeProvider timeProvider,
            CouponRoundRebuildProperties properties) {
        return new CouponRoundRebuildRunner(
                roundJdbc, guard, gate, warmupPort, timeProvider, properties.drain());
    }
}
