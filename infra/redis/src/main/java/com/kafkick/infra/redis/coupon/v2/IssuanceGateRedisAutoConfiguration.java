package com.kafkick.infra.redis.coupon.v2;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.core.coupon.v2.port.IssuanceWarmupPort;
import com.kafkick.core.admin.stock.V2AdminStockReader;

/**
 * v2 게이트 조립. 스크립트 5종은 {@link IssuanceScripts} 의 상수라 빈이 아니고,
 * 빈이 되는 것은 <b>그것들을 부르는 통로</b>뿐이다.
 *
 * <p>{@code StringRedisTemplate} 이 없는 배포(V1 전용)에서는 게이트도 없다 —
 * 없는 통로를 빈으로 세워 두면 첫 발급 요청에서야 그 사실이 드러난다.
 */
@AutoConfiguration(after = DataRedisAutoConfiguration.class)
@ConditionalOnBean(StringRedisTemplate.class)
public class IssuanceGateRedisAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(IssuanceScriptRunner.class)
    IssuanceScriptRunner issuanceScriptRunner(StringRedisTemplate redisTemplate) {
        return new IssuanceScriptRunner(redisTemplate);
    }

    @Bean
    @ConditionalOnMissingBean(IssuanceGatePort.class)
    IssuanceGatePort issuanceGatePort(
            IssuanceScriptRunner scriptRunner, StringRedisTemplate redisTemplate) {
        return new RedisIssuanceGate(scriptRunner, redisTemplate);
    }

    /** 발급 게이트와 같은 Redis 연결에서 관리자용 V2 재고 정본 조회 포트를 제공합니다. */
    @Bean
    @ConditionalOnMissingBean(V2AdminStockReader.class)
    V2AdminStockReader v2AdminStockReader(StringRedisTemplate redisTemplate) {
        return new RedisV2AdminStockReader(redisTemplate);
    }

    /**
     * 워밍업 시딩. 게이트와 <b>다른 빈</b>인 이유는 수명이 달라서다 — 이쪽은 회차당 한 번,
     * 게이트가 닫힌 창에서만 돈다(설계 §6.2). 한 빈으로 묶으면 발급 유스케이스가 재고를
     * 통째로 쓰는 연산을 주입받게 된다.
     */
    @Bean
    @ConditionalOnMissingBean(IssuanceWarmupPort.class)
    IssuanceWarmupPort issuanceWarmupPort(StringRedisTemplate redisTemplate) {
        return new RedisIssuanceWarmup(redisTemplate);
    }
}
