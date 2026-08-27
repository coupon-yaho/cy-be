package com.kafkick.infra.redis.coupon.v2;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.kafkick.core.coupon.v2.port.IssuanceGatePort;

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
}
