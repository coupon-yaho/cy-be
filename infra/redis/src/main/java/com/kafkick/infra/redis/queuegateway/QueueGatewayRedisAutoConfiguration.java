package com.kafkick.infra.redis.queuegateway;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.kafkick.core.queuegateway.QueueGatewayStatePort;

/** Redis 연결이 있는 실행 환경에 외부 게이트웨이 상태 공급 포트를 조립합니다. */
@AutoConfiguration(after = DataRedisAutoConfiguration.class)
@ConditionalOnBean(StringRedisTemplate.class)
public class QueueGatewayRedisAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(QueueGatewayStatePort.class)
    QueueGatewayStatePort queueGatewayStatePort(StringRedisTemplate redisTemplate) {
        return new RedisQueueGatewayStateAdapter(redisTemplate);
    }
}
