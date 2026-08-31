package com.kafkick.infra.redis;

import io.lettuce.core.ReadFrom;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.boot.data.redis.autoconfigure.LettuceClientConfigurationBuilderCustomizer;
import org.springframework.context.annotation.Bean;

/**
 * 공유 Redis 연결의 읽기를 master로 고정한다.
 *
 * <p>Sentinel failover 뒤 replica의 낡은 재고를 읽으면 발급 한도보다 큰 값을 보게 된다.
 * 조회 L2도 같은 connection factory를 공유하므로, 별도 연결을 만들기 전까지 전부 MASTER다.
 */
@AutoConfiguration(before = DataRedisAutoConfiguration.class)
@ConditionalOnClass(LettuceClientConfigurationBuilderCustomizer.class)
@ConditionalOnProperty(prefix = "spring.data.redis.sentinel", name = "master")
public class RedisMasterOnlyReadAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(name = "redisMasterOnlyReadCustomizer")
    LettuceClientConfigurationBuilderCustomizer redisMasterOnlyReadCustomizer() {
        return builder -> builder.readFrom(ReadFrom.MASTER);
    }
}
