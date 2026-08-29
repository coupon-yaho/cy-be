package com.kafkick.infra.redis.runtimeconfig;

import com.kafkick.core.runtimeconfig.RuntimeConfigStore;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.beans.factory.ObjectProvider;

import java.time.Clock;

@AutoConfiguration(after = DataRedisAutoConfiguration.class)
@EnableConfigurationProperties(RuntimeConfigBootstrapProperties.class)
public class RuntimeConfigRedisAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(RuntimeConfigJsonMapper.class)
    RuntimeConfigJsonMapper runtimeConfigJsonMapper() {
        return new RuntimeConfigJsonMapper();
    }

    @Bean
    @ConditionalOnMissingBean(RuntimeConfigStore.class)
    RedisRuntimeConfigStore runtimeConfigStore(
            StringRedisTemplate redisTemplate,
            RuntimeConfigJsonMapper jsonMapper,
            ObjectProvider<Clock> clock
    ) {
        return new RedisRuntimeConfigStore(
                redisTemplate, jsonMapper.objectMapper(), clock.getIfAvailable(Clock::systemUTC));
    }

    @Bean
    @ConditionalOnBean(RedisRuntimeConfigStore.class)
    @ConditionalOnMissingBean(RuntimeConfigBootstrap.class)
    RuntimeConfigBootstrap runtimeConfigBootstrap(
            StringRedisTemplate redisTemplate,
            RuntimeConfigJsonMapper jsonMapper,
            ObjectProvider<Clock> clock,
            RuntimeConfigBootstrapProperties properties
    ) {
        return new RuntimeConfigBootstrap(
                redisTemplate, jsonMapper.objectMapper(), clock.getIfAvailable(Clock::systemUTC), properties);
    }
}
