package com.kafkick.infra.redis.runtimeconfig;

import com.kafkick.core.runtimeconfig.RuntimeConfigStore;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.beans.factory.ObjectProvider;

import java.time.Clock;

@AutoConfiguration(after = DataRedisAutoConfiguration.class)
public class RuntimeConfigRedisAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(RuntimeConfigJsonMapper.class)
    RuntimeConfigJsonMapper runtimeConfigJsonMapper() {
        return new RuntimeConfigJsonMapper();
    }

    @Bean
    @ConditionalOnMissingBean(RuntimeConfigStore.class)
    RuntimeConfigStore runtimeConfigStore(
            StringRedisTemplate redisTemplate,
            RuntimeConfigJsonMapper jsonMapper,
            ObjectProvider<Clock> clock
    ) {
        return new RedisRuntimeConfigStore(
                redisTemplate, jsonMapper.objectMapper(), clock.getIfAvailable(Clock::systemUTC));
    }
}
