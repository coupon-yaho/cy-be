package com.kafkick.infra.redis;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.util.StringUtils;

/** Sentinel 프로파일인데 설정 파일을 복사하지 않은 배포를 직결 Redis로 조용히 흘리지 않는다. */
@AutoConfiguration
public class RedisSentinelConfigurationGuardAutoConfiguration {

    @Bean
    InitializingBean redisSentinelConfigurationGuard(Environment environment) {
        return () -> {
            if (!environment.matchesProfiles("redis-sentinel")) return;
            String master = environment.getProperty("spring.data.redis.sentinel.master");
            String nodes = environment.getProperty("spring.data.redis.sentinel.nodes");
            if (!StringUtils.hasText(master) || !StringUtils.hasText(nodes)) {
                throw new IllegalStateException(
                        "redis-sentinel 프로파일에는 spring.data.redis.sentinel.master와 nodes가 필요합니다.");
            }
        };
    }
}
