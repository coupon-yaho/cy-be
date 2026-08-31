package com.kafkick.infra.redis;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.context.annotation.ImportCandidates;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class RedisSentinelConfigurationGuardAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(RedisSentinelConfigurationGuardAutoConfiguration.class));

    @Test
    void isRegisteredAsAutoConfiguration() {
        assertThat(ImportCandidates.load(AutoConfiguration.class, getClass().getClassLoader()))
                .contains(RedisSentinelConfigurationGuardAutoConfiguration.class.getName());
    }

    @Test
    void refusesSentinelProfileWithoutTheImportedSentinelSettings() {
        runner.withPropertyValues("spring.profiles.active=redis-sentinel")
                .run(context -> assertThat(context.getStartupFailure())
                        .hasMessageContaining("spring.data.redis.sentinel.master"));
    }

    /**
     * 환경변수가 아예 없어 빈 문자열로 해석된 경우도 가드가 잡는다. {@code redis.yml} 이
     * {@code ${REDIS_SENTINEL_MASTER:}} 로 기본값을 비워 두는 이유가 이것이다 — 기본값이
     * 없으면 프로퍼티 해석이 먼저 터져 "Could not resolve placeholder" 스택이 나가고,
     * 무엇이 필요한지 말해 주는 이 메시지는 영영 안 나온다.
     */
    @Test
    void refusesSentinelProfileWhenTheEnvironmentResolvedToEmptyStrings() {
        runner.withPropertyValues(
                        "spring.profiles.active=redis-sentinel",
                        "spring.data.redis.sentinel.master=",
                        "spring.data.redis.sentinel.nodes=")
                .run(context -> assertThat(context.getStartupFailure())
                        .hasMessageContaining("spring.data.redis.sentinel.master"));
    }

    @Test
    void allowsSentinelProfileWhenMasterAndNodesAreBothPresent() {
        runner.withPropertyValues(
                        "spring.profiles.active=redis-sentinel",
                        "spring.data.redis.sentinel.master=coupon-master",
                        "spring.data.redis.sentinel.nodes=s1:26379,s2:26379,s3:26379")
                .run(context -> assertThat(context).hasNotFailed());
    }
}
