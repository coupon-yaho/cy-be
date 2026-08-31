package com.kafkick.api.observation;

import java.io.IOException;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.yaml.snakeyaml.Yaml;

import static org.assertj.core.api.Assertions.assertThat;

class CouponRoundLifecycleRedisYamlContractTest {

    @Test
    @DisplayName("API 관측 설정만 쿠폰 회차 종료 Redis 구독을 켠다")
    void apiEnablesCouponRoundLifecycleRedisSubscriber() throws IOException {
        Map<String, Object> config;
        try (var input = new ClassPathResource(
                "observation.yml.example"
        ).getInputStream()) {
            config = new Yaml().load(input);
        }

        assertThat(path(config, "coupon-round", "lifecycle", "redis",
                "subscriber-enabled")).isEqualTo(
                        "${COUPON_ROUND_LIFECYCLE_REDIS_SUBSCRIBER_ENABLED:true}"
                );
    }

    @SuppressWarnings("unchecked")
    private static Object path(Map<String, Object> root, String... keys) {
        Object value = root;
        for (String key : keys) {
            value = ((Map<String, Object>) value).get(key);
        }
        return value;
    }
}
