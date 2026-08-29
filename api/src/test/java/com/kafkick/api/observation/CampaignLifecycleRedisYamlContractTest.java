package com.kafkick.api.observation;

import java.io.IOException;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.yaml.snakeyaml.Yaml;

import static org.assertj.core.api.Assertions.assertThat;

class CampaignLifecycleRedisYamlContractTest {

    @Test
    @DisplayName("API 관측 설정만 캠페인 종료 Redis 구독을 켠다")
    void apiEnablesCampaignLifecycleRedisSubscriber() throws IOException {
        Map<String, Object> config;
        try (var input = new ClassPathResource(
                "observation.yml.example"
        ).getInputStream()) {
            config = new Yaml().load(input);
        }

        assertThat(path(config, "campaign", "lifecycle", "redis",
                "subscriber-enabled")).isEqualTo(
                        "${CAMPAIGN_LIFECYCLE_REDIS_SUBSCRIBER_ENABLED:true}"
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
