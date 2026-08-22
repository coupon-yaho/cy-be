package com.kafkick;

import static com.kafkick.testsupport.CommittedConfigStager.stage;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.context.annotation.Configuration;

/**
 * Redis 스타터를 올린 것만으로 헬스 기여자가 붙어 V1 batch 의 헬스가 DOWN 으로 굳는지 본다.
 * 도메인 Gauge 쪽 {@code ObjectProvider} 가드는 수집만 막지 헬스 기여자는 못 막는다.
 */
class BatchRedisHealthTest {

    private static final Path STAGED_CONFIG = Path.of("build/batch-redis-health/management.yml");

    @BeforeAll
    static void stageManagementConfig() throws Exception {
        stage(STAGED_CONFIG, "management.yml.example");
    }

    private static int healthStatus(int port) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(
                URI.create("http://localhost:" + port + "/actuator/health")).build();
        return HttpClient.newHttpClient()
                .send(request, HttpResponse.BodyHandlers.discarding())
                .statusCode();
    }

    @Nested
    @SpringBootTest(
            classes = TestApp.class,
            webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
            properties = {
                    "spring.config.location=file:build/batch-redis-health/management.yml",
                    "management.server.port=0",
                    "management.endpoint.health.validate-group-membership=false",
                    // ⚠️ 이 줄이 없으면 개발 기계에 떠 있는 다른 Redis(6379)에 붙어 헬스가 UP 이 되고,
                    //    스위치를 지워도 이 테스트가 통과한다 — 실제로 그렇게 무효였다.
                    "spring.data.redis.port=1",
                    "spring.data.redis.timeout=200ms"
            })
    class WithCommittedConfig {

        @LocalManagementPort
        int managementPort;

        @Test
        @DisplayName("Redis 에 닿지 못해도 batch 헬스는 UP 이다 — 기여자를 껐기 때문이다")
        void healthIsUpWithoutRedis() throws Exception {
            assertThat(healthStatus(managementPort)).isEqualTo(200);
        }
    }

    @Nested
    @SpringBootTest(
            classes = TestApp.class,
            webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
            properties = {
                    "spring.config.location=file:build/batch-redis-health/management.yml",
                    "management.server.port=0",
                    "management.endpoint.health.validate-group-membership=false",
                    // 커밋된 설정의 스위치를 되돌리고, 아무도 듣지 않는 포트를 가리킨다.
                    "management.health.redis.enabled=true",
                    "spring.data.redis.port=1",
                    "spring.data.redis.timeout=200ms"
            })
    class WithRedisHealthEnabled {

        @LocalManagementPort
        int managementPort;

        @Test
        @DisplayName("스위치를 되돌리면 곧바로 DOWN 이 된다 — 그 줄이 방어의 실체다")
        void healthGoesDownWhenTheSwitchIsReverted() throws Exception {
            assertThat(healthStatus(managementPort)).isEqualTo(503);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration(exclude = {
            DataSourceAutoConfiguration.class,
            HibernateJpaAutoConfiguration.class
    })
    static class TestApp {
    }
}
