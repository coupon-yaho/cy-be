package com.kafkick.api;

import static com.kafkick.testsupport.CommittedConfigStager.stage;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;

/**
 * {@code include} 를 {@code *} 로 넓혀도 {@code exclude} 가 막는지 확인한다.
 *
 * <p>커밋되는 템플릿을 실제 설정으로 로드해 YAML 계약과 HTTP 동작을 함께 검증한다.
 */
@SpringBootTest(
        classes = ManagementExposureTest.TestApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.config.location=file:build/management-exposure/management.yml",
                "management.server.port=0",
                "management.endpoints.web.exposure.include=*",
                // 이 앱에는 관측 풀이 없다. obs 그룹 검증까지 켜면 노출 규칙과 무관한 이유로 깨진다.
                "management.endpoint.health.validate-group-membership=false"
        })
class ManagementExposureTest {

    private static final Path STAGED_CONFIG = Path.of("build/management-exposure/management.yml");

    @BeforeAll
    static void stageManagementConfig() throws Exception {
        stage(STAGED_CONFIG, "management.yml.example");
    }

    @LocalManagementPort
    int managementPort;

    private int statusOf(String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(
                URI.create("http://localhost:" + managementPort + path)).build();
        return HttpClient.newHttpClient()
                .send(request, HttpResponse.BodyHandlers.ofString()).statusCode();
    }

    @Test
    @DisplayName("include 를 * 로 넓혀도 env 는 열리지 않는다 — exclude 가 회귀를 막는다")
    void envStaysClosedEvenWhenIncludeIsWidened() throws Exception {
        assertThat(statusOf("/actuator/env")).isEqualTo(404);
    }

    @Test
    @DisplayName("configprops · beans · heapdump 도 마찬가지")
    void otherSensitiveEndpointsStayClosed() throws Exception {
        assertThat(statusOf("/actuator/configprops")).isEqualTo(404);
        assertThat(statusOf("/actuator/beans")).isEqualTo(404);
        assertThat(statusOf("/actuator/heapdump")).isEqualTo(404);
    }

    @Test
    @DisplayName("health 는 열려 있다 — 로드밸런서가 찔러야 한다")
    void healthStaysOpen() throws Exception {
        assertThat(statusOf("/actuator/health")).isEqualTo(200);
    }

    @Test
    @DisplayName("prometheus scrape 엔드포인트가 실제 관리 포트에서 열린다")
    void prometheusEndpointIsExposed() throws Exception {
        assertThat(statusOf("/actuator/prometheus")).isEqualTo(200);
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration(exclude = {
            DataSourceAutoConfiguration.class,
            HibernateJpaAutoConfiguration.class
    })
    static class TestApp {
    }
}
