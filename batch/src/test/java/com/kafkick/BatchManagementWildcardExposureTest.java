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
import org.junit.jupiter.api.Test;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.context.annotation.Configuration;

/** {@code include=*}로 잘못 확장해도 템플릿의 exclude가 민감 엔드포인트를 막는지 검증한다. */
@SpringBootTest(
        classes = BatchManagementWildcardExposureTest.TestApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.config.location=file:build/batch-management-wildcard/management.yml",
                "management.server.port=0",
                "management.endpoints.web.exposure.include=*",
                // 이 앱에는 관측 풀이 없다. obs 그룹 검증까지 켜면 노출 규칙과 무관한 이유로 깨진다.
                "management.endpoint.health.validate-group-membership=false"
        })
class BatchManagementWildcardExposureTest {

    private static final Path STAGED_CONFIG =
            Path.of("build/batch-management-wildcard/management.yml");

    @LocalManagementPort
    int managementPort;

    @BeforeAll
    static void stageManagementConfig() throws Exception {
        stage(STAGED_CONFIG, "management.yml.example");
    }

    @Test
    @DisplayName("include 를 * 로 넓혀도 민감 엔드포인트는 닫힌다")
    void sensitiveEndpointsStayClosed() throws Exception {
        assertThat(statusOf("/actuator/env")).isEqualTo(404);
        assertThat(statusOf("/actuator/configprops")).isEqualTo(404);
        assertThat(statusOf("/actuator/beans")).isEqualTo(404);
        assertThat(statusOf("/actuator/heapdump")).isEqualTo(404);
    }

    private int statusOf(String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(
                URI.create("http://localhost:" + managementPort + path)).build();
        return HttpClient.newHttpClient()
                .send(request, HttpResponse.BodyHandlers.discarding())
                .statusCode();
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration(exclude = {
            DataSourceAutoConfiguration.class,
            HibernateJpaAutoConfiguration.class
    })
    static class TestApp {
    }
}
