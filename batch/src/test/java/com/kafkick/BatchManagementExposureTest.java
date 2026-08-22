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
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Configuration;

/** 커밋되는 관리 설정을 실제로 로드해 업무 포트와 관리 포트의 HTTP 경계를 검증한다. */
@SpringBootTest(
        classes = BatchManagementExposureTest.TestApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.config.location=file:build/batch-management-exposure/management.yml",
                "management.server.port=0",
                // 이 앱에는 관측 풀이 없다. obs 그룹 검증까지 켜면 노출 규칙과 무관한 이유로 깨진다.
                "management.endpoint.health.validate-group-membership=false"
        })
class BatchManagementExposureTest {

    private static final Path STAGED_CONFIG =
            Path.of("build/batch-management-exposure/management.yml");

    @LocalServerPort
    int businessPort;

    @LocalManagementPort
    int managementPort;

    @BeforeAll
    static void stageManagementConfig() throws Exception {
        stage(STAGED_CONFIG, "management.yml.example");
    }

    @Test
    @DisplayName("관리 포트에서 prometheus 는 열리고 env 는 닫힌다")
    void managementPortExposesPrometheusAndBlocksEnv() throws Exception {
        assertThat(statusOf(managementPort, "/actuator/prometheus")).isEqualTo(200);
        assertThat(statusOf(managementPort, "/actuator/env")).isEqualTo(404);
    }

    @Test
    @DisplayName("업무 포트에서는 actuator 가 열리지 않는다")
    void businessPortDoesNotExposeActuator() throws Exception {
        assertThat(statusOf(businessPort, "/actuator/health")).isEqualTo(404);
        assertThat(statusOf(businessPort, "/actuator/prometheus")).isEqualTo(404);
    }

    private static int statusOf(int port, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).build();
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
