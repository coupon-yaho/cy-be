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
                "management.endpoint.health.validate-group-membership=false",
                "spring.autoconfigure.exclude="
                        + "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration,"
                        + "org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration"
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
    /**
     * ⚠️ 제외 목록을 {@code @EnableAutoConfiguration(exclude = ...)} 로 적지 않는다. 이 클래스는
     * {@code com.kafkick} 안에 있고 {@code BatchApplication} 이 그 패키지를 통째로 스캔한다.
     *
     * <p><b>실측.</b> 세 테스트(여기 · Wildcard · RedisHealth)가 제외를 애노테이션으로 적으면
     * {@code :batch:test} 에서 batch 앱 컨텍스트가 {@code EntityManagerFactory} 없이 떠서 13개가
     * 깨진다 — batch 의 yml 에서 JPA 제외를 이미 걷었는데도 그렇다. 제외를 각 테스트의
     * {@code properties} 로 옮기면 사라진다.
     *
     * <p><b>밝히지 못한 것.</b> 세 파일 중 하나만 애노테이션으로 되돌렸을 때는 재현되지 않았다.
     * 전파 경로를 정확히 짚지 못했으므로 여기 적는 것은 관측된 사실까지다 — 되돌리려는 사람은
     * 세 파일을 함께 바꿔 {@code :batch:test} 전체를 돌려 볼 것.
     */
    @EnableAutoConfiguration
    static class TestApp {
    }
}
