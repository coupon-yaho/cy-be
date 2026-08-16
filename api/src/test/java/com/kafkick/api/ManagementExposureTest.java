package com.kafkick.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

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
 * <p>PRD 보안 필수 항목 — {@code /actuator/env} 는 AES·HMAC 키를 그대로 내보낸다.
 */
@SpringBootTest(
        classes = ManagementExposureTest.TestApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "management.server.port=0",
                "management.endpoints.web.exposure.include=*",
                "management.endpoints.web.exposure.exclude=env,configprops,beans,heapdump"
        })
class ManagementExposureTest {

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

    @SpringBootConfiguration
    @EnableAutoConfiguration(exclude = {
            DataSourceAutoConfiguration.class,
            HibernateJpaAutoConfiguration.class
    })
    static class TestApp {
    }
}
