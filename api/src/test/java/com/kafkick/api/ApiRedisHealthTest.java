package com.kafkick.api;

import com.kafkick.testsupport.CommittedConfigStager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        classes = ApiRedisHealthTest.TestApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.config.location=file:build/api-redis-health/management.yml",
                "management.server.port=0",
                "management.endpoint.health.validate-group-membership=false",
                "spring.data.redis.port=1",
                "spring.data.redis.timeout=200ms"
        })
class ApiRedisHealthTest {

    private static final Path STAGED_CONFIG = Path.of("build/api-redis-health/management.yml");

    @BeforeAll
    static void stageManagementConfig() throws Exception {
        CommittedConfigStager.stage(STAGED_CONFIG, "management.yml.example");
    }

    @LocalManagementPort
    int managementPort;

    @Test
    void healthIsUpWithoutRedis() throws Exception {
        HttpRequest request = HttpRequest.newBuilder(
                URI.create("http://localhost:" + managementPort + "/actuator/health")).build();
        int status = HttpClient.newHttpClient()
                .send(request, HttpResponse.BodyHandlers.discarding())
                .statusCode();

        assertThat(status).isEqualTo(200);
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration(exclude = {
            DataSourceAutoConfiguration.class,
            HibernateJpaAutoConfiguration.class
    })
    static class TestApp {
    }
}
