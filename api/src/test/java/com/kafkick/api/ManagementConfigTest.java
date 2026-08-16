package com.kafkick.api;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;

/**
 * 실제 설정 파일의 값을 검증한다.
 *
 * <p>{@link ManagementExposureTest} 는 프로퍼티를 덮어써서 {@code exclude} 가 동작하는지만 본다.
 * 여기서는 아무것도 덮어쓰지 않아, {@code management.yml} 이 바뀌거나 {@code application.yml} 의
 * import 가 끊기면 깨진다.
 *
 * <p>포트를 실제로 열지 않으려고 {@code WebEnvironment.NONE} 을 쓴다 — 값만 확인하면 된다.
 */
@SpringBootTest(
        classes = ManagementConfigTest.TestApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE)
class ManagementConfigTest {

    @Autowired
    Environment environment;

    @Test
    @DisplayName("관리 포트가 9090 이다 — Compose 가 막을 포트와 같아야 한다")
    void managementPortIsSeparated() {
        assertThat(environment.getProperty("management.server.port")).isEqualTo("9090");
    }

    @Test
    @DisplayName("allowlist 에 health · metrics · admission-capacity 만 있다")
    void exposureIncludeIsAllowlist() {
        assertThat(environment.getProperty("management.endpoints.web.exposure.include"))
                .isEqualTo("health,metrics,admission-capacity");
    }

    @Test
    @DisplayName("env · configprops · beans · heapdump 는 exclude 로 이중 차단한다")
    void sensitiveEndpointsAreExcluded() {
        assertThat(environment.getProperty("management.endpoints.web.exposure.exclude"))
                .isEqualTo("env,configprops,beans,heapdump");
    }

    @Test
    @DisplayName("health 상세는 닫혀 있다 — 절대경로와 JDBC URL 이 실린다")
    void healthDetailsAreNever() {
        assertThat(environment.getProperty("management.endpoint.health.show-details"))
                .isEqualTo("never");
    }

    /** 이 값들이 보인다는 것 자체가 {@code application.yml} 의 import 가 살아 있다는 뜻이다. */
    @SpringBootConfiguration
    @EnableAutoConfiguration(exclude = {
            DataSourceAutoConfiguration.class,
            HibernateJpaAutoConfiguration.class
    })
    static class TestApp {
    }
}
