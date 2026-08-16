package com.kafkick.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;

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

    private final Environment environment;

    ManagementConfigTest(@Autowired Environment environment) {
        this.environment = environment;
    }

    /**
     * 포트만 템플릿을 파싱해 확인한다.
     *
     * <p>{@code Environment} 로 보면 {@code MANAGEMENT_PORT} 가 설정된 환경에서 깨지고,
     * 그렇다고 테스트에서 그 값을 고정하면 기본값이 바뀌어도 통과해 검증이 무의미해진다.
     * 확인하고 싶은 것은 <b>주입이 없을 때의 기본값</b>이다.
     *
     * <p>문자열 포함 검사가 아니라 파싱이어야 한다 — 들여쓰기가 깨져도 문자열은 그대로 남는다.
     */
    @Test
    @DisplayName("템플릿의 관리 포트 기본값이 9090 이다 — Compose 가 막을 포트와 같아야 한다")
    void managementPortDefaultIsSeparated() {
        assertThat(template().getProperty("management.server.port"))
                .isEqualTo("${MANAGEMENT_PORT:9090}");
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

    private static Properties template() {
        YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new ClassPathResource("management.yml.example"));
        return yaml.getObject();
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
