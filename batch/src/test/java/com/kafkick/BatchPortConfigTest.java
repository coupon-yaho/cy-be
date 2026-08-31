package com.kafkick;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;

class BatchPortConfigTest {

    @Test
    @DisplayName("batch 업무 포트와 관리 포트 기본값은 9091 · 9092 로 분리된다")
    void portsHaveDifferentDefaults() {
        String businessPort = applicationTemplate().getProperty("server.port");
        String managementPort = managementTemplate().getProperty("management.server.port");

        assertThat(businessPort).isEqualTo("${BATCH_PORT:9091}");
        assertThat(managementPort).isEqualTo("${BATCH_MANAGEMENT_PORT:9092}");
        assertThat(managementPort).isNotEqualTo(businessPort);
    }

    @Test
    @DisplayName("batch 가 management 설정을 import 한다")
    void applicationImportsManagementConfig() {
        Properties template = applicationTemplate();

        assertThat(template.getProperty("spring.config.import[0]")).isEqualTo("classpath:storage.yml");
        assertThat(template.getProperty("spring.config.import[1]")).isEqualTo("classpath:management.yml");
        assertThat(template.getProperty("spring.config.import[2]")).isEqualTo("classpath:redis.yml");
    }

    @Test
    @DisplayName("batch actuator allowlist 와 민감 엔드포인트 exclude 를 고정한다")
    void exposurePolicyIsFixed() {
        Properties template = managementTemplate();

        assertThat(template.getProperty("management.endpoints.web.exposure.include"))
                .isEqualTo("health,metrics,prometheus");
        // ⚠️ **열하나다(넷 아님).** include 가 * 로 넓어지는 날 실제로 열리는 것을 재 보니
        //    16개였고 그중 위험한 것을 전부 적었다 — loggers 는 POST 로 런타임 로그 레벨을
        //    바꾸는 **쓰기**다. CY-744 합류 전까지 이 파일은 넷만 갖고 있었고,
        //    application.yml 이 적어 둔 열하나를 **조용히 덮고 있었다**
        //    (import 문서가 선언 문서를 이긴다 — ActuatorWildcardExposureTest 가 200 을 받았다).
        assertThat(template.getProperty("management.endpoints.web.exposure.exclude"))
                .isEqualTo("env,configprops,beans,heapdump,loggers,threaddump,mappings,"
                        + "scheduledtasks,conditions,flyway,sbom");
        assertThat(template.getProperty("management.endpoint.health.show-details")).isEqualTo("never");
    }

    private static Properties applicationTemplate() {
        return parse("application.yml.example");
    }

    private static Properties managementTemplate() {
        return parse("management.yml.example");
    }

    private static Properties parse(String resource) {
        YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new ClassPathResource(resource));
        return yaml.getObject();
    }
}
