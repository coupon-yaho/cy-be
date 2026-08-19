package com.kafkick.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;

/**
 * 템플릿에 적힌 설정 값을 검증한다.
 *
 * <p>{@link ManagementExposureTest} 는 프로퍼티를 덮어써서 {@code exclude} 가 동작하는지만 본다.
 * 여기서는 아무것도 덮어쓰지 않아, 템플릿이 바뀌면 깨진다.
 *
 * <p>{@code Environment} 를 읽지 않는 이유 — {@code management.yml} 과 {@code application.yml} 은
 * gitignore 대상이라 신규 클론과 CI 에는 없다. 그 파일들을 읽으면 코드를 건드리지 않아도 깨진다.
 * 그래서 커밋되는 {@code .example} 를 파싱한다. 문자열 포함 검사가 아니라 파싱이어야 하는 이유는
 * 들여쓰기가 깨져도 문자열은 그대로 남기 때문이다.
 *
 * <p><b>대신 잃은 것</b> — 실제로 앱이 읽는 {@code application.yml} 을 보는 테스트는 이제 없다.
 * 그 파일은 커밋되지 않으니 CI 는 원래도 못 봤지만, 로컬에서 자기 파일의 import 를 끊었을 때
 * 받던 빨간불은 사라졌다. 여기서 검증하는 것은 <b>템플릿</b>이고, 각자의 실제 파일이 템플릿과
 * 같은지는 기동 시 검증(@Validated)과 문서가 맡는다.
 */
class ManagementConfigTest {

    private static final String GROUP_MAPPING = "management.endpoint.health.group.obs.status.http-mapping.";

    /**
     * 확인하고 싶은 것은 <b>주입이 없을 때의 기본값</b>이다. {@code MANAGEMENT_PORT} 가 설정된
     * 환경에서도 같은 결과가 나와야 하므로 치환 전 표현식 그대로 본다.
     */
    @Test
    @DisplayName("템플릿의 관리 포트 기본값이 9090 이다 — Compose 가 막을 포트와 같아야 한다")
    void managementPortDefaultIsSeparated() {
        assertThat(template().getProperty("management.server.port"))
                .isEqualTo("${MANAGEMENT_PORT:9090}");
    }

    @Test
    @DisplayName("allowlist 에 health · metrics · prometheus · admission-capacity 만 있다")
    void exposureIncludeIsAllowlist() {
        assertThat(template().getProperty("management.endpoints.web.exposure.include"))
                .isEqualTo("health,metrics,prometheus,admission-capacity");
    }

    @Test
    @DisplayName("env · configprops · beans · heapdump 는 exclude 로 이중 차단한다")
    void sensitiveEndpointsAreExcluded() {
        assertThat(template().getProperty("management.endpoints.web.exposure.exclude"))
                .isEqualTo("env,configprops,beans,heapdump");
    }

    @Test
    @DisplayName("health 상세는 닫혀 있다 — 절대경로와 JDBC URL 이 실린다")
    void healthDetailsAreNever() {
        assertThat(template().getProperty("management.endpoint.health.show-details"))
                .isEqualTo("never");
    }

    /** 값이 아무리 맞아도 import 가 끊기면 앱에는 실리지 않는다. 값 검증과 짝이 되는 확인이다. */
    @Test
    @DisplayName("api 템플릿이 storage · management 를 모두 import 한다")
    void applicationTemplateImportsBothFiles() {
        Properties template = parse("application.yml.example");

        assertThat(template.getProperty("spring.config.import[0]")).isEqualTo("classpath:storage.yml");
        assertThat(template.getProperty("spring.config.import[1]")).isEqualTo("classpath:management.yml");
    }

    /**
     * 관측 풀이 죽어도 기본 헬스는 UP 이어야 한다 — 발급 API 가 멀쩡한데 인스턴스가 빠지면 안 된다.
     * 그 성질은 status 순서가 만든다. OBSERVATION_DOWN 이 UP 뒤에 있어야 합산 상태를 못 끌어내린다.
     */
    @Test
    @DisplayName("관측 풀 상태는 UP 보다 덜 심각하게 배치된다")
    void observationStatusNeverDragsDownTheAggregate() {
        String order = template().getProperty("management.endpoint.health.status.order");

        assertThat(order).isEqualTo("DOWN,OUT_OF_SERVICE,UP,OBSERVATION_DOWN,UNKNOWN");
        assertThat(order.indexOf("OBSERVATION_DOWN")).isGreaterThan(order.indexOf("UP"));
    }

    @Test
    @DisplayName("관측 풀은 obs 그룹으로 따로 감시한다 — 오타 배포를 여기서 잡는다")
    void observationPoolHasItsOwnHealthGroup() {
        Properties template = template();

        assertThat(template.getProperty("management.endpoint.health.group.obs.include")).isEqualTo("obsDb");
        assertThat(template.getProperty(GROUP_MAPPING + "OBSERVATION_DOWN")).isEqualTo("503");
    }

    /**
     * http-mapping 은 하나라도 지정하면 기본 매핑을 <b>교체</b>한다(병합이 아니다). 전역에 한 줄 넣는
     * 순간 DOWN·OUT_OF_SERVICE 의 503 이 사라져서, 운영 풀이 죽어도 헬스가 200 을 돌려준다.
     * 그러면 로드밸런서가 죽은 인스턴스를 계속 물고 있는다 — 관측을 위한 설정이 서비스를 망가뜨린다.
     */
    @Test
    @DisplayName("전역 http-mapping 은 비어 있다 — 지정하면 DOWN 의 503 이 사라진다")
    void globalHttpMappingStaysEmpty() {
        // stringPropertyNames() 로 보면 안 된다 — 매핑 값이 Integer 라 그 목록에서 빠지고,
        // 전역 매핑이 있어도 테스트가 통과한다. 실제로 그렇게 썼다가 회귀를 못 잡았다.
        assertThat(template().keySet())
                .noneMatch(key -> key.toString().startsWith("management.endpoint.health.status.http-mapping"));
    }

    @Test
    @DisplayName("그룹 매핑은 기본값까지 다시 적는다 — 교체 규칙 때문에 빠뜨리면 그룹만 200 이 된다")
    void groupMappingRestatesDefaults() {
        Properties template = template();

        assertThat(template.getProperty(GROUP_MAPPING + "DOWN")).isEqualTo("503");
        assertThat(template.getProperty(GROUP_MAPPING + "OUT_OF_SERVICE")).isEqualTo("503");
    }

    private static Properties template() {
        return parse("management.yml.example");
    }

    private static Properties parse(String resource) {
        YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new ClassPathResource(resource));
        return yaml.getObject();
    }
}
