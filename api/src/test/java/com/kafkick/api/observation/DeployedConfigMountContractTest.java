// 설정을 마운트하는 서비스가 그 설정이 실렸는지 확인하게 강제합니다.
package com.kafkick.api.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

/**
 * <b>{@link com.kafkick.core.support.config.DeployedConfigGuard} 는 켜 준 서비스에서만
 * 뜻이 있다.</b> 마운트는 하는데 스위치를 안 켜면 가드가 그 서비스만 조용히 비껴간다 —
 * 가드가 없는 것과 같은데 초록불은 있는 상태다. 그 짝을 여기서 못 박는다.
 *
 * <h2>막으려는 사고</h2>
 *
 * <p>{@code compose.yml} 이 호스트의 {@code ./application.yml} 을 bind mount 하는데
 * <b>그 파일이 없으면 Docker 가 같은 이름의 디렉터리를 만들어 붙인다.</b> 스프링은 디렉터리를
 * 설정으로 못 읽고 <b>오류 없이</b> jar 기본값으로 뜬다. 2026-08-30 에 실제로 그 상태로
 * 여섯 시간을 돌았고, tomcat 스레드가 15 대신 Boot 기본 200 이었다.
 *
 * <p>{@code compose.yml} 은 그 함정을 주석으로 이미 적어 뒀다. <b>주석은 실행되지 않는다.</b>
 *
 * <h2>왜 실제 {@code application.yml} 을 안 보나</h2>
 *
 * <p>그 파일은 gitignore 대상이라 CI 체크아웃에 없다. 그래서 이 테스트가 보는 것은
 * <b>템플릿과 compose 의 짝</b>이고, 실제 파일이 비었는지는 <b>런타임 가드</b>가 본다.
 * 둘이 한 쌍이라 어느 하나만으로는 이 사고를 못 막는다.
 */
class DeployedConfigMountContractTest {

    /** 컨테이너 안에서 스프링이 읽는 자리. compose 의 bind mount 오른쪽이다. */
    private static final String MOUNT_TARGET = "/app/config/application.yml";

    /** {@link com.kafkick.core.support.config.DeployedConfigGuard#REQUIRED} 의 환경변수 이름. */
    private static final String SWITCH = "DEPLOYED_CONFIG_REQUIRED";

    /** {@link com.kafkick.core.support.config.DeployedConfigGuard#MARKER} 가 찾는 키. */
    private static final String MARKER_KEY = "deployed-config";

    @Test
    @DisplayName("설정을 마운트하는 서비스는 전부 가드를 켠다 — 켜는 자리와 붙이는 자리가 같아야 한다")
    void everyMountingServiceEnablesTheGuard() throws IOException {
        List<String> mounting = servicesMounting(MOUNT_TARGET);

        assertThat(mounting)
                .as("설정을 마운트하는 서비스가 하나도 없다 — 이 테스트가 아무것도 안 잰다. "
                        + "compose 의 volumes 가 바뀌었으면 MOUNT_TARGET 을 함께 고쳐라")
                .isNotEmpty();

        List<String> unguarded = new ArrayList<>();
        for (String name : mounting) {
            if (!"true".equals(environmentOf(name).get(SWITCH))) {
                unguarded.add(name);
            }
        }

        assertThat(unguarded)
                .as("""
                        설정을 마운트하면서 %s 를 안 켠 서비스가 있다.

                        그 서비스는 파일이 없어 디렉터리가 붙어도 조용히 뜬다 — 설정이 통째로
                        비는데 로그에도 안 나온다. 마운트를 지웠다면 이 목록에서도 빠져야 하고,
                        마운트를 남겼다면 스위치를 켜야 한다.""".formatted(SWITCH))
                .isEmpty();
    }

    /**
     * <b>마커가 템플릿에 있어야 복사가 곧 처방이 된다.</b> 가드의 거절 메시지가
     * {@code cp application.yml.example application.yml} 을 시키는데, 템플릿에 마커가 없으면
     * 그 처방을 따라도 여전히 거절당한다.
     */
    @Test
    @DisplayName("템플릿에 마커가 있다 — 거절 메시지가 시키는 복사가 실제로 문제를 푼다")
    void templateCarriesTheMarker() throws IOException {
        String template = Files.readString(
                repoRoot().resolve("application.yml.example"), StandardCharsets.UTF_8);

        assertThat(template)
                .as("%s 가 application.yml.example 에 없다. 가드가 시키는 복사를 해도 "
                        + "여전히 거절당한다", MARKER_KEY)
                .contains(MARKER_KEY + ":");
    }

    /**
     * <b>jar 안에 같은 키가 있으면 가드가 통째로 무의미해진다.</b> 마운트가 없어도 값이
     * 보이기 때문이다. 모듈 리소스의 {@code .example} 은 스프링이 안 읽지만, 누군가
     * {@code application.yml} 로 이름을 바꿔 넣는 날 이 검사가 잡는다.
     */
    @Test
    @DisplayName("모듈 리소스에는 마커가 없다 — 있으면 마운트 없이도 통과한다")
    void moduleResourcesDoNotCarryTheMarker() throws IOException {
        for (String module : List.of("api", "batch", "core", "storage")) {
            Path resources = repoRoot().resolve(module).resolve("src/main/resources");
            if (!Files.isDirectory(resources)) {
                continue;
            }
            try (var files = Files.walk(resources)) {
                for (Path file : files.filter(Files::isRegularFile).toList()) {
                    String name = file.getFileName().toString();
                    if (!name.endsWith(".yml") && !name.endsWith(".yaml")) {
                        continue;
                    }
                    assertThat(Files.readString(file, StandardCharsets.UTF_8))
                            .as("%s 가 마커를 갖고 있다 — 마운트 없이도 가드가 통과한다", file)
                            .doesNotContain(MARKER_KEY + ":");
                }
            }
        }
    }

    private List<String> servicesMounting(String target) throws IOException {
        List<String> names = new ArrayList<>();
        for (Map.Entry<String, Object> entry : services().entrySet()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> service = (Map<String, Object>) entry.getValue();
            Object volumes = service.get("volumes");
            if (!(volumes instanceof List<?> list)) {
                continue;
            }
            for (Object volume : list) {
                if (volume instanceof String bind && bind.contains(":" + target)) {
                    names.add(entry.getKey());
                    break;
                }
            }
        }
        return names;
    }

    private Map<String, Object> environmentOf(String service) throws IOException {
        @SuppressWarnings("unchecked")
        Map<String, Object> definition = (Map<String, Object>) services().get(service);
        Object environment = definition.get("environment");
        if (environment instanceof Map<?, ?> map) {
            Map<String, Object> result = new java.util.LinkedHashMap<>();
            map.forEach((k, v) -> result.put(String.valueOf(k), v == null ? null : String.valueOf(v)));
            return result;
        }
        return Map.of();
    }

    private Map<String, Object> services() throws IOException {
        try (var in = Files.newInputStream(repoRoot().resolve("compose.yml"))) {
            @SuppressWarnings("unchecked")
            Map<String, Object> root = new Yaml().load(in);
            @SuppressWarnings("unchecked")
            Map<String, Object> services = (Map<String, Object>) root.get("services");
            assertThat(services).as("compose.yml 에 services 가 없다").isNotNull();
            return services;
        }
    }

    /** 테스트 작업 디렉터리는 모듈 루트다 — {@code batch} 의 대조 테스트들과 같은 방식. */
    private Path repoRoot() {
        return Path.of("..");
    }
}
