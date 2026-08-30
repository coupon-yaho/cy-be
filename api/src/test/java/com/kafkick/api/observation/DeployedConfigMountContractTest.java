// 설정을 마운트하는 서비스가 그 설정이 실렸는지 확인하게 강제합니다.
package com.kafkick.api.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
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

    /** {@link com.kafkick.core.support.config.DeployedConfigGuard#MARKER} 의 앞·뒤 마디. */
    private static final String MARKER_KEY = "deployed-config";

    private static final String MARKER_LEAF = "marker";

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
        // **문자열로 찾으면 안 된다.** `# deployed-config:` 처럼 주석 처리해도 통과하는데,
        // 그 템플릿을 복사하면 프로퍼티가 없어 정상 배포까지 거절당한다 — 이 테스트가
        // 지킨다고 말한 계약이 정확히 그때 깨진다(리뷰가 잡았다). 그래서 파싱한다.
        assertThat(markerValueIn(repoRoot().resolve("application.yml.example")))
                .as("application.yml.example 에 %s.%s 가 실제 프로퍼티로 없다. "
                        + "주석 처리된 것도 없는 것이다 — 가드가 시키는 복사를 해도 "
                        + "여전히 거절당한다", MARKER_KEY, MARKER_LEAF)
                .isNotBlank();
    }

    /**
     * <b>jar 안에 같은 키가 있으면 가드가 통째로 무의미해진다</b> — 마운트가 없어도 값이
     * 보이기 때문이다.
     *
     * <p><b>{@code .yml.example} 을 봐야 한다.</b> 처음에 {@code .yml}·{@code .yaml} 만
     * 훑었는데, 저장소에는 그런 파일이 <b>하나도 없어서</b> 이 검사가 파일 0개를 보고
     * 통과했다(리뷰가 잡았다). 실제로 jar 에 들어가는 것은 이것들이다 —
     * {@code Dockerfile} 이 이미지 빌드 때 확장자를 떼어 복사한다:
     *
     * <pre>
     * find . -path '*&#47;src/main/resources/*.yml.example' \
     *   -exec sh -c 'cp "$1" "${1%.example}"' _ {} \;
     * </pre>
     *
     * <p><b>모듈 목록도 안 박는다.</b> {@code api·batch·core·storage} 만 적었다가
     * {@code infra/redis} 를 빠뜨렸는데, 그 모듈의 {@code redis.yml.example} 도 같은 규칙으로
     * jar 에 들어간다. Dockerfile 과 같은 패턴으로 <b>저장소 전체를 훑는다.</b>
     */
    @Test
    @DisplayName("모듈 리소스에는 마커가 없다 — 있으면 마운트 없이도 통과한다")
    void moduleResourcesDoNotCarryTheMarker() throws IOException {
        List<Path> scanned = moduleResourceConfigs();

        assertThat(scanned)
                .as("훑은 파일이 하나도 없다 — 이 검사가 아무것도 안 잰다. Dockerfile 의 "
                        + "복사 패턴이 바뀌었으면 여기도 함께 고쳐라")
                .isNotEmpty();

        for (Path file : scanned) {
            assertThat(markerValueIn(file))
                    .as("%s 가 마커를 갖고 있다 — Dockerfile 이 이 파일을 jar 에 넣으므로 "
                            + "마운트가 없어도 가드가 통과한다", file)
                    .isNull();
        }
    }

    /**
     * {@code Dockerfile} 이 jar 에 넣는 설정 리소스 전부. 모듈 목록을 박지 않는 이유는
     * 위 javadoc 에 있다 — 박으면 새 모듈이 조용히 검사 밖에 남는다.
     */
    private static String text(Object value) {
        return value == null || String.valueOf(value).isBlank() ? null : String.valueOf(value);
    }

    private List<Path> moduleResourceConfigs() throws IOException {
        List<Path> found = new ArrayList<>();
        try (var files = Files.walk(repoRoot())) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                String path = file.toString().replace('\\', '/');
                if (!path.contains("/src/main/resources/") || path.contains("/build/")) {
                    continue;
                }
                String name = file.getFileName().toString();
                if (name.endsWith(".yml.example") || name.endsWith(".yaml.example")
                        || name.endsWith(".yml") || name.endsWith(".yaml")) {
                    found.add(file);
                }
            }
        }
        return found;
    }

    /**
     * <b>파싱해서 값을 꺼낸다.</b> 문자열로 찾으면 주석까지 잡힌다. 여러 문서가 든 파일이라
     * {@code loadAll} 로 전부 본다 — 프로파일 문서에 숨겨 둔 마커도 같은 효력이다.
     */
    private String markerValueIn(Path file) throws IOException {
        try (var in = Files.newInputStream(file)) {
            for (Object document : new Yaml().loadAll(in)) {
                if (!(document instanceof Map<?, ?> map)) {
                    continue;
                }
                // **중첩과 평면 둘 다 본다.** 스프링은 아래 두 모양을 같은 프로퍼티로
                // 읽는데, 중첩만 보면 평면으로 적힌 마커가 jar 에 섞여 들어가도
                // 못 잡는다(리뷰가 잡았다).
                //
                //   deployed-config:        deployed-config.marker: v
                //     marker: v
                Object nested = map.get(MARKER_KEY);
                if (nested instanceof Map<?, ?> marker) {
                    String value = text(marker.get(MARKER_LEAF));
                    if (value != null) {
                        return value;
                    }
                }
                String flat = text(map.get(MARKER_KEY + "." + MARKER_LEAF));
                if (flat != null) {
                    return flat;
                }
            }
        }
        return null;
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
