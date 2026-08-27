package com.kafkick.api.observation;

import static org.assertj.core.api.Assertions.assertThat;
import static com.kafkick.api.observation.ConfigContractFixture.defaultOf;
import static com.kafkick.api.observation.ConfigContractFixture.loadYaml;
import static com.kafkick.api.observation.ConfigContractFixture.repoRoot;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Locale;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

/**
 * 컨테이너에 마운트되는 루트 {@code application.yml} 의 템플릿이 모듈 계약과 어긋나지
 * 않는지 본다.
 *
 * <p>이 파일이 왜 위험한가 — {@code compose.yml} 이 이것을 {@code /app/config/application.yml}
 * 로 마운트하고, 그 위치는 이미지 안 클래스패스보다 우선한다. 즉 <b>실질 운영 설정</b>인데,
 * 모듈의 {@code *.yml.example} 을 검증하는 기존 테스트들({@code BatchPortConfigTest} 등)은
 * 이 파일을 전혀 보지 않는다. 한쪽만 고치면 로컬 gradle 과 컨테이너가 조용히 갈라진다.
 *
 * <p><b>겹침 구조는 실측으로 확인했다</b>(2026-08). 마운트 파일은 덮어쓰기가 아니라 겹치기다 —
 * 여기서 언급하지 않은 키는 이미지 안 값이 살아남는다. batch 프로파일에 {@code management}
 * 블록이 없어도 최종 해석값이 {@code management.server.port=9092},
 * {@code include=health,metrics,prometheus} 였다. 그래서 이 테스트는 "값이 여기 있는가" 가
 * 아니라 <b>"여기서 계약을 깨는 값을 덮어쓰지 않는가"</b> 를 본다.
 *
 * <p><b>잡지 못하는 것.</b> Boot 의 겹침 규칙 자체가 바뀌면 이 테스트는 여전히 통과한다.
 * 그건 정적 대조로는 못 잡고, 실제 이미지를 띄워야 드러난다. 배포 검증은
 * {@code up{job="api"}}와 {@code up{job="batch"}} 실측으로 별도 확인한다.
 */
class DeployedConfigContractTest {

    /**
     * 실제로 마운트되는 것은 {@code application.yml} 이지만 그건 gitignore 대상이라 신규
     * 클론에 없다. 커밋되는 템플릿을 본다 — 각자 이걸 복사해 쓰므로 여기가 계약의 원본이다.
     *
     * <p><b>잡지 못하는 것.</b> 복사한 뒤 로컬에서 고친 값은 여기 안 보인다. 그건 커밋되지
     * 않는 파일이라 대조할 원본이 없다.
     */
    private static final String DEPLOYED = "application.yml.example";

    /** compose 의 ports 왼쪽. 호스트 어느 번호로 내보낼지만 정한다. */
    private static final String HOST_PORT_VARIABLE = "API_HOST_PORT";

    /** compose 의 ports 오른쪽이자 {@code application.yml.example} 의 {@code server.port}. */
    private static final String CONTAINER_PORT_VARIABLE = "SERVER_PORT";

    @Test
    @DisplayName("호스트 공개 포트와 컨테이너 Spring 포트가 서로 다른 변수다")
    void theHostPortAndTheContainerPortAreDifferentVariables() throws IOException {
        Map<String, Object> api = composeService("api");

        @SuppressWarnings("unchecked")
        List<String> ports = (List<String>) api.get("ports");
        String mapping = ports.stream()
                .filter(port -> port.endsWith("}") || port.endsWith(":8080"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("api 에 업무 포트 매핑이 없다"));

        // ⚠️ 그냥 마지막 ':' 로 자르면 안 된다. ${VAR:-기본값} 안에도 콜론이 있어서
        //    "${A:-8080}:${B:-8080}" 이 "-8080}" 으로 잘린다(실측 — 처음에 그렇게 짰다).
        List<String> sides = ComposePortMapping.split(mapping);
        String host = sides.get(0);
        String container = sides.get(sides.size() - 1);

        assertThat(variableOf(host))
                .as("한 이름이 두 자리에서 두 뜻이면 어느 쪽을 고쳐도 반대쪽이 깨진다."
                        + " 실측 — 예전 구조에서 .env 의 SERVER_PORT 를 18080 으로 올리면"
                        + " 호스트 매핑만 바뀌고 컨테이너는 계속 8080 에서 들었다")
                .isNotEqualTo(variableOf(container));

        assertThat(variableOf(container))
                .as("컨테이너 쪽은 api/src/main/resources/application.yml.example 의"
                        + " ${SERVER_PORT:8080} 이 읽는 이름과 같아야 한다. 다르면 매핑은"
                        + " 성립하는데 Spring 이 다른 포트에서 들어 연결이 reset 된다")
                .isEqualTo(CONTAINER_PORT_VARIABLE);

        // "서로 다르기만 하면 된다" 로는 부족하다 — ${MYSQL_PORT}:${SERVER_PORT} 도 통과한다.
        // 호스트 쪽 이름은 .env.example 이 사람에게 알려 주는 이름과 같아야 한다.
        assertThat(variableOf(host))
                .as("이 이름이 계약이다. compose 만 다른 이름으로 바꾸면 .env.example 의 %s 는"
                        + " 아무도 안 읽는 값이 되고, 그걸 고친 사람은 포트가 안 바뀌는 이유를"
                        + " 알 수 없다", HOST_PORT_VARIABLE)
                .isEqualTo(HOST_PORT_VARIABLE);

        assertThat(environmentDeclarations())
                .as("compose 가 쓰는 두 이름이 .env.example 에 없으면 신규 클론은 그 값을"
                        + " 채울 자리를 못 찾는다. 호스트 쪽은 기본값이 있어 조용히 8080 이 되고,"
                        + " 그건 이미 물려 있는 포트일 수 있다")
                .contains(HOST_PORT_VARIABLE, CONTAINER_PORT_VARIABLE);

        @SuppressWarnings("unchecked")
        Map<String, Object> environment = (Map<String, Object>) api.get("environment");
        assertThat(environment)
                .as("이제 덮어쓸 이유가 없다. 남겨 두면 .env 의 SERVER_PORT 를 고쳐도 컨테이너"
                        + " 포트가 안 바뀌어, 뜻을 가른 것이 다시 무의미해진다")
                .doesNotContainKey("SERVER_PORT");
    }

    @Test
    @DisplayName("배포 설정이 batch 관리 포트를 덮어쓰지 않는다 — 덮어쓰면 scrape 대상과 갈린다")
    void deployedConfigDoesNotOverrideTheBatchManagementPort() throws IOException {
        Map<String, Object> batchProfile = profile("batch");
        Object management = batchProfile.get("management");

        if (management == null) {
            // 지금 상태다. 이미지 안 management.yml 이 9092 를 정하고 그대로 살아남는다.
            return;
        }

        // 굳이 적었다면, 모듈이 정한 값과 같아야 한다. 다르면 prometheus 가 긁는 포트와
        // 실제 포트가 갈리고, 그건 up=0 말고는 아무 신호가 없다.
        assertThat(managementPortOf(management))
                .as("배포 설정이 batch 관리 포트를 적었다면 %s 의 기본값과 같아야 한다",
                        "batch/src/main/resources/management.yml.example")
                .isEqualTo(moduleBatchManagementPort());
    }

    @Test
    @DisplayName("배포 설정의 batch 업무 포트 기본값이 모듈·계약과 같다")
    void deployedBatchServerPortMatchesTheModule() throws IOException {
        assertThat(defaultOf(String.valueOf(serverPortOf(profile("batch")))))
                .as("9090 은 이 저장소에서 api 관리 포트 관용값이라, batch 업무 포트에 쓰면"
                        + " 읽는 사람이 헷갈린다. 모듈 example 과 같은 값이어야 한다")
                .isEqualTo(defaultOf(moduleBatchServerPort()));
    }

    @Test
    @DisplayName("배포 설정의 api 관리 포트가 prometheus 가 긁는 포트와 같다")
    void deployedApiManagementPortMatchesTheScrapeTarget() throws IOException {
        Map<String, Object> apiProfile = profile("api");
        Object management = apiProfile.get("management");
        assertThat(management).as("api 프로파일이 관리 포트를 직접 정하고 있다").isNotNull();

        String scraped = scrapeTargetPort("api");
        assertThat(defaultOf(managementPortOf(management)))
                .as("prometheus.yml 은 api:%s 를 긁는다. 이 파일이 다른 포트를 지정하면"
                        + " 실제 서비스는 그쪽에 뜨고 scrape 는 영영 실패한다", scraped)
                .isEqualTo(scraped);
    }

    @Test
    @DisplayName("배포 설정이 비밀번호 fallback 을 두지 않는다 — .env 가 빠지면 죽어야 한다")
    void noPasswordFallsBackToALiteral() throws IOException {
        List<String> offenders = new ArrayList<>();
        for (String line : Files.readAllLines(repoRoot().resolve(DEPLOYED))) {
            String stripped = line.strip();
            int keyEnd = stripped.indexOf(':');
            if (stripped.startsWith("#") || keyEnd < 0
                    || !stripped.substring(0, keyEnd).toLowerCase(Locale.ROOT)
                            .contains("password")) {
                continue;
            }
            // ${VAR} 는 되고 ${VAR:literal} 은 안 된다 — 후자는 .env 가 없어도 조용히 뜬다.
            if (stripped.matches(".*\\$\\{[A-Z_]+:[^}]+}.*")) {
                offenders.add(stripped);
            }
        }
        assertThat(offenders)
                .as("기본 비밀번호가 박혀 있으면 .env 를 빠뜨린 배포가 에러 없이 약한 비번으로"
                        + " 기동한다. 그 상태는 로그에도 안 남는다")
                .isEmpty();
    }

    @Test
    @DisplayName("모든 실행 프로필은 오류 응답에 스택트레이스를 노출하지 않는다")
    void everyRuntimeProfileHidesStacktraces() throws IOException {
        for (String profile : List.of("api", "batch")) {
            assertThat(serverErrorStacktrace(profile(profile)))
                    .as("배포 %s 프로필의 server.error.include-stacktrace", profile)
                    .isEqualTo("never");
        }
        for (String module : List.of("api", "batch")) {
            assertThat(serverErrorStacktrace(loadModuleApplication(module)))
                    .as("%s 모듈 example 의 server.error.include-stacktrace", module)
                    .isEqualTo("never");
        }
    }

    // ── 읽기 도우미 ───────────────────────────────────────────────────────────

    /** {@code "${NAME:-기본값}"} · {@code "${NAME}"} 에서 이름만 뽑는다. */
    private String variableOf(String fragment) {
        int open = fragment.indexOf("${");
        assertThat(open)
                .as("포트 매핑 양쪽이 변수여야 한다. 숫자를 박으면 .env 로 못 바꾼다: %s",
                        fragment)
                .isNotNegative();
        String inside = fragment.substring(open + 2, fragment.lastIndexOf('}'));
        int separator = inside.indexOf(':');
        return separator < 0 ? inside : inside.substring(0, separator);
    }

    private Map<String, Object> composeService(String name) throws IOException {
        @SuppressWarnings("unchecked")
        Map<String, Object> services = (Map<String, Object>) loadYaml(
                repoRoot().resolve("compose.yml")).get("services");
        @SuppressWarnings("unchecked")
        Map<String, Object> service = (Map<String, Object>) services.get(name);
        assertThat(service).as("compose 에 %s 서비스가 없다", name).isNotNull();
        return service;
    }

    /** {@code .env.example} 이 정의하는 변수 이름들. 주석과 빈 줄은 뺀다. */
    private List<String> environmentDeclarations() throws IOException {
        List<String> names = new ArrayList<>();
        for (String line : Files.readAllLines(repoRoot().resolve(".env.example"))) {
            String stripped = line.strip();
            int assign = stripped.indexOf('=');
            if (stripped.startsWith("#") || assign <= 0) {
                continue;
            }
            names.add(stripped.substring(0, assign));
        }
        return names;
    }

    /**
     * OBS-35 에서 뜻을 갈랐다. 예전에는 호스트 공개 포트와 컨테이너 Spring 포트가 둘 다
     * {@code SERVER_PORT} 였고, compose 가 컨테이너 값을 {@code "8080"} 으로 덮어써서
     * 버텼다 — 그 시절 이 테스트는 그 덮어쓰기의 존재를 지켰다. 이제는 <b>두 자리가 서로 다른
     * 변수를 쓰는지</b>를 지킨다. 덮어쓰기는 증상 대처였고 이건 원인 쪽이다.
     *
     * <p><b>잡지 못하는 것.</b> 커밋되지 않는 {@code .env} 에서 두 값을 어떻게 채웠는지.
     * 거기서 {@code API_HOST_PORT} 를 빼면 compose 기본값 8080 이 쓰인다.
     */

    /** 여러 문서(---) 중 {@code spring.config.activate.on-profile} 이 맞는 것. */
    private Map<String, Object> profile(String name) throws IOException {
        try (var in = Files.newInputStream(repoRoot().resolve(DEPLOYED))) {
            for (Object document : new Yaml().loadAll(in)) {
                if (!(document instanceof Map<?, ?> map)) {
                    continue;
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> typed = (Map<String, Object>) map;
                if (name.equals(activatedProfile(typed))) {
                    return typed;
                }
            }
        }
        throw new AssertionError(DEPLOYED + " 에 " + name + " 프로파일 문서가 없다");
    }

    private String activatedProfile(Map<String, Object> document) {
        Object node = document.get("spring");
        for (String key : List.of("config", "activate", "on-profile")) {
            if (!(node instanceof Map<?, ?> map)) {
                return null;
            }
            node = map.get(key);
        }
        return node instanceof String profile ? profile : null;
    }

    private String managementPortOf(Object management) {
        @SuppressWarnings("unchecked")
        Map<String, Object> server = (Map<String, Object>) ((Map<String, Object>) management)
                .get("server");
        return String.valueOf(server.get("port"));
    }

    private Object serverPortOf(Map<String, Object> profile) {
        @SuppressWarnings("unchecked")
        Map<String, Object> server = (Map<String, Object>) profile.get("server");
        return server.get("port");
    }

    private Map<String, Object> loadModuleApplication(String module) throws IOException {
        return ConfigContractFixture.loadYaml(repoRoot().resolve(
                module + "/src/main/resources/application.yml.example"));
    }

    private String serverErrorStacktrace(Map<String, Object> config) {
        Object node = config;
        for (String key : List.of("server", "error", "include-stacktrace")) {
            node = ((Map<?, ?>) node).get(key);
        }
        return String.valueOf(node);
    }

    private String moduleBatchManagementPort() throws IOException {
        return defaultOf(portFrom("batch/src/main/resources/management.yml.example",
                List.of("management", "server", "port")));
    }

    private String moduleBatchServerPort() throws IOException {
        return portFrom("batch/src/main/resources/application.yml.example",
                List.of("server", "port"));
    }

    private String portFrom(String file, List<String> path) throws IOException {
        try (var in = Files.newInputStream(repoRoot().resolve(file))) {
            Object node = new Yaml().load(in);
            for (String key : path) {
                node = ((Map<?, ?>) node).get(key);
            }
            return String.valueOf(node);
        }
    }

    /** {@code prometheus.yml} 의 {@code job} 이 긁는 포트. */
    private String scrapeTargetPort(String job) throws IOException {
        try (var in = Files.newInputStream(repoRoot().resolve("infra/prometheus/prometheus.yml"))) {
            Map<String, Object> config = new Yaml().load(in);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> jobs =
                    (List<Map<String, Object>>) config.get("scrape_configs");
            for (Map<String, Object> definition : jobs) {
                if (!job.equals(definition.get("job_name"))) {
                    continue;
                }
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> statics =
                        (List<Map<String, Object>>) definition.get("static_configs");
                String target = String.valueOf(((List<?>) statics.get(0).get("targets")).get(0));
                return target.substring(target.lastIndexOf(':') + 1);
            }
        }
        throw new AssertionError("prometheus.yml 에 job " + job + " 이 없다");
    }

}
