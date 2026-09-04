package com.kafkick.storage.db;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

/**
 * <b>{@code app-grants} 가 Flyway 뒤에 도는 것은 compose 가 지킨다.</b>
 *
 * <p>스크립트가 스스로 판정하려던 두 번의 시도가 모두 틀렸다({@code apply.sh} ② 참고) —
 * 공통 원인은 <b>바깥에서 안을 추측한 것</b>이다. 지금 쓰는 신호는 <b>실행 중인 이미지
 * 자신이</b> 낸다: Spring 은 Flyway 를 컨텍스트 초기화 중에 돌리고 웹 서버는 그 뒤에 뜨므로,
 * actuator 가 응답하면 마이그레이션은 끝나 있다(실측: 기동 로그에서
 * {@code Successfully applied 45 migrations} 가 {@code Tomcat started on port} 보다 앞선다).
 *
 * <p>그래서 이 계약은 <b>파일 두 개에 걸쳐 있고 각각으로는 못 지킨다</b> — healthcheck 만
 * 있고 {@code service_healthy} 가 아니면 순서가 안 서고, {@code service_healthy} 만 있고
 * healthcheck 가 없으면 compose 가 아예 안 뜬다. 어느 쪽으로 깨져도 증상은 조용하다:
 * 권한이 이른 시점에 적용되고, 뒤에 생긴 테이블이 DML 권한을 못 받아 앱이 런타임에
 * {@code 1142} 로 죽는다.
 *
 * <p><b>잡지 못하는 것</b> — 실제로 그 순서로 도는지. 여기서 보는 것은 선언뿐이다.
 */
class AppGrantsComposeContractTest {

    private static final Path COMPOSE_FILE = Path.of("..", "compose.yml");

    @SuppressWarnings("unchecked")
    private Map<String, Object> service(String name) throws IOException {
        Map<String, Object> root;
        try (var in = Files.newInputStream(COMPOSE_FILE)) {
            root = new Yaml().load(in);
        }
        Map<String, Object> services = (Map<String, Object>) root.get("services");
        assertThat(services).as("compose.yml 에 services 가 없습니다").isNotNull();
        Map<String, Object> service = (Map<String, Object>) services.get(name);
        assertThat(service).as("compose.yml 에 %s 서비스가 없습니다", name).isNotNull();
        return service;
    }

    @Test
    @DisplayName("app-grants 는 api 가 healthy 가 될 때까지 기다린다 — started 로는 Flyway 종료를 모른다")
    void appGrantsWaitsForApiToBecomeHealthy() throws IOException {
        @SuppressWarnings("unchecked")
        Map<String, Object> dependsOn = (Map<String, Object>) service("app-grants").get("depends_on");
        assertThat(dependsOn).as("app-grants 가 아무것도 안 기다립니다").isNotNull();

        @SuppressWarnings("unchecked")
        Map<String, Object> api = (Map<String, Object>) dependsOn.get("api");
        assertThat(api).as("app-grants 가 api 를 안 기다리면 Flyway 와 경쟁합니다").isNotNull();
        assertThat(api.get("condition"))
                .as("service_started 는 프로세스 시작만 뜻합니다 — Flyway 는 그 뒤에 돕니다")
                .isEqualTo("service_healthy");
    }

    @Test
    @DisplayName("api 에 healthcheck 가 있다 — 없으면 위의 service_healthy 가 영영 안 풀린다")
    void apiDeclaresAHealthcheck() throws IOException {
        @SuppressWarnings("unchecked")
        Map<String, Object> healthcheck = (Map<String, Object>) service("api").get("healthcheck");
        assertThat(healthcheck).as("api 에 healthcheck 가 없으면 compose 가 아예 안 뜹니다").isNotNull();
        assertThat(healthcheck.get("test").toString())
                .as("컨텍스트가 떴는지만 보면 된다 — health 는 Redis·Kafka 상태를 합산해서"
                        + " 그것들이 없는 회차에 api 가 영영 healthy 가 안 된다")
                .contains("/actuator/metrics");
    }

    /**
     * <b>healthcheck 가 두드리는 포트는 앱이 실제로 여는 포트여야 한다.</b>
     *
     * <p>첫 판은 {@code MANAGEMENT_SERVER_PORT} 를 썼는데 <b>그 이름은 죽어 있었다</b> —
     * {@code application.yml} 과 {@code management.yml} 이 같은 키를 서로 다른 변수로
     * 적고 있었고, {@code spring.config.import} 로 들어온 문서가 이긴다(실측: 두 변수를
     * 9391·9392 로 주고 컨텍스트를 띄우니 {@code management.server.port} 가 9391 이었다).
     * 기본값이 양쪽 다 9090 이라 <b>기본 배포에서는 증상이 없다</b> — 포트를 옮긴 환경에서만
     * api 가 영영 healthy 가 안 되고 권한 적용이 통째로 멈춘다. 그래서 눈으로는 안 잡힌다.
     */
    @Test
    @DisplayName("healthcheck 의 포트 변수가 앱이 읽는 그 이름이다 — 기본값이 같아 눈으로는 안 잡힌다")
    void theHealthcheckUsesThePortVariableTheAppActuallyReads() throws IOException {
        @SuppressWarnings("unchecked")
        Map<String, Object> healthcheck = (Map<String, Object>) service("api").get("healthcheck");

        String appConfig = Files.readString(Path.of("..", "api", "src", "main", "resources",
                "management.yml.example"));
        String declared = appConfig.lines()
                .filter(line -> line.contains("port: ${"))
                .map(line -> line.substring(line.indexOf("${") + 2, line.indexOf(':', line.indexOf("${"))))
                .findFirst()
                .orElseThrow(() -> new AssertionError("management.yml.example 에 관리 포트 선언이 없습니다"));

        // `$$` 여야 한다. `$` 하나면 compose 가 호스트에서 치환하는데, api 가 실제로 쓰는
        // 값은 env_file 로 컨테이너에 주입된 것이라 출처가 다르다 — 셸에 같은 이름이
        // export 돼 있거나 --env-file 로 다른 파일을 주면 엇갈린다.
        assertThat(healthcheck.get("test").toString())
                .as("앱은 %s 를 읽는데 healthcheck 가 다른 이름을 쓰거나 호스트에서 미리"
                        + " 치환되면, api 가 떠 있어도 healthy 가 안 되고 app-grants 가 안 돕니다",
                        declared)
                .contains("$${" + declared + ":-");
    }
}
