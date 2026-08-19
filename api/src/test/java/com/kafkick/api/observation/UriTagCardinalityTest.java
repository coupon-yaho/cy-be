package com.kafkick.api.observation;

import static com.kafkick.testsupport.CommittedConfigStager.stage;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * {@code http_server_requests_seconds} 의 {@code uri} 라벨이 <b>경로 템플릿</b>으로 나오는지
 * 본다. P-1(CY-213)의 cardinality 규칙 중 하나다.
 *
 * <p>왜 이게 인프라 티켓의 일인가 — 라벨 하나가 시계열 수를 그 라벨의 값 개수만큼 곱한다.
 * {@code uri} 가 템플릿이 아니라 실제 id 로 나오면 쿠폰 수(300만)만큼 시계열이 생기고,
 * 1초 간격으로 긁는 Prometheus 가 먼저 죽는다. 그런데 이건 {@code prometheus.yml} 로는 못
 * 막는다 — 라벨을 만드는 쪽이 앱이기 때문이다. 그래서 여기서 본다.
 *
 * <p><b>검사가 둘인 이유.</b> 하나는 런타임(라벨이 실제로 어떻게 찍히는지), 하나는 정적
 * (실제 컨트롤러의 매핑이 어떻게 생겼는지)이다. 어느 한쪽만으로는 못 잡는다:
 * <ul>
 *   <li>런타임 검사는 이 테스트가 띄운 프로브 컨트롤러만 본다. 진짜 컨트롤러가
 *       {@code /coupons/**} 처럼 매핑돼 있어도 프로브는 멀쩡하다
 *   <li>정적 검사는 매핑 문자열만 본다. 누가 {@code ObservationConvention} 이나
 *       {@code MeterFilter} 로 {@code uri} 를 다시 쓰면 매핑은 그대로인데 라벨만 터진다
 * </ul>
 *
 * <p><b>잡지 못하는 것.</b> 쿼리스트링·헤더에서 오는 다른 고카디널리티 라벨은 범위 밖이다.
 * 그리고 이 저장소에는 아직 사용자 경로 컨트롤러가 없다 — 지금 정적 검사가 보는 것은 admin
 * 컨트롤러뿐이다. 발급 API 가 생기면 자동으로 검사 범위에 들어온다(경로를 지정하지 않고
 * {@code api/src/main/java} 전체를 훑기 때문이다).
 */
class UriTagCardinalityTest {

    /**
     * 런타임 검사용 설정. 스크레이프가 열려 있어야 라벨을 볼 수 있고, 그 allowlist 는
     * {@code management.yml.example}(CY-246 이 prometheus 를 열었다)에 있다.
     */
    private static final String CONFIG_LOCATION =
            "spring.config.location=file:build/cy213-uri-tag/management.yml,"
                    + "file:build/cy213-uri-tag/observation.yml";

    /**
     * 자동설정 제외는 애노테이션이 아니라 프로퍼티로 준다 — 중첩 {@code TestApp} 이
     * {@code com.kafkick} 스캔에 걸려서, 애노테이션으로 달면 그 제외가 다른 테스트의
     * 컨텍스트까지 따라간다.
     */
    private static final String EXCLUDE_DATASOURCE =
            "spring.autoconfigure.exclude="
                    + "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration,"
                    + "org.springframework.boot.hibernate.autoconfigure."
                    + "HibernateJpaAutoConfiguration";

    @BeforeAll
    static void stageCommittedConfig() throws Exception {
        stage(Path.of("build/cy213-uri-tag/management.yml"), "management.yml.example");
        stage(Path.of("build/cy213-uri-tag/observation.yml"), "observation.yml.example");
    }

    // ── 런타임 ────────────────────────────────────────────────────────────────

    @SpringBootTest(
            classes = TestApp.class,
            webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
            properties = {CONFIG_LOCATION, EXCLUDE_DATASOURCE, "management.server.port=0",
                    "management.endpoint.health.validate-group-membership=false"})
    @Nested
    class LabelAsActuallyEmitted {

        @LocalManagementPort
        int managementPort;

        @LocalServerPort
        int appPort;

        private HttpResponse<String> call(int port, String path) throws Exception {
            return HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).build(),
                    HttpResponse.BodyHandlers.ofString());
        }

        @Test
        @DisplayName("서로 다른 id 를 세 번 불러도 시계열은 하나다 — uri 는 템플릿으로 찍힌다")
        void distinctIdsCollapseIntoOneTemplatedSeries() throws Exception {
            for (String id : List.of("1", "77", "30000000")) {
                call(appPort, "/cy213-uri-probe/" + id);
            }

            String scrape = call(managementPort, "/actuator/prometheus").body();

            assertThat(scrape)
                    .as("템플릿으로 찍혀야 id 가 몇 개든 시계열이 하나다")
                    .contains("uri=\"/cy213-uri-probe/{id}\"");
            assertThat(scrape)
                    .as("실제 id 가 라벨에 들어가면 쿠폰 수만큼 시계열이 생긴다 — 300만 개다."
                            + " ObservationConvention · MeterFilter 로 uri 를 다시 쓰면 여기서"
                            + " 깨진다")
                    .doesNotContain("uri=\"/cy213-uri-probe/30000000\"");

            long series = scrape.lines()
                    .filter(line -> line.startsWith("http_server_requests_seconds_count"))
                    .filter(line -> line.contains("/cy213-uri-probe/"))
                    .count();
            assertThat(series)
                    .as("호출 3번이 시계열 3개가 되면 안 된다")
                    .isEqualTo(1);
        }
    }

    // ── 정적 ──────────────────────────────────────────────────────────────────

    /** {@code @GetMapping("...")} 처럼 애노테이션에 적힌 경로 문자열. */
    private static final Pattern MAPPING_PATH =
            Pattern.compile("@(?:Request|Get|Post|Put|Patch|Delete)Mapping\\(\\s*(?:value\\s*=\\s*)?\"([^\"]*)\"");

    @Test
    @DisplayName("실제 컨트롤러 매핑에 와일드카드가 없다 — {} 가 아니면 uri 가 원본 경로로 찍힌다")
    void noControllerMappingUsesAWildcardSegment() throws Exception {
        List<String> offenders = new java.util.ArrayList<>();

        for (Path source : javaSourcesUnder(moduleRoot().resolve("src/main/java"))) {
            Matcher matcher = MAPPING_PATH.matcher(Files.readString(source));
            while (matcher.find()) {
                String path = matcher.group(1);
                // ** 와 * 는 여러 경로를 하나의 매핑으로 받는다. 그러면 uri 라벨이 템플릿이
                // 아니라 요청된 경로 그대로 찍혀 id 마다 시계열이 생긴다.
                if (path.contains("*")) {
                    offenders.add(source.getFileName() + " → " + path);
                }
            }
        }

        assertThat(offenders)
                .as("와일드카드 매핑은 uri 라벨을 폭발시킨다. 경로 변수({id})로 바꿔라")
                .isEmpty();
    }

    private List<Path> javaSourcesUnder(Path root) throws Exception {
        try (Stream<Path> files = Files.walk(root)) {
            return files.filter(path -> path.toString().endsWith(".java")).toList();
        }
    }

    /**
     * 소스는 클래스패스가 아니라 파일시스템에서 읽는다. Gradle(모듈 디렉터리)과 IDE(저장소
     * 루트) 양쪽을 받아 주고, 어느 쪽도 아니면 skip 이 아니라 실패한다 — skip 하면 아무것도
     * 검사하지 않으면서 초록불이 된다.
     */
    private Path moduleRoot() {
        Path working = Path.of("").toAbsolutePath();
        for (Path candidate : List.of(working, working.resolve("api"))) {
            if (Files.isDirectory(candidate.resolve("src/main/java"))) {
                return candidate;
            }
        }
        throw new IllegalStateException("api 모듈 루트를 찾지 못했다. 실행 디렉터리: " + working);
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @RestController
    static class TestApp {

        @GetMapping("/cy213-uri-probe/{id}")
        String probe(@PathVariable String id) {
            return id;
        }
    }
}
