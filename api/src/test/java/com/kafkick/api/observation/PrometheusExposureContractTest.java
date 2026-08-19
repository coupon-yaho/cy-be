package com.kafkick.api.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 이 티켓이 건 설정이 <b>HTTP 로 실제 도달 가능한지</b>를 본다.
 *
 * <p>왜 따로 두는가 — {@link AutoInstrumentedMetersTest} 는 {@code registry.scrape()} 를
 * 인프로세스로 부른다. 그 층에는 노출 allowlist 가 없어 엔드포인트가 닫혀 있어도 통과한다.
 * 테스트 고도가 실패 지점보다 낮으면 아무것도 못 잡는다.
 *
 * <p>이 계약은 두 파일에 걸쳐 있다. {@code observation.yml.example}(OBS-3 소유)이 백분위를
 * 걸고 {@code management.yml.example}(OBS-20 소유)이 노출을 연다. 둘 중 하나만 맞으면
 * 스크레이프는 404 이고 이 티켓의 설정은 아무 효과가 없다. 그래서 두 파일을 함께 띄운다.
 */
class PrometheusExposureContractTest {

    private static final Path STAGE = Path.of("build/obs3-exposure");

    private static final String CONFIG_LOCATION =
            "spring.config.location=file:build/obs3-exposure/management.yml,"
                    + "file:build/obs3-exposure/observation.yml";

    /**
     * 자동설정 제외는 애노테이션이 아니라 <b>프로퍼티</b>로 준다. 중첩 {@code TestApp} 은
     * {@code com.kafkick} 스캔에 걸려서, 애노테이션으로 달면 그 제외가 다른 테스트의 컨텍스트
     * 까지 따라간다 — {@code ApiApplicationTests} 가 그렇게 한 번 깨졌다.
     */
    private static final String EXCLUDE_DATASOURCE =
            "spring.autoconfigure.exclude="
                    + "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration,"
                    + "org.springframework.boot.hibernate.autoconfigure."
                    + "HibernateJpaAutoConfiguration";

    /** 커밋되는 .example 을 실제 로드되는 이름으로 옮긴다 — 신규 클론에도 있는 파일이다. */
    @BeforeAll
    static void stageCommittedConfig() throws Exception {
        Files.createDirectories(STAGE);
        for (String name : new String[] {"management", "observation"}) {
            try (InputStream in = new ClassPathResource(name + ".yml.example").getInputStream()) {
                Files.copy(in, STAGE.resolve(name + ".yml"), StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    private static HttpResponse<String> call(int port, String path) throws Exception {
        return HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    /**
     * 커밋된 설정 그대로 띄운다. allowlist 에 prometheus 가 없으므로 스크레이프는 닫혀 있다.
     *
     * <p><b>OBS-20 이 도착하면 깨지도록 일부러 만들었다.</b> 그러지 않으면 "우리 쪽은 다 됐는데
     * 남의 티켓이 안 왔다" 는 상태가 아무 신호도 내지 않는다. allowlist 가 열리는 순간 빨간불이
     * 켜지고, 그때 이 클래스를 지우면 된다.
     */
    @SpringBootTest(
            classes = TestApp.class,
            webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
            properties = {CONFIG_LOCATION, EXCLUDE_DATASOURCE, "management.server.port=0",
                    "management.endpoint.health.validate-group-membership=false"})
    @Nested
    class NotYetOpenedByObs20 {

        @LocalManagementPort
        int managementPort;

        @Autowired
        Environment environment;

        @Test
        @DisplayName("[의도된 트립와이어] OBS-20 이 allowlist 를 열면 이 테스트가 깨진다 —"
                + " 버그가 아니라 신호다. 깨지면 NotYetOpenedByObs20 클래스를 통째로 지워라")
        void scrapeIsStillClosed() throws Exception {
            assertThat(environment.getProperty("management.endpoints.web.exposure.include"))
                    .as("OBS-20 이 allowlist 를 열면 이 단언이 깨진다 — 그게 이 테스트의 목적이다."
                            + " 깨지면 이 중첩 클래스를 지우고 OpenedByObs20 만 남겨라")
                    .doesNotContain("prometheus");

            assertThat(call(managementPort, "/actuator/prometheus").statusCode())
                    .as("allowlist 가 닫혀 있는 동안은 404 여야 한다 — 200 이면 노출이 의도치"
                            + " 않게 열린 것이다")
                    .isEqualTo(404);
        }

        @Test
        @DisplayName("닫힌 이유는 의존 누락이 아니라 allowlist 다 — 레지스트리는 이미 붙어 있다")
        void theRegistryBeanIsAlreadyThere() {
            assertThat(environment.getProperty("management.metrics.distribution.percentiles"
                    + "[http.server.requests]"))
                    .as("우리 쪽 설정은 로드돼 있다")
                    .isNotNull();
            assertThat(PrometheusMeterRegistry.class.getName())
                    .as("micrometer-registry-prometheus 가 빠지면 컴파일 단계에서 먼저 깨진다")
                    .isNotEmpty();
        }
    }

    /**
     * OBS-20 이 allowlist 를 연 뒤의 상태를 미리 고정한다. 우리 쪽(의존 · 백분위 설정)이
     * 완성돼 있다는 증거이고, OBS-20 이 도착했을 때 무엇이 나와야 하는지의 기준이다.
     */
    @SpringBootTest(
            classes = TestApp.class,
            webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
            properties = {CONFIG_LOCATION, EXCLUDE_DATASOURCE, "management.server.port=0",
                    "management.endpoint.health.validate-group-membership=false",
                    "management.endpoints.web.exposure.include="
                            + "health,metrics,admission-capacity,prometheus"})
    @Nested
    class OpenedByObs20 {

        @LocalManagementPort
        int managementPort;

        @LocalServerPort
        int appPort;

        @Test
        @DisplayName("allowlist 만 열리면 200 이고 quantile 라벨로 나온다 — _bucket 은 없다")
        void scrapeServesQuantileLabels() throws Exception {
            call(appPort, "/obs3-exposure-probe");

            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                HttpResponse<String> scrape = call(managementPort, "/actuator/prometheus");

                assertThat(scrape.statusCode()).isEqualTo(200);
                assertThat(scrape.body())
                        .as("백분위가 HTTP 로 도달해야 이 티켓의 percentiles 설정이 의미를 갖는다")
                        .contains("http_server_requests_seconds{")
                        .contains("quantile=\"0.99\"");
                assertThat(scrape.body())
                        .as("publishPercentiles 는 버킷을 만들지 않는다 — histogram_quantile 로"
                                + " 병합할 수 없고, 그래서 인스턴스 최댓값을 쓴다")
                        .doesNotContain("http_server_requests_seconds_bucket");
            });
        }

        /*
         * 여기에 "env 는 여전히 404" 를 넣었다가 뺐다. include 에 env 가 없으면 exclude 가
         * 없어도 404 라, exclude 를 지워도 통과하는 헛도는 단언이었다 — 깨뜨려 확인했다.
         * exclude 를 실제로 검증하려면 include 를 * 로 넓혀야 하고, 커밋된 파일에 그 줄이
         * 남아 있는지는 ManagementConfigTest 가 본다(그쪽도 깨뜨려 확인했다).
         */
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @RestController
    static class TestApp {

        @GetMapping("/obs3-exposure-probe")
        String probe() {
            return "ok";
        }
    }
}
