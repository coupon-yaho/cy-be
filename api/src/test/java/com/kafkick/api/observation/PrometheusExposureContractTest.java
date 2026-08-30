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
import java.time.Instant;
import java.util.List;

import com.kafkick.core.member.Grade;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.EventRecorder;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.IssuanceFlowEventFactory;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
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

    private static void assertMetricSample(
            String scrape,
            String metricName,
            double expectedValue,
            String... expectedTags
    ) {
        List<String> samples = scrape.lines()
                .filter(line -> line.startsWith(metricName + "{") || line.startsWith(metricName + " "))
                .filter(line -> containsTags(line, expectedTags))
                .toList();

        assertThat(samples).as(metricName).hasSize(1);
        String sample = samples.getFirst();
        assertThat(Double.parseDouble(sample.substring(sample.lastIndexOf(' ') + 1)))
                .isEqualTo(expectedValue);
    }

    private static boolean containsTags(String sample, String... expectedTags) {
        for (int index = 0; index < expectedTags.length; index += 2) {
            if (!sample.contains(expectedTags[index] + "=\"" + expectedTags[index + 1] + "\"")) {
                return false;
            }
        }
        return true;
    }

    /** 커밋된 OBS-20 allowlist와 OBS-3 백분위 설정을 함께 로드한다. */
    @SpringBootTest(
            classes = TestApp.class,
            webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
            properties = {CONFIG_LOCATION, EXCLUDE_DATASOURCE, "management.server.port=0",
                    "management.endpoint.health.validate-group-membership=false"})
    @Nested
    class PrometheusIncluded {

        @LocalManagementPort
        int managementPort;

        @LocalServerPort
        int appPort;

        @Autowired
        EventRecorder eventRecorder;

        @Autowired
        IssuanceFlowEventFactory eventFactory;

        @Test
        @DisplayName("커밋된 allowlist로 200 이고 quantile 라벨로 나온다 — _bucket 은 없다")
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

        @Test
        @DisplayName("쿠폰 회차 발급 미터 6종이 이름 · 라벨 · 값 그대로 scrape 에 실린다")
        void scrapeServesCouponRoundIssuanceMeters() {
            IssuanceFlowEvent.Ctx context = new IssuanceFlowEvent.Ctx(
                    "obs25-exposure", 101L, 202L, Grade.GOLD, false,
                    Instant.parse("2026-08-23T00:00:00Z"), EngineVersion.V3, ReleaseStage.V3,
                    QueueMode.ADAPTIVE, 901L, "api-1"
            );
            eventRecorder.record(eventFactory.issueAttempt(context));
            eventRecorder.record(eventFactory.admitted(context, 7L));
            eventRecorder.record(eventFactory.issued(context, 1L, "coupon-code-0001"));

            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                HttpResponse<String> scrape = call(managementPort, "/actuator/prometheus");

                assertThat(scrape.statusCode()).isEqualTo(200);
                assertMetricSample(scrape.body(), "app_issuance_flow_total", 1.0,
                        "coupon_id", "202", "stage", "attempt");
                assertMetricSample(scrape.body(), "app_issuance_flow_total", 1.0,
                        "coupon_id", "202", "stage", "success");
                assertMetricSample(scrape.body(), "app_queue_admitted_total", 1.0,
                        "coupon_id", "202");
                assertMetricSample(scrape.body(), "app_issuance_outcome_total", 1.0,
                        "outcome", "ISSUED");
                assertMetricSample(scrape.body(), "app_issuance_event_last_success_epoch", 1_787_443_200d,
                        "coupon_id", "202");
                assertMetricSample(scrape.body(), "app_queue_event_last_admitted_epoch", 1_787_443_200d,
                        "coupon_id", "202");
                assertMetricSample(scrape.body(), "app_observation_coupon_round_limit_exceeded_total", 0.0);
            });
        }

        /*
         * 여기에 "env 는 여전히 404" 를 넣었다가 뺐다. include 에 env 가 없으면 exclude 가
         * 없어도 404 라, exclude 를 지워도 통과하는 헛도는 단언이었다 — 깨뜨려 확인했다.
         * exclude 를 실제로 검증하려면 include 를 * 로 넓혀야 하고, 커밋된 파일에 그 줄이
         * 남아 있는지는 ManagementConfigTest 가 본다(그쪽도 깨뜨려 확인했다).
         */
    }

    /** registry가 있어도 allowlist에서 제외하면 HTTP scrape가 닫히는지 검증한다. */
    @SpringBootTest(
            classes = TestApp.class,
            webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
            properties = {CONFIG_LOCATION, EXCLUDE_DATASOURCE, "management.server.port=0",
                    "management.endpoint.health.validate-group-membership=false",
                    "management.endpoints.web.exposure.include="
                            + "health,metrics,admission-capacity"})
    @Nested
    class PrometheusExcluded {

        @LocalManagementPort
        int managementPort;

        @Autowired
        PrometheusMeterRegistry registry;

        @Test
        @DisplayName("registry가 있어도 allowlist에서 제외하면 prometheus는 404다")
        void scrapeIsClosedByAllowlist() throws Exception {
            assertThat(registry).isNotNull();
            assertThat(call(managementPort, "/actuator/prometheus").statusCode()).isEqualTo(404);
        }
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
