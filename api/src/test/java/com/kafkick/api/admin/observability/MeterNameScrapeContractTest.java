package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.io.InputStream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.observation.http.HttpMetrics;
import com.kafkick.api.observation.http.HttpMetricsFilter;
import com.kafkick.api.observation.http.InFlightRegistry;
import com.kafkick.api.observation.http.ResultClassifier;

/**
 * 조회하는 쪽이 기대하는 지표 이름이 <b>실제 scrape 출력에 있는지</b> 봅니다.
 *
 * <p>{@link MetricAggregation} 의 상수는 Micrometer 미터 이름을 점→밑줄로 바꾸고 접미사를 붙여
 * 만듭니다. 그 변환 규칙이나 접미사가 어긋나면 <b>예외 없이 값이 비고 앱은 정상 기동합니다</b> —
 * 로그도 안 남고 화면만 빕니다. 조립기 테스트는 만든 표본을 정한 이름으로 넣으므로 이 어긋남을
 * 영원히 못 잡습니다.</p>
 *
 * <p>그래서 실제 HTTP scrape 를 긁어 이름을 대조합니다. {@code PrometheusExposureContractTest} 가
 * allowlist 와 백분위 설정을 보는 것과 같은 방식이고, 보는 대상만 다릅니다.</p>
 */
@SpringBootTest(
        classes = MeterNameScrapeContractTest.TestApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.config.location=file:build/obs6-meter-names/management.yml,"
                        + "file:build/obs6-meter-names/observation.yml",
                "spring.autoconfigure.exclude="
                        + "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration,"
                        + "org.springframework.boot.hibernate.autoconfigure."
                        + "HibernateJpaAutoConfiguration",
                "management.server.port=0",
                "management.endpoint.health.validate-group-membership=false"})
class MeterNameScrapeContractTest {

    private static final Path STAGE = Path.of("build/obs6-meter-names");

    @LocalManagementPort
    int managementPort;

    @LocalServerPort
    int appPort;

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

    /**
     * api 가 직접 등록하는 미터의 이름이 조회 쪽 상수와 같은지 봅니다.
     *
     * <p>{@code HttpMetrics} 는 기동 시점에 uri 그룹 × 결과 분류를 전부 등록하므로 요청이 하나도
     * 없어도 이름은 나옵니다. 백분위({@code quantile} 라벨)는 표본이 있어야 나오므로 검사하지
     * 않습니다 — 그건 {@code PrometheusExposureContractTest} 가 봅니다.</p>
     */
    @Test
    @DisplayName("조회 쪽이 기대하는 이름이 실제 scrape 출력에 있다")
    void scrapeCarriesTheNamesTheQuerySideExpects() throws Exception {
        call(appPort, "/obs6-meter-name-probe");
        String scrape = call(managementPort, "/actuator/prometheus").body();

        assertThat(scrape)
                .as("이름이 어긋나면 예외 없이 값만 비고 앱은 정상 기동한다 — 화면만 빈다")
                .contains(MetricAggregation.HTTP_RESULT_TOTAL)
                .contains(MetricAggregation.HTTP_LATENCY_SECONDS)
                .contains(MetricAggregation.HTTP_IN_FLIGHT)
                .contains(MetricAggregation.CPU_USAGE)
                .contains(MetricAggregation.JVM_MEMORY_USED);
    }

    /**
     * 지연 질의는 {@code app_http_latency_seconds{quantile!=""}} 입니다. 백분위가 이 이름에
     * {@code quantile} 라벨로 붙어 나오지 않으면 그 질의는 영원히 빈 결과를 돌려줍니다.
     */
    @Test
    @DisplayName("지연 미터가 quantile 라벨을 달고 나온다 — 지연 질의가 기대하는 형태다")
    void latencyMeterCarriesQuantileLabels() throws Exception {
        call(appPort, "/obs6-meter-name-probe");
        String scrape = call(managementPort, "/actuator/prometheus").body();

        assertThat(scrape.lines()
                .filter(line -> line.startsWith(MetricAggregation.HTTP_LATENCY_SECONDS + "{"))
                .filter(line -> line.contains("quantile=\""))
                .toList())
                .as("백분위가 안 나오면 지연 패널이 영원히 PENDING 이다")
                .isNotEmpty();
    }

    /** 조립기가 거는 라벨도 실제 출력에 있어야 필터가 표본을 골라낼 수 있다. */
    @Test
    @DisplayName("조립기가 거는 uri_group·result·outcome 라벨이 실제로 붙어 나온다")
    void scrapeCarriesTheLabelsTheQuerySideFiltersOn() throws Exception {
        call(appPort, "/obs6-meter-name-probe");
        String scrape = call(managementPort, "/actuator/prometheus").body();

        assertThat(scrape)
                .as("라벨 이름이 어긋나면 필터가 아무것도 못 고른다 — 결과는 이름이 틀렸을 때와 같다")
                .contains("uri_group=\"issue\"")
                .contains("result=\"success\"")
                .contains("outcome=\"success\"");
    }

    private static HttpResponse<String> call(int port, String path) throws Exception {
        return HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    /**
     * 컴포넌트 스캔을 켜지 않고 필요한 빈만 넣는다. 스캔을 켜면 이 모듈의 모든 설정이 딸려 와
     * 이 테스트가 무엇을 검증하는지 흐려지고, 다른 테스트 컨텍스트까지 오염된다.
     */
    @SpringBootConfiguration
    @EnableAutoConfiguration
    @RestController
    @Import({HttpMetrics.class, InFlightRegistry.class, ResultClassifier.class,
            HttpMetricsFilter.class})
    static class TestApp {

        @GetMapping("/obs6-meter-name-probe")
        String probe() {
            return "ok";
        }
    }
}
