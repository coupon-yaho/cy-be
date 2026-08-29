package com.kafkick.batch.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;

import com.kafkick.storage.db.MySqlContainerConfig;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * 이력 엔드포인트를 HTTP 로 잰다.
 *
 * <p>변환은 BatchRunViewTest 가 따로 잰다. 여기서 재려는 것은 컨트롤러의 배선이다 —
 * 페이지 클램프와 필터가 쿼리 파라미터로 실제로 통하는지.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "batch.metrics.expire-pending-initial-delay-ms=3600000",
        "batch.metrics.run-refresh-ms=120000",
        "management.server.port=0"
})
@Import(MySqlContainerConfig.class)
class BatchHistoryApiTest {

    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    @LocalServerPort
    private int port;

    @Test
    @DisplayName("큰 limit 은 상한으로 잘린다 — 안 자르면 화면 실수 하나가 DB 를 오래 잡는다")
    void clampsLimitToMaximum() {
        assertThat(page("/api/v1/admin/batch/runs?limit=999999").path("limit").asInt())
                .isEqualTo(HistoryPage.MAX_LIMIT);
        assertThat(page("/api/v1/admin/verify/runs?limit=999999").path("limit").asInt())
                .isEqualTo(HistoryPage.MAX_LIMIT);
    }

    @Test
    @DisplayName("limit 을 안 주거나 0 이하면 기본값이다")
    void fallsBackToDefaultLimit() {
        assertThat(page("/api/v1/admin/batch/runs").path("limit").asInt())
                .isEqualTo(HistoryPage.DEFAULT_LIMIT);
        assertThat(page("/api/v1/admin/batch/runs?limit=0").path("limit").asInt())
                .isEqualTo(HistoryPage.DEFAULT_LIMIT);
    }

    @Test
    @DisplayName("음수 offset 은 0 으로 — SQL 에 그대로 가면 문법 오류다")
    void clampsNegativeOffset() {
        assertThat(page("/api/v1/admin/batch/runs?offset=-5").path("offset").asInt()).isZero();
    }

    @Test
    @DisplayName("필터가 쿼리 파라미터로 통한다 — 없는 잡을 물으면 빈 목록이다")
    void appliesFilters() {
        JsonNode noSuchJob = page("/api/v1/admin/batch/runs?jobName=noSuchJob");

        assertThat(noSuchJob.path("items")).isEmpty();
        assertThat(noSuchJob.path("total").asInt()).isZero();
        assertThat(page("/api/v1/admin/verify/runs?dataset=CORRUPT").path("total").asInt())
                .isNotNegative();
    }

    @Test
    @DisplayName("잘못된 dataset 은 400 이다 — 500 이면 규약 밖 코드가 나간다")
    void rejectsUnknownDataset() {
        assertThat(status("/api/v1/admin/verify/runs?dataset=NOPE")).isEqualTo(400);
    }

    /** 봉투를 벗겨 data 를 준다. 저장소 규약이 모든 응답을 ResponseEnvelope 로 감싼다. */
    private JsonNode page(String path) {
        HttpResponse<String> response = send(path);
        assertThat(response.statusCode()).isEqualTo(200);
        return MAPPER.readTree(response.body()).path("data");
    }

    private int status(String path) {
        return send(path).statusCode();
    }

    private HttpResponse<String> send(String path) {
        try {
            return CLIENT.send(HttpRequest.newBuilder(
                            URI.create("http://127.0.0.1:" + port + path))
                    .GET().timeout(Duration.ofSeconds(10)).build(),
                    HttpResponse.BodyHandlers.ofString());
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
