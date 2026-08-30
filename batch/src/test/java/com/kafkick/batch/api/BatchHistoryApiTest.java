package com.kafkick.batch.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDateTime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.storage.db.MySqlContainerConfig;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * 이력 엔드포인트를 HTTP 로 잰다.
 *
 * <p>변환은 BatchRunViewTest 가 따로 잰다. 여기서 재려는 것은 컨트롤러의 배선이다 —
 * 페이지 클램프와 필터가 쿼리 파라미터로 실제로 통하는지.
 *
 * <p><b>필터는 심어 놓고 재야 한다.</b> 빈 DB 에 "없는 잡을 물으면 빈 목록" 을 물으면
 * 필터를 <b>통째로 무시해도</b> 통과한다 — 원래 아무것도 없기 때문이다. 그래서 두 잡을
 * 심고 <b>하나만 걸러 나오는지</b>를 본다. 검증 쪽도 CLEAN·CORRUPT 를 한 건씩 심어
 * <b>안 고른 쪽이 빠졌는지</b>까지 본다.
 *
 * <p>이름과 asOf 에 이 클래스 전용 값을 쓴다. 컨테이너를 다른 테스트와 나눠 쓰므로
 * 남의 행이 섞일 수 있는데, 전용 값이면 total 을 정확한 수로 단언할 수 있다.
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

    private static final String ONLY_JOB = "apiHistoryProbeJob";
    private static final String OTHER_JOB = "apiHistoryOtherJob";

    /**
     * <b>스프링 배치의 시퀀스가 절대 못 닿는 자리에 심는다.</b> 배치 메타 id 는
     * {@code BATCH_JOB_SEQ} 가 발급하는데, 여기서 낮은 번호를 손으로 넣으면 <b>같은 컨테이너를
     * 쓰는 다른 테스트가 실제로 잡을 띄울 때 PK 가 충돌</b>한다 — 시퀀스는 내가 심은 것을
     * 모른다. 실제로 CleanupRecoveryTest 가 그 자리에서 죽었다.
     */
    private static final long PROBE_ID = 900_000_001L;

    /** 다른 테스트의 검증 실행과 안 겹치는 창. uk_run_params 가 (as_of, dataset, scope, attempt) 다. */
    private static final LocalDateTime AS_OF = LocalDateTime.of(2031, 3, 3, 3, 3, 3);

    @LocalServerPort
    private int port;

    @Autowired
    private JdbcClient jdbcClient;

    @AfterEach
    void removeHistory() {
        // 안 지우면 미래(2031) as_of 행이 컨테이너 수명 내내 남는다. 지금은
        // findLatestClosed 가 finished_at IS NOT NULL 로 걸러 무해한 것을 확인했지만,
        // "가장 최근 실행" 을 그 조건 없이 읽는 코드가 하나라도 생기면 그 테스트만
        // 알 수 없는 이유로 깨진다.
        deleteBatchRun(PROBE_ID);
        deleteBatchRun(PROBE_ID + 1);
        deleteVerifyRun("CLEAN");
        deleteVerifyRun("CORRUPT");
    }

    @BeforeEach
    void plantHistory() {
        plantBatchRun(PROBE_ID, ONLY_JOB);
        plantBatchRun(PROBE_ID + 1, OTHER_JOB);
        plantVerifyRun("CLEAN", "PASS");
        plantVerifyRun("CORRUPT", "FAIL");
    }

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
    @DisplayName("jobName 이 심어 놓은 둘 중 하나만 고른다 — 필터를 무시해도 둘 다 나오면 안 된다")
    void filtersBatchRunsByJobName() {
        JsonNode filtered = page("/api/v1/admin/batch/runs?jobName=" + ONLY_JOB);

        assertThat(filtered.path("items")).isNotEmpty();
        assertThat(filtered.path("items")).allSatisfy(item ->
                assertThat(item.path("jobName").asString()).isEqualTo(ONLY_JOB));
        assertThat(jobNames("/api/v1/admin/batch/runs?limit=200"))
                .as("필터 없이 부르면 둘 다 나와야 한다 — 안 그러면 위 단언은 필터가 아니라 "
                        + "심기가 실패한 것을 보고 있다")
                .contains(ONLY_JOB, OTHER_JOB);
    }

    @Test
    @DisplayName("없는 잡은 빈 목록 — 심어 둔 행이 있는 상태에서 재야 뜻이 있다")
    void returnsEmptyForUnknownJobName() {
        JsonNode noSuchJob = page("/api/v1/admin/batch/runs?jobName=noSuchJob");

        assertThat(noSuchJob.path("items")).isEmpty();
        assertThat(noSuchJob.path("total").asInt()).isZero();
    }

    @Test
    @DisplayName("dataset 이 CLEAN 을 빼고 CORRUPT 만 준다")
    void filtersVerifyRunsByDataset() {
        JsonNode corrupt = page("/api/v1/admin/verify/runs?dataset=CORRUPT&limit=200");

        assertThat(corrupt.path("items")).isNotEmpty();
        assertThat(corrupt.path("items")).allSatisfy(item ->
                assertThat(item.path("dataset").asString()).isEqualTo("CORRUPT"));
        assertThat(asOfValues(corrupt))
                .as("이 클래스가 심은 CORRUPT 행이 그 안에 있어야 한다")
                .contains(AS_OF.toString());
        JsonNode clean = page("/api/v1/admin/verify/runs?dataset=CLEAN&limit=200");

        // 두 행이 **같은 asOf 를 공유**하므로 asOf 만 봐서는 어느 쪽인지 못 가른다 —
        // dataset 을 뒤바꾸는 오류는 asOf 단언만으로 안 잡힌다.
        assertThat(clean.path("items")).allSatisfy(item ->
                assertThat(item.path("dataset").asString()).isEqualTo("CLEAN"));
        assertThat(asOfValues(clean))
                .as("같은 asOf 로 CLEAN 도 심었으므로, 두 필터가 서로 다른 행을 골라야 한다")
                .contains(AS_OF.toString());
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

    private java.util.List<String> jobNames(String path) {
        return page(path).path("items").valueStream()
                .map(item -> item.path("jobName").asString()).toList();
    }

    private java.util.List<String> asOfValues(JsonNode envelope) {
        return envelope.path("items").valueStream()
                .map(item -> item.path("asOf").asString()).toList();
    }

    /**
     * 배치 메타에 직접 심는다. 잡을 띄우면 이름·상태를 원하는 모양으로 못 만들고,
     * 이 테스트는 스케줄러를 꺼 둔 채 뜬다.
     *
     * <p>BeforeEach 라 클래스 안에서만도 여러 번 돈다 — 고정 id 를 쓰므로 먼저 지운다.
     */
    private void plantBatchRun(long id, String jobName) {
        deleteBatchRun(id);
        jdbcClient.sql("""
                INSERT INTO BATCH_JOB_INSTANCE (JOB_INSTANCE_ID, VERSION, JOB_NAME, JOB_KEY)
                VALUES (:id, 0, :jobName, :key)
                """).param("id", id).param("jobName", jobName)
                .param("key", jobName + id).update();
        jdbcClient.sql("""
                INSERT INTO BATCH_JOB_EXECUTION
                    (JOB_EXECUTION_ID, VERSION, JOB_INSTANCE_ID, CREATE_TIME, START_TIME,
                     END_TIME, STATUS, EXIT_CODE, EXIT_MESSAGE)
                VALUES (:id, 0, :id, :now, :now, :end, 'COMPLETED', 'COMPLETED', '')
                """).param("id", id)
                // **NOW() 를 안 쓴다.** 컨테이너 세션 존이 UTC 라 NOW() 는 UTC 벽시계를
                // 주는데, BatchRunView 는 그 값을 **JVM 기본 존 벽시계로 보고** 다시 UTC 로
                // 옮긴다 — KST JVM 이면 응답이 9시간 이르다. 지금 이 클래스는 시각을
                // 단언하지 않아 안 깨지지만, 여기서 축을 재려는 다음 사람이 틀린 기대값을
                // 박는다. 형제 BatchRunHistoryTest.plant 가 이미 자바 쪽 값을 바인딩한다.
                .param("now", LocalDateTime.now())
                .param("end", LocalDateTime.now().plusSeconds(3)).update();
    }

    private void deleteBatchRun(long id) {
        jdbcClient.sql("DELETE FROM BATCH_JOB_EXECUTION WHERE JOB_EXECUTION_ID = :id")
                .param("id", id).update();
        jdbcClient.sql("DELETE FROM BATCH_JOB_INSTANCE WHERE JOB_INSTANCE_ID = :id")
                .param("id", id).update();
    }

    private void deleteVerifyRun(String dataset) {
        jdbcClient.sql("""
                DELETE FROM verification_runs
                 WHERE as_of = :asOf AND dataset = :dataset AND scope = 'FULL' AND attempt = 1
                """).param("asOf", AS_OF).param("dataset", dataset).update();
    }

    /**
     * uk_run_params 가 {@code (as_of, dataset, scope, attempt)} 라 dataset 만 갈라도 둘이
     * 공존한다. 대신 <b>같은 키를 두 번 심으면 두 번째 테스트에서 죽으므로</b> 먼저 지운다 —
     * BeforeEach 라 클래스 안에서만도 다섯 번 돈다.
     */
    private void plantVerifyRun(String dataset, String verdict) {
        deleteVerifyRun(dataset);
        jdbcClient.sql("""
                INSERT INTO verification_runs
                    (as_of, scope, dataset, attempt, verdict, finding_count, started_at)
                VALUES (:asOf, 'FULL', :dataset, 1, :verdict, 0, :asOf)
                """).param("asOf", AS_OF).param("dataset", dataset)
                .param("verdict", verdict).update();
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
