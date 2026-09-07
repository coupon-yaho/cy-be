package com.kafkick.batch.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.LocalDateTime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.batch.config.RunningJobFixture;
import com.kafkick.batch.job.CleanupJobConfig;
import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>범용 관제의 쓰기 동작 둘을 잰다 — 재시작과 중단.</b>
 *
 * <h2>가드를 새로 짜지 않았다는 것이 이 테스트의 요점이다</h2>
 *
 * <p>{@code JobOperator} 시그니처를 실측하니 위험한 전이를 이미 거절한다. 그래서
 * {@link BatchControlController} 가 하는 일은 <b>그 거절을 HTTP 로 옮기는 것</b>뿐이고,
 * 여기서 재는 것도 <b>옮겨진 뒤의 모양</b>이다 — 상태코드와 에러코드가 사람이 읽을 수
 * 있게 갈리는지.
 *
 * <p><b>없는 실행이 500 이 아니라 404 인 것이 특히 중요하다.</b>
 * {@code JobRepository.getJobExecution} 은 없는 id 에 <b>{@code EmptyResultDataAccessException}
 * 을 던진다</b>(실측 — 처음에는 {@code null} 을 돌려준다고 적어 뒀다가 틀렸다).
 * 그것을 안 잡으면 화면은 500 을 보고 <b>"없는 실행" 과 "서버가 깨졌다" 를 구분하지
 * 못한다.</b>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        // 되읽기가 같은 배치 메타를 훑는다. 손으로 심은 행과 섞이지 않게 창을 닫는다.
        "batch.metrics.expire-pending-initial-delay-ms=3600000",
        "batch.metrics.run-refresh-ms=120000",
        "server.port=0",
        "management.server.port=0"
})
@Import(MySqlContainerConfig.class)
class BatchControlApiTest {

    /** {@code batch.stuck-job-after-ms} 기본이 30분이다. 넉넉히 넘긴다. */
    private static final Duration DEAD = Duration.ofHours(2);

    @LocalServerPort
    private int port;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JdbcClient jdbcClient;

    private VerifyApiProbe api;

    @AfterEach
    void tearDown() {
        new JobRepositoryTestUtils(jobRepository).removeJobExecutions();
    }

    private VerifyApiProbe api() {
        if (api == null) {
            api = new VerifyApiProbe(port);
        }
        return api;
    }

    /** 테스트마다 다른 키를 쓴다 — {@code JOB_INST_UN} 이 (잡 이름, 파라미터 키)에 걸려 있다. */
    private static LocalDateTime key(int slot) {
        return LocalDateTime.of(2026, 7, 1, 0, 0).plusHours(slot);
    }

    /**
     * <b>없는 실행은 404 다.</b> 이 갈래가 없으면 {@code getJobExecution} 이 준
     * {@code null} 이 그대로 흘러 {@code NullPointerException} → 500 이 된다.
     */
    @Test
    @DisplayName("없는 실행에 재시작을 걸면 404 와 BATCH-001 이다")
    void restartingAnUnknownExecutionIsNotFound() throws Exception {
        var response = api().post("/api/v1/admin/batch/runs/999999999/restart");

        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(VerifyApiProbe.json(response).path("error").path("code").asText())
                .isEqualTo("BATCH-001");
    }

    @Test
    @DisplayName("없는 실행에 중단을 걸어도 404 와 BATCH-001 이다")
    void stoppingAnUnknownExecutionIsNotFound() throws Exception {
        var response = api().post("/api/v1/admin/batch/runs/999999999/stop");

        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(VerifyApiProbe.json(response).path("error").path("code").asText())
                .isEqualTo("BATCH-001");
    }

    /**
     * <b>죽은 실행에도 신호가 받아들여진다 — 그리고 영영 안 멈춘다.</b>
     *
     * <p>처음에 이 테스트는 409 를 기대했다. <b>틀렸다</b> — 실측하니
     * {@code SimpleJobOperator.stop} 의 관문이 {@code STARTED}·{@code STARTING} 두 상태라
     * ({@code isRunning()} 이 아니다 — 그것은 {@code STOPPING} 에도 참인데 stop 은 그것을
     * 거절한다) {@code JobOperator} 가 거절하지 않고
     * {@code true} 를 돌려준다. 신호는 DB 에 적히지만 <b>그것을 읽을 프로세스가 없다.</b>
     *
     * <p>그래서 이 테스트는 <b>내가 바라는 동작이 아니라 실제 동작</b>을 못 박는다. 그리고
     * 응답이 {@code status} 를 실어, 사람이 <b>오래된 {@code STARTED} 를 눌렀다</b>는 것을
     * 알 수 있게 한다 — 그때 필요한 것은 중단이 아니라 회수다.
     */
    @Test
    @DisplayName("죽었지만 STARTED 인 실행은 신호를 받아들인다 — 상태가 그 사실을 말한다")
    void stoppingADeadButStartedExecutionIsAcceptedButNeverStops() throws Exception {
        try (RunningJobFixture dead = RunningJobFixture.plant(
                jobRepository, jdbcClient, CleanupJobConfig.JOB_NAME, key(1), DEAD, DEAD)) {

            var response = api().post(
                    "/api/v1/admin/batch/runs/" + dead.executionId() + "/stop");

            assertThat(response.statusCode()).isEqualTo(200);
            var data = VerifyApiProbe.json(response).path("data");
            assertThat(data.path("signalled").asBoolean())
                    .as("프레임워크가 거절하지 않는다 — 이것이 실측한 동작이다").isTrue();
            assertThat(data.path("status").asText())
                    .as("이 값이 없으면 '눌렀는데 안 멈춘다' 의 이유를 사람이 못 찾는다")
                    .isEqualTo("STARTED");
        }
    }

    /**
     * <b>이미 끝난 실행은 못 멈춘다.</b> 여기가 {@code BATCH-004} 가 실제로 뜨는 자리다 —
     * 위의 시체가 아니라.
     */
    @Test
    @DisplayName("이미 끝난 실행을 멈추려 하면 409 와 BATCH-004 다")
    void stoppingAFinishedExecutionIsRefused() throws Exception {
        try (RunningJobFixture dead = RunningJobFixture.plant(
                jobRepository, jdbcClient, CleanupJobConfig.JOB_NAME, key(2), DEAD, DEAD)) {
            // 끝난 상태로 만든다. 회수 경로가 하는 일과 같다.
            jdbcClient.sql("UPDATE BATCH_JOB_EXECUTION SET STATUS='FAILED', EXIT_CODE='FAILED',"
                            + " END_TIME=CURRENT_TIMESTAMP(6) WHERE JOB_EXECUTION_ID=:id")
                    .param("id", dead.executionId()).update();

            var response = api().post(
                    "/api/v1/admin/batch/runs/" + dead.executionId() + "/stop");

            assertThat(response.statusCode()).isEqualTo(409);
            assertThat(VerifyApiProbe.json(response).path("error").path("code").asText())
                    .isEqualTo("BATCH-004");
        }
    }

    /**
     * <b>성공한 실행은 다시 못 돌린다 — 이것이 재시작의 핵심 안전장치다.</b>
     *
     * <p>막지 않으면 같은 일이 두 번 처리된다. 같은 조건으로 또 돌리고 싶으면 재시작이
     * 아니라 <b>다른 파라미터로 새 인스턴스</b>이고, 무엇이 인스턴스를 가르는지는
     * 파라미터 화면의 {@code identifying} 이 말한다(CY-911).
     */
    @Test
    @DisplayName("성공한 실행을 다시 돌리려 하면 409 와 BATCH-002 다")
    void restartingACompletedExecutionIsRefused() throws Exception {
        try (RunningJobFixture done = RunningJobFixture.plant(
                jobRepository, jdbcClient, CleanupJobConfig.JOB_NAME, key(3), DEAD, DEAD)) {
            jdbcClient.sql("UPDATE BATCH_JOB_EXECUTION SET STATUS='COMPLETED',"
                            + " EXIT_CODE='COMPLETED', END_TIME=CURRENT_TIMESTAMP(6)"
                            + " WHERE JOB_EXECUTION_ID=:id")
                    .param("id", done.executionId()).update();

            var response = api().post(
                    "/api/v1/admin/batch/runs/" + done.executionId() + "/restart");

            assertThat(response.statusCode()).isEqualTo(409);
            assertThat(VerifyApiProbe.json(response).path("error").path("code").asText())
                    .as("성공한 실행을 다시 돌리면 같은 일이 두 번 처리된다")
                    .isEqualTo("BATCH-002");
        }
    }

    /**
     * <b>사람이 멈춘 잡은 다시 돌릴 수 있어야 한다.</b>
     *
     * <p>첫 판의 가드가 {@code isUnsuccessful() || isRunning()} 이었고 <b>틀렸다.</b>
     * {@code BatchStatus} 를 실측하니 {@code STOPPED} 는 둘 다 {@code false} 다 —
     * 그래서 방금 중단한 잡을 다시 못 돌렸고, 그것도 <b>"이미 성공했다"(BATCH-002)는
     * 거짓 이유</b>로 거절했다. 중단 기능을 쓴 사람이 바로 다음에 막히는 셈이라 최악이다.
     * 리뷰가 잡았다.
     *
     * <pre>
     *   running=false unsuccessful=false : COMPLETED · STOPPED   ← 둘을 가려야 한다
     *   running=true                     : STARTING · STARTED · STOPPING
     *   unsuccessful=true                : FAILED · ABANDONED · UNKNOWN
     * </pre>
     *
     * <p><b>여기서 재는 것은 "우리가 막지 않는다" 하나다.</b> 실제 재시작이 성공하는지는
     * 잡 정의와 스텝 상태에 달렸고 그 판정은 프레임워크가 한다 — 심어 둔 픽스처는 진짜
     * 스텝 이력이 아니라서, 여기서 성공까지 요구하면 <b>프레임워크를 재는 테스트</b>가 된다.
     * 그래서 단언이 {@code isNotEqualTo("BATCH-002")} 인 것이 <b>의도</b>다.
     */
    @Test
    @DisplayName("STOPPED 실행을 우리가 BATCH-002 로 막지 않는다 — 성공까지는 프레임워크 몫")
    void restartingAStoppedExecutionIsNotRefusedAsCompleted() throws Exception {
        try (RunningJobFixture stopped = RunningJobFixture.plant(
                jobRepository, jdbcClient, CleanupJobConfig.JOB_NAME, key(4), DEAD, DEAD)) {
            jdbcClient.sql("UPDATE BATCH_JOB_EXECUTION SET STATUS='STOPPED', EXIT_CODE='STOPPED',"
                            + " END_TIME=CURRENT_TIMESTAMP(6) WHERE JOB_EXECUTION_ID=:id")
                    .param("id", stopped.executionId()).update();

            var response = api().post(
                    "/api/v1/admin/batch/runs/" + stopped.executionId() + "/restart");

            // **500 을 통과시키면 안 된다.** isNotEqualTo("BATCH-002") 하나만 보면
            // 서버가 깨져도, 응답이 비어도 초록이다 — 리뷰가 짚었다.
            assertThat(response.statusCode())
                    .as("우리 판정을 지났으면 프레임워크가 답한다. 500 은 그 둘 다 아니다")
                    .isIn(200, 409);
            assertThat(VerifyApiProbe.json(response).path("error").path("code").asText())
                    .as("중단 기능을 쓴 사람이 바로 다음에 '이미 성공했다' 로 막히면 안 된다")
                    .isNotEqualTo("BATCH-002");
        }
    }

    /**
     * <b>이 관제는 잡을 모른다.</b> 경로에 잡 이름이 없고, 실행 id 하나로만 판단한다 —
     * 도메인이 바뀌어도 새 잡이 그날부터 이 통로를 쓴다.
     *
     * <p>그것을 <b>없는 실행 id 로</b> 확인한다. 잡 이름을 받는 설계였다면 이 요청은
     * 경로 자체가 성립하지 않았을 것이다.
     */
    @Test
    @DisplayName("경로에 잡 이름이 없다 — 실행 id 하나로 돈다")
    void theControlPathDoesNotNameAnyJob() throws Exception {
        assertThat(api().post("/api/v1/admin/batch/runs/1/stop").statusCode())
                .as("경로가 성립해야 한다. 404 는 실행이 없다는 뜻이지 경로가 없다는 뜻이 아니다")
                .isEqualTo(404);
    }

    /**
     * <b>중단 신호만 남고 프로세스가 죽은 실행을 걷는다.</b>
     *
     * <p>{@code stop} 은 도는 잡이 있으면 {@code STOPPING} 을 남기고 청크 경계에서
     * {@code STOPPED} 로 가지만, <b>하드킬로 잡이 이미 없으면 그 자리에 머문다.</b>
     * 그 행이 이 API 의 주 사용처다.
     */
    @Test
    @DisplayName("STOPPING 에 멈춘 실행을 버린다 — 신호를 읽을 프로세스가 없는 경우다")
    void anExecutionStrandedInStoppingIsAbandoned() throws Exception {
        try (RunningJobFixture dead = RunningJobFixture.plant(
                jobRepository, jdbcClient, CleanupJobConfig.JOB_NAME, key(7), DEAD, DEAD)) {
            jdbcClient.sql("UPDATE BATCH_JOB_EXECUTION SET STATUS='STOPPING'"
                            + " WHERE JOB_EXECUTION_ID=:id")
                    .param("id", dead.executionId()).update();

            var response = api().post(
                    "/api/v1/admin/batch/runs/" + dead.executionId() + "/abandon");

            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(VerifyApiProbe.json(response).path("data").path("status").asText())
                    .isEqualTo("ABANDONED");
        }
    }

    /**
     * <b>{@code STOPPED} 도 받아야 한다.</b> 안 받으면 {@code stop} 은 됐는데 버리기가
     * 실패한 실행이 <b>범용 경로로는 끝낼 수 없게</b> 된다 — 리뷰가 짚은 자리다.
     */
    @Test
    @DisplayName("이미 STOPPED 인 실행도 버릴 수 있다 — 중간에 실패해도 이어서 끝낸다")
    void anExecutionAlreadyStoppedCanStillBeAbandoned() throws Exception {
        try (RunningJobFixture stopped = RunningJobFixture.plant(
                jobRepository, jdbcClient, CleanupJobConfig.JOB_NAME, key(8), DEAD, DEAD)) {
            jdbcClient.sql("UPDATE BATCH_JOB_EXECUTION SET STATUS='STOPPED',"
                            + " EXIT_CODE='STOPPED' WHERE JOB_EXECUTION_ID=:id")
                    .param("id", stopped.executionId()).update();

            var response = api().post(
                    "/api/v1/admin/batch/runs/" + stopped.executionId() + "/abandon");

            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(VerifyApiProbe.json(response).path("data").path("status").asText())
                    .isEqualTo("ABANDONED");
        }
    }

    /**
     * <b>도는 실행은 먼저 {@code stop} 이다.</b> 여기서 바로 버리면 살아 있는 잡을 걷는다.
     *
     * <p>{@code stop} 을 이 안에서 이어 부르지 않는 이유이기도 하다 — 그러면 이 거절이
     * 사라지고, {@code stop} 만 성공한 중간 상태가 생긴다.
     */
    @Test
    @DisplayName("도는 실행은 버리기를 409 와 BATCH-005 로 거절한다 — 먼저 stop 이다")
    void aRunningExecutionMustBeStoppedFirst() throws Exception {
        try (RunningJobFixture running = RunningJobFixture.plant(
                jobRepository, jdbcClient, CleanupJobConfig.JOB_NAME, key(9), DEAD, DEAD)) {

            var response = api().post(
                    "/api/v1/admin/batch/runs/" + running.executionId() + "/abandon");

            assertThat(response.statusCode()).isEqualTo(409);
            assertThat(VerifyApiProbe.json(response).path("error").path("code").asText())
                    .isEqualTo("BATCH-005");
        }
    }

    /**
     * <b>끝난 실행을 막는 것이 핵심이다.</b> 프레임워크는 {@code isLessThan(STOPPING)} 일
     * 때만 거부하므로 {@code FAILED} 는 통과한다 — 그대로 두면 <b>실패 이력을
     * {@code ABANDONED} 로 덮고 {@code END_TIME} 을 현재로 다시 쓴다.</b> 이 저장소는
     * 실행 이력을 판정 근거로 삼는다(docs/11).
     */
    @Test
    @DisplayName("실패로 끝난 실행은 버리기를 거절한다 — 증거를 덮으면 안 된다")
    void aFailedExecutionIsNotOverwritten() throws Exception {
        try (RunningJobFixture failed = RunningJobFixture.plant(
                jobRepository, jdbcClient, CleanupJobConfig.JOB_NAME, key(10), DEAD, DEAD)) {
            jdbcClient.sql("UPDATE BATCH_JOB_EXECUTION SET STATUS='FAILED', EXIT_CODE='FAILED',"
                            + " END_TIME=CURRENT_TIMESTAMP(6) WHERE JOB_EXECUTION_ID=:id")
                    .param("id", failed.executionId()).update();

            var response = api().post(
                    "/api/v1/admin/batch/runs/" + failed.executionId() + "/abandon");

            assertThat(response.statusCode()).isEqualTo(409);
            assertThat(VerifyApiProbe.json(response).path("error").path("code").asText())
                    .isEqualTo("BATCH-005");
            assertThat(jdbcClient.sql("SELECT STATUS FROM BATCH_JOB_EXECUTION"
                            + " WHERE JOB_EXECUTION_ID=:id")
                    .param("id", failed.executionId()).query(String.class).single())
                    .as("거절했는데 상태가 바뀌었으면 증거를 덮은 것이다")
                    .isEqualTo("FAILED");
        }
    }

    /** 없는 실행은 500 이 아니라 404 다 — 화면이 "없다" 와 "깨졌다" 를 갈라야 한다. */
    @Test
    @DisplayName("없는 실행을 버리려 하면 404 다")
    void abandoningAnAbsentExecutionIsNotFound() throws Exception {
        var response = api().post("/api/v1/admin/batch/runs/999999999/abandon");

        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(VerifyApiProbe.json(response).path("error").path("code").asText())
                .isEqualTo("BATCH-001");
    }
}
