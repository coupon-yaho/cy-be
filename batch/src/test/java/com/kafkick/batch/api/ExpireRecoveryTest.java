// 종료 표시를 못 남기고 죽은 만료 실행을 API 로 걷어내는 경로를 확인합니다.
package com.kafkick.batch.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import com.kafkick.batch.config.ExpireStepContext;
import com.kafkick.batch.config.RunningJobFixture;
import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>알림이 우는데 처방이 손 SQL 뿐이었다.</b>
 *
 * <p>{@code BatchStuckExecution} 은 <i>"몇 건 있다"</i> 까지만 말한다. 그다음 절차가
 * {@code docs/13} 의 {@code UPDATE BATCH_JOB_EXECUTION ...} 이었는데, 그 SQL 은
 * 살아 있는 실행에 쓰면 다음 {@code jobRepository.update()} 를 터뜨린다 — 새벽에 깨어난
 * 사람에게 그 판단을 맡기고 있었다.
 *
 * <p>여기서 재는 것은 <b>그 판단을 코드가 지는가</b>다.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        // 되읽기가 같은 배치 메타를 훑는다. 손으로 심은 행과 섞이지 않게 창을 닫는다 —
        // 근거는 ExpirePendingRefresher 의 @Scheduled 주석에 적혀 있다.
        "batch.metrics.expire-pending-initial-delay-ms=3600000",
        "batch.metrics.run-refresh-ms=120000",
        "server.port=0",
        "management.server.port=0"
})
@Import(MySqlContainerConfig.class)
class ExpireRecoveryTest {

    /** {@code batch.stuck-job-after-ms} 기본이 30분이다. 넉넉히 넘긴다. */
    private static final Duration DEAD = Duration.ofHours(2);

    @LocalServerPort
    private int port;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private RequestMappingHandlerMapping mappings;

    @Autowired
    private ExpireRecoveryService recovery;

    @Autowired
    private com.kafkick.batch.config.RunningJobProbe runningJobs;

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

    /**
     * 테스트마다 다른 키를 쓴다 — {@code JOB_INST_UN} 이 (잡 이름, 파라미터 키)에 걸려 있어
     * 정리가 한 번 실패하면 그 뒤 전부가 <b>심기 단계</b>에서 깨지고, 원인이 안 드러난다.
     */
    private static LocalDateTime key(int slot) {
        // 슬롯이 28 을 넘어도 되게 시(時)로 넓힌다 — 일(日)로만 만들면 32 에서 죽는다.
        return LocalDateTime.of(2026, 5, 1, 0, 0).plusHours(slot);
    }

    /**
     * <b>이 클래스의 핵심 단언.</b> 진도가 도는 실행은 못 걷어낸다.
     *
     * <p>손 SQL 이 임계를 직접 적어야 했던 자리다 — 그 숫자가 코드와 갈리면 운영자가
     * <b>살아 있는 만료를 걷어낸다.</b> 만료는 재고를 쓰는 유일한 잡이라, 중간에 끊기면
     * 그 뒤 검증의 판정 근거가 흔들린다.
     */
    @Test
    @DisplayName("진도가 도는 실행은 409 로 거절하고 배치 메타를 안 건드린다")
    void refusesToRecoverALiveRun() throws Exception {
        try (RunningJobFixture alive = RunningJobFixture.plant(
                jobRepository, jdbcClient, ExpireStepContext.JOB_NAME, key(1),
                Duration.ofHours(3), Duration.ofSeconds(5))) {

            assertThat(VerifyApiProbe.json(api().get("/api/v1/admin/expire/runs/stuck"))
                    .path("data"))
                    .as("세 시간째 돌지만 방금 진도를 냈다 — 나이로 자르면 여기서 잡힌다")
                    .isEmpty();

            long versionBefore = version(alive.executionId());

            var response = api().post(
                    "/api/v1/admin/expire/runs/" + alive.executionId() + "/recover");

            assertThat(response.statusCode()).isEqualTo(409);
            assertThat(VerifyApiProbe.error(response).path("code").asString())
                    .isEqualTo("EXPIRATION-007");
            assertThat(jobRepository.getJobExecution(alive.executionId()).getStatus())
                    .isEqualTo(BatchStatus.STARTED);
            // **이 API 의 안전 근거가 정확히 이 한 줄이다.** 손 SQL 과 같은 쓰기를 하되
            // 시체 판정을 통과한 실행에만 한다 — 거절이 VERSION 을 올리면 살아 있는
            // 만료의 다음 update() 가 OptimisticLockingFailureException 으로 터진다.
            assertThat(version(alive.executionId()))
                    .as("거절이 배치 메타를 건드리면 살아 있는 실행이 죽는다")
                    .isEqualTo(versionBefore);
        }
    }

    /**
     * <b>시체는 목록에 나오고 한 번에 걷힌다.</b> {@code recover} 가 Step 행까지
     * {@code FAILED} 로 닫는다 — {@code docs/13} 이 <i>"{@code STARTED} 로 남는다"</i> 고
     * 적어 뒀던 찌꺼기가 없어진다.
     *
     * <p><b>{@code ABANDONED} 가 아니라 {@code FAILED} 인 것이 계약이다.</b>
     * {@code ABANDONED} 면 그 {@code JobInstance} 를 같은 {@code asOf} 로 영원히 못 돌린다.
     */
    @Test
    @DisplayName("시체는 목록에 나오고 한 번에 FAILED 로 닫힌다")
    void listsAndRecoversAStuckRun() throws Exception {
        try (RunningJobFixture dead = RunningJobFixture.plant(
                jobRepository, jdbcClient, ExpireStepContext.JOB_NAME, key(2), DEAD, DEAD)) {

            var listed = VerifyApiProbe.json(api().get("/api/v1/admin/expire/runs/stuck"))
                    .path("data");
            assertThat(listed).hasSize(1);
            assertThat(listed.get(0).path("executionId").asLong()).isEqualTo(dead.executionId());
            assertThat(listed.get(0).path("stalledSeconds").asLong())
                    .as("정말 죽었는지를 사람이 판단할 재료다 — 없으면 손 SQL 로 되돌아간다")
                    .isGreaterThan(DEAD.toSeconds() - 60);
            assertThat(listed.get(0).path("startTime").isNull())
                    .as("언제 시작한 실행인지도 함께 봐야 한다")
                    .isFalse();

            var recovered = api().post(
                    "/api/v1/admin/expire/runs/" + dead.executionId() + "/recover");
            assertThat(recovered.statusCode()).isEqualTo(200);
            assertThat(VerifyApiProbe.json(recovered).path("data").path("status").asString())
                    .as("ABANDONED 면 그 asOf 슬롯을 영원히 못 돌린다")
                    .isEqualTo("FAILED");

            var closed = jobRepository.getJobExecution(dead.executionId());
            assertThat(closed.getStatus()).isEqualTo(BatchStatus.FAILED);
            assertThat(closed.getEndTime()).isNotNull();
            assertThat(closed.getStepExecutions())
                    .as("Step 행도 함께 닫혀야 한다 — 손 SQL 은 그것을 STARTED 로 남겼다")
                    .allSatisfy(step -> assertThat(step.getStatus().isRunning()).isFalse());

            assertThat(VerifyApiProbe.json(api().get("/api/v1/admin/expire/runs/stuck"))
                    .path("data"))
                    .as("걷어냈으면 BatchStuckExecution 이 보는 축도 함께 내려가야 한다")
                    .isEmpty();
        }
    }

    /**
     * <b>복구 API 가 재시도에서 에러를 내면 안 된다.</b> {@code curl} 이 응답 직전에
     * 끊기는 일은 흔하고, 그때 운영자가 <b>같은 실행을 두 번 걷어내려다 다른 실행을
     * 건드린다.</b> 한때 2단계({@code stop → abandon})였을 때 두 번째 호출이 서로 다른
     * 이유로 409 를 내고 그 문구가 상황과 반대로 나갔다 — 그것이 {@code recover} 로
     * 갈아탄 이유 중 하나다.
     */
    @Test
    @DisplayName("이미 걷어낸 실행은 다시 불러도 200 이고 이력을 다시 안 쓴다")
    void recoveryIsIdempotent() throws Exception {
        try (RunningJobFixture dead = RunningJobFixture.plant(
                jobRepository, jdbcClient, ExpireStepContext.JOB_NAME, key(3), DEAD, DEAD)) {

            var first = api().post(
                    "/api/v1/admin/expire/runs/" + dead.executionId() + "/recover");
            assertThat(first.statusCode()).isEqualTo(200);
            assertThat(VerifyApiProbe.json(first).path("data").path("alreadyRecovered")
                    .asBoolean()).isFalse();

            long versionAfterFirst = version(dead.executionId());

            var again = api().post(
                    "/api/v1/admin/expire/runs/" + dead.executionId() + "/recover");

            // **이 단언이 코드의 순서를 못 박는다.** 시체 판정을 먼저 하면 recover 뒤
            // FAILED 가 된 실행이 시체 목록에서 빠져 재시도가 자기 성공에 막힌다 —
            // 2단계 abandon 을 버린 이유였는데 같은 모양을 다시 만들 뻔했다.
            // batch.recovered 를 컨트롤러가 직접 영속하지 않아도 여기가 빨개진다.
            assertThat(again.statusCode())
                    .as("409 면 운영자가 첫 호출이 실패했다고 읽고 손 SQL 로 되돌아간다")
                    .isEqualTo(200);
            assertThat(VerifyApiProbe.json(again).path("data").path("alreadyRecovered")
                    .asBoolean()).isTrue();
            assertThat(VerifyApiProbe.json(again).path("data").path("status").asString())
                    .isEqualTo("FAILED");
            assertThat(version(dead.executionId()))
                    .as("두 번째 호출이 END_TIME 을 사람이 curl 을 친 시각으로 밀면 안 된다")
                    .isEqualTo(versionAfterFirst);
        }
    }

    /**
     * <b>남의 잡과 없는 실행을 같은 404 로 접는다.</b> 인증이 없는 API 라, 남의 잡이라고
     * 알려 주면 배치 메타의 실행 번호 공간을 훑는 수단이 된다.
     */
    @Test
    @DisplayName("검증 실행과 없는 번호를 같은 404 로 접는다")
    void hidesForeignAndMissingRuns() throws Exception {
        try (RunningJobFixture verify = RunningJobFixture.plant(
                jobRepository, jdbcClient, "verifyJob", key(4), DEAD, DEAD)) {

            var foreign = api().post(
                    "/api/v1/admin/expire/runs/" + verify.executionId() + "/recover");
            var missing = api().post("/api/v1/admin/expire/runs/999999/recover");

            assertThat(foreign.statusCode()).isEqualTo(404);
            assertThat(missing.statusCode()).isEqualTo(404);
            assertThat(VerifyApiProbe.error(foreign).path("code").asString())
                    .isEqualTo(VerifyApiProbe.error(missing).path("code").asString())
                    .isEqualTo("EXPIRATION-006");

            assertThat(VerifyApiProbe.json(api().get("/api/v1/admin/expire/runs/stuck"))
                    .path("data"))
                    .as("만료 목록에 검증 시체가 섞이면 운영자가 남의 잡을 걷어낸다")
                    .isEmpty();
        }
    }

    /**
     * <b>Step 이 하나도 없는 실행도 걷힌다.</b> {@code beforeJob} 가드나 커넥션 획득에서
     * 죽으면 {@code BATCH_STEP_EXECUTION} 행이 없다. 그때 판정은 {@code START_TIME} 으로
     * 떨어지는데, <b>표시가 그 폴백을 안 따라가면 판정과 다른 컬럼을 보여 준다</b> —
     * 실제로 그렇게 썼다가 리뷰가 잡았고, 그래서 계산을 프로브 한 곳으로 모았다.
     */
    @Test
    @DisplayName("Step 이 없는 실행도 START_TIME 기준으로 잡힌다")
    void handlesRunsWithoutAnyStep() throws Exception {
        try (RunningJobFixture dead = RunningJobFixture.plantWithoutStep(
                jobRepository, ExpireStepContext.JOB_NAME, key(5),
                LocalDateTime.now().minus(DEAD))) {

            var listed = VerifyApiProbe.json(api().get("/api/v1/admin/expire/runs/stuck"))
                    .path("data");
            assertThat(listed).hasSize(1);
            assertThat(listed.get(0).path("lastProgress").asString())
                    .as("판정은 START_TIME 으로 떨어지는데 표시가 CREATE_TIME 이면 둘이 갈린다")
                    .isEqualTo(listed.get(0).path("startTime").asString());
            assertThat(listed.get(0).path("stalledSeconds").asLong())
                    .isGreaterThan(DEAD.toSeconds() - 60);

            assertThat(api().post("/api/v1/admin/expire/runs/"
                    + dead.executionId() + "/recover").statusCode()).isEqualTo(200);
        }
    }

    /**
     * <b>{@code START_TIME} 마저 없는 행.</b> {@code STARTING} 에서 죽으면 Step 도 시작
     * 시각도 없다. {@code RunningJobProbe.lastProgress} 의 마지막 폴백
     * ({@code CREATE_TIME})이 그 갈래를 진다 — <b>그것이 없으면 이런 행이 영원히 가드를
     * 켜 둔다.</b> 위 테스트만으로는 그 폴백을 지워도 초록이다.
     */
    @Test
    @DisplayName("START_TIME 마저 없으면 CREATE_TIME 으로 떨어진다")
    void fallsBackToCreateTimeWhenNotEvenStarted() throws Exception {
        try (RunningJobFixture dead = RunningJobFixture.plantWithoutStep(
                jobRepository, ExpireStepContext.JOB_NAME, key(6), null)) {

            jdbcClient.sql("UPDATE BATCH_JOB_EXECUTION SET CREATE_TIME = "
                            + "DATE_SUB(CREATE_TIME, INTERVAL :s SECOND) "
                            + "WHERE JOB_EXECUTION_ID = :id")
                    .param("s", DEAD.toSeconds()).param("id", dead.executionId()).update();

            var listed = VerifyApiProbe.json(api().get("/api/v1/admin/expire/runs/stuck"))
                    .path("data");
            assertThat(listed).hasSize(1);
            assertThat(listed.get(0).path("startTime").isNull()).isTrue();
            assertThat(listed.get(0).path("lastProgress").asString())
                    .isEqualTo(listed.get(0).path("createTime").asString());
        }
    }

    /**
     * <b>시체가 하나 있다고 아무 만료나 걷어내면 안 된다.</b>
     *
     * <p>시체 판정을 <i>"목록이 비어 있지 않은가"</i> 로 느슨하게 써도 다른 테스트는 전부
     * 초록이다 — 살아 있는 만료와 죽은 만료가 <b>동시에</b> 있는 케이스가 없었기 때문이다.
     * 그 변이체는 <i>"만료 시체가 하나라도 남아 있으면 아무 만료나 걷어낼 수 있다"</i> 이고,
     * 만료는 재고를 쓰는 유일한 잡이라 그것이 이 API 를 만든 이유 전부를 뒤집는다.
     */
    @Test
    @DisplayName("시체가 따로 있어도 살아 있는 실행은 못 걷어낸다")
    void refusesALiveRunEvenWhenAnotherCorpseExists() throws Exception {
        try (RunningJobFixture dead = RunningJobFixture.plant(
                jobRepository, jdbcClient, ExpireStepContext.JOB_NAME, key(7), DEAD, DEAD);
             RunningJobFixture alive = RunningJobFixture.plant(
                jobRepository, jdbcClient, ExpireStepContext.JOB_NAME, key(8),
                Duration.ofHours(3), Duration.ofSeconds(5))) {

            assertThat(VerifyApiProbe.json(api().get("/api/v1/admin/expire/runs/stuck"))
                    .path("data"))
                    .as("전제 — 목록에 시체가 하나 있다")
                    .hasSize(1);

            long before = version(alive.executionId());
            var response = api().post(
                    "/api/v1/admin/expire/runs/" + alive.executionId() + "/recover");

            assertThat(response.statusCode()).isEqualTo(409);
            assertThat(version(alive.executionId()))
                    .as("살아 있는 만료의 배치 메타를 건드리면 그 실행이 다음 update() 에서 죽는다")
                    .isEqualTo(before);
            assertThat(jobRepository.getJobExecution(dead.executionId()).getStatus())
                    .as("남의 실행도 건드리면 안 된다")
                    .isEqualTo(BatchStatus.STARTED);
        }
    }

    /**
     * <b>같은 실행에 동시 요청 둘이 들어와도 한 번만 쓴다.</b>
     *
     * <p><b>낙관적 락에 기대면 안 된다.</b> {@code update(JobExecution)} 은 쓰기 직전에
     * {@code synchronizeStatus} 로 자기 버전을 DB 값에 다시 맞추므로 스테일 버전으로 안
     * 터진다(6.0.4 바이트코드). 버전 검사가 걸리는 것은 {@code update(StepExecution)} 뿐이라
     * <b>Step 행이 없는 실행에는 검사가 한 개도 없다</b> — 그래서 여기서
     * {@code plantWithoutStep} 을 쓴다. 상호배제는 조건부 갱신의 {@code affected rows} 다.
     */
    @Test
    @DisplayName("Step 이 없는 실행에 동시 요청 둘이 와도 END_TIME 을 한 번만 쓴다")
    void concurrentRecoveriesWriteOnce() throws Exception {
        try (RunningJobFixture dead = RunningJobFixture.plantWithoutStep(
                jobRepository, ExpireStepContext.JOB_NAME, key(9),
                LocalDateTime.now().minus(DEAD))) {

            long before = version(dead.executionId());
            String path = "/api/v1/admin/expire/runs/" + dead.executionId() + "/recover";

            var pool = java.util.concurrent.Executors.newFixedThreadPool(2);
            var start = new java.util.concurrent.CountDownLatch(1);
            var results = new java.util.concurrent.CopyOnWriteArrayList<Boolean>();
            // Future 를 모아 get() 으로 다시 던진다. 안 받으면 워커의 예외와 단언 실패가
            // Future 안에 갇혀, 남는 신호가 results 크기 불일치 하나뿐이라 원인이 안 보인다.
            var futures = new java.util.ArrayList<java.util.concurrent.Future<Void>>();
            for (int i = 0; i < 2; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    var res = api().post(path);
                    assertThat(res.statusCode()).isEqualTo(200);
                    results.add(VerifyApiProbe.json(res).path("data")
                            .path("alreadyRecovered").asBoolean());
                    return null;
                }));
            }
            start.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            for (var future : futures) {
                future.get();
            }

            assertThat(results).as("둘 다 200 이어야 한다").hasSize(2);
            assertThat(results.stream().filter(already -> !already).count())
                    .as("실제로 쓴 요청은 정확히 하나여야 한다")
                    .isEqualTo(1);
            assertThat(version(dead.executionId()) - before)
                    .as("두 번 쓰면 END_TIME 이 나중 요청의 시각으로 덮인다")
                    .isEqualTo(2);
            assertThat(jobRepository.getJobExecution(dead.executionId()).getStatus())
                    .isEqualTo(BatchStatus.FAILED);
        }
    }

    /**
     * <b>선점문의 {@code STATUS} 목록을 못 박는다.</b>
     *
     * <p>그 목록은 {@code findRunningJobExecutions} 의 상수와 손으로 맞춰 놓은 것이라,
     * 하나만 빠져도 그 상태의 시체가 <b>아무것도 안 한 채 200</b> 을 받는다. 픽스처가
     * {@code STARTED} 만 심던 동안에는 {@code IN ('STARTED')} 변이체가 전 테스트 초록으로
     * 살아남았다 — 하필 {@code STARTING} 에서 죽은 행이 이 API 의 주 대상이다.
     */
    @ParameterizedTest
    @EnumSource(names = {"STARTING", "STARTED", "STOPPING"})
    @DisplayName("실행 중 상태 셋을 모두 걷어낸다 — 하나만 빠져도 조용히 안 걷힌다")
    void recoversEveryRunningStatus(BatchStatus status) throws Exception {
        try (RunningJobFixture dead = RunningJobFixture.plantWithoutStep(
                jobRepository, ExpireStepContext.JOB_NAME,
                key(10 + status.ordinal()), LocalDateTime.now().minus(DEAD), status)) {

            var response = api().post(
                    "/api/v1/admin/expire/runs/" + dead.executionId() + "/recover");

            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(VerifyApiProbe.json(response).path("data").path("alreadyRecovered")
                    .asBoolean())
                    .as("선점문에서 이 상태가 빠지면 0행이 되어 '이미 걷힘' 으로 나간다")
                    .isFalse();
            assertThat(jobRepository.getJobExecution(dead.executionId()).getStatus())
                    .isEqualTo(BatchStatus.FAILED);
        }
    }

    /**
     * <b>정상 완료한 실행은 걷어낼 대상이 아니다.</b> 실행 번호를 한 자리 잘못 치는 것은
     * 새벽 세 시의 기본값이고, 그때 200 <i>"이미 처리됨"</i> 을 받으면 운영자가 창을 닫는다 —
     * <b>진짜 시체는 그대로 남고 그동안 만료↔검증 상호 배제가 꺼져 있다.</b>
     */
    @Test
    @DisplayName("정상 완료한 실행은 409 다 — 200 '이미 처리됨' 이 아니다")
    void refusesACompletedRun() throws Exception {
        try (RunningJobFixture done = RunningJobFixture.plantCompleted(
                jobRepository, ExpireStepContext.JOB_NAME, key(20), Duration.ofDays(1))) {

            var response = api().post(
                    "/api/v1/admin/expire/runs/" + done.executionId() + "/recover");

            assertThat(response.statusCode()).isEqualTo(409);
            assertThat(VerifyApiProbe.error(response).path("code").asString())
                    .isEqualTo("EXPIRATION-007");
            assertThat(jobRepository.getJobExecution(done.executionId()).getStatus())
                    .isEqualTo(BatchStatus.COMPLETED);
        }
    }

    /**
     * <b>트랜잭션 경계를 구조로 못 박는다.</b> 동시 요청 테스트는 <b>겹침 자체를 단언하지
     * 못한다</b> — 겹치지 않아도 결과가 같아서, {@code @Transactional} 을 떼는 변이체가
     * 확률적으로만 잡힌다. 프록시 존재를 직접 보면 결정적으로 잡힌다.
     */
    @Test
    @DisplayName("복구는 한 트랜잭션에서 돈다 — 선점 락이 recover 끝까지 유지된다")
    void recoveryRunsInOneTransaction() {
        assertThat(org.springframework.aop.support.AopUtils.isAopProxy(recovery)).isTrue();
        assertThat(((org.springframework.aop.framework.Advised) recovery).getAdvisors())
                .as("떼면 선점이 자동커밋되어 락 보유가 문장 하나로 줄고, 동시 요청 둘이 "
                        + "모두 END_TIME 을 쓴다")
                .anyMatch(advisor -> advisor.getAdvice()
                        instanceof org.springframework.transaction.interceptor.TransactionInterceptor);
    }

    /**
     * <b>선점문이 되살아난 실행을 막는지 직접 잰다.</b>
     *
     * <p>판정({@code requireStuck})과 쓰기 사이의 창은 마이크로초라 흐름 테스트로는 재현이
     * 안 된다. 그래서 <b>문장 자체를 잰다</b> — 시체로 판정된 뒤 그 실행이 청크를 커밋하면
     * (락 대기가 풀리는 경우가 그렇다) 선점이 <b>0행이어야 한다.</b> 진도 조건이 빠지면
     * <b>살아 있는 만료를 {@code FAILED} 로 닫고</b>, 그 실행의 다음 {@code update} 가
     * 낙관적 락에 걸려 죽는다 — 만료는 재고를 쓰는 유일한 잡이다.
     */
    @Test
    @DisplayName("판정 뒤 되살아난 실행은 선점이 0행이다")
    void claimRefusesARevivedRun() throws Exception {
        try (RunningJobFixture dead = RunningJobFixture.plant(
                jobRepository, jdbcClient, ExpireStepContext.JOB_NAME, key(30), DEAD, DEAD)) {

            LocalDateTime stuckBefore = runningJobs
                    .stuckExecutions(ExpireStepContext.JOB_NAME).stream()
                    .filter(run -> run.execution().getId() == dead.executionId())
                    .findFirst().orElseThrow().stuckBefore();

            // 판정과 쓰기 사이에 청크가 커밋된 상황.
            //
            // ⚠️ **좌표계를 추측하지 않는다.** LAST_UPDATED 는 프레임워크가
            // LocalDateTime.now()(JVM 기본 존)로 찍고 MySQL 의 NOW() 는 컨테이너 존(UTC)이라
            // 둘이 아홉 시간 어긋난다 — NOW() 로 밀었더니 되살렸는데 안 되살린 것으로 보였다.
            // 그래서 **선점문이 비교에 쓰는 값(stuckBefore) 자체를 기준으로** 민다.
            // 같은 파라미터 경로를 지나므로 드라이버가 어떻게 다루든 대소 관계가 보존된다.
            int revived = jdbcClient.sql("UPDATE BATCH_STEP_EXECUTION SET LAST_UPDATED = :at "
                            + "WHERE JOB_EXECUTION_ID = :id")
                    .param("at", stuckBefore.plusMinutes(1))
                    .param("id", dead.executionId()).update();
            assertThat(revived).as("픽스처가 Step 을 심었어야 한다").isEqualTo(1);

            int claimed = jdbcClient.sql(ExpireRecoveryService.CLAIM)
                    .param("id", dead.executionId())
                    .param("stuckBefore", stuckBefore)
                    .update();

            assertThat(claimed)
                    .as("진도 조건이 없으면 살아 있는 만료를 FAILED 로 닫는다")
                    .isZero();
            assertThat(jobRepository.getJobExecution(dead.executionId()).getStatus())
                    .isEqualTo(BatchStatus.STARTED);
        }
    }

    /**
     * <b>Step 이 없으면 선점이 {@code START_TIME} 으로 판정한다.</b>
     *
     * <p>{@code NOT EXISTS (LAST_UPDATED > stuckBefore)} 로만 쓰면 <b>Step 행이 없을 때
     * 무조건 참</b>이라 방금 뜬 {@code STARTING} 실행도 닫힌다 — 하필 그 모양이 이 API 의
     * 주 대상이라 위험이 크다. 판정({@code RunningJobProbe.lastProgress})은
     * {@code START_TIME} 으로 떨어지는데 선점문이 그 폴백을 안 따라가면 둘이 갈린다.
     *
     * <p><b>기준 시각을 DB 에서 읽어 만든다.</b> 프레임워크가 쓴 {@code START_TIME} 은
     * 드라이버의 존 정규화를 타는데 파라미터로 넣는 값은 안 탄다 — 테스트 JVM 이 KST 라
     * (CY-392 가 일부러 그렇게 뒀다) 자바 쪽 {@code now()} 로 만든 값과 <b>아홉 시간
     * 어긋난다</b>(실측: {@code stuckBefore=01:07} vs {@code START_TIME=16:37}).
     * 그래서 그 컬럼을 읽어 <b>상대로</b> 밀어 술어만 잰다. 운영은 {@code TZ=UTC} 라
     * 두 축이 같다.
     */
    @Test
    @DisplayName("Step 이 없으면 START_TIME 으로 가른다 — 없다는 이유만으로 닫지 않는다")
    void claimFallsBackToStartTimeWhenNoStepExists() throws Exception {
        try (RunningJobFixture fresh = RunningJobFixture.plantWithoutStep(
                jobRepository, ExpireStepContext.JOB_NAME, key(31), LocalDateTime.now(),
                BatchStatus.STARTING)) {

            LocalDateTime startTime = jdbcClient
                    .sql("SELECT START_TIME FROM BATCH_JOB_EXECUTION "
                            + "WHERE JOB_EXECUTION_ID = :id")
                    .param("id", fresh.executionId())
                    .query(LocalDateTime.class).single();

            assertThat(claim(fresh.executionId(), startTime.minusMinutes(1)))
                    .as("START_TIME 이 임계보다 뒤면 살아 있는 것이다 — "
                            + "폴백이 없으면 Step 행이 없다는 이유만으로 닫힌다")
                    .isZero();

            assertThat(claim(fresh.executionId(), startTime.plusMinutes(1)))
                    .as("임계를 넘겼으면 걷힌다 — 폴백이 아예 안 걸리는 것도 아니어야 한다")
                    .isEqualTo(1);
        }
    }

    /** 선점문을 그대로 쳐서 대상 여부만 잰다. 흐름 테스트로는 못 만드는 창을 여기서 본다. */
    private int claim(long executionId, LocalDateTime stuckBefore) {
        return jdbcClient.sql(ExpireRecoveryService.CLAIM)
                .param("id", executionId)
                .param("stuckBefore", stuckBefore)
                .update();
    }

    /**
     * <b>이 컨트롤러가 여는 매핑 전체를 못 박는다.</b>
     *
     * <p>한때 이 자리가 {@code POST /api/v1/admin/expire} 하나의 404 만 쟀는데, 그 경로는
     * 클래스 레벨 매핑뿐이라 <b>지금 구조에서 자동으로 404</b> 다 — 아무것도 안 재고
     * 있었다. 매핑 목록을 통째로 단언하면 엔드포인트를 더하거나 빼는 티켓이 반드시
     * 이 목록을 고치게 되고, 그때 아래 결정을 함께 본다.
     *
     * <p><b>지켜야 하는 결정.</b> 만료를 띄우는 엔드포인트가 생기면 CY-421 이
     * {@code ExpirePendingRefresher} 를 {@code END_TIME} 정렬로 바꾼 근거가 깨진다 —
     * 과거 {@code asOf} 실행이 나중에 끝나 게이지가 <b>더 좁은 창의 더 작은 값</b>을 내고,
     * 관제는 그것을 <i>"밀린 것이 없다"</i> 로 읽는다.
     */
    @Test
    @DisplayName("복구 둘만 열려 있다 — 트리거가 생기면 CY-421 의 정렬 근거가 깨진다")
    void exposesExactlyTheRecoveryEndpoints() {
        Set<String> exposed = mappings.getHandlerMethods().entrySet().stream()
                .filter(entry -> entry.getValue().getBeanType() == ExpireAdminController.class)
                .flatMap(entry -> entry.getKey().getPathPatternsCondition().getPatternValues()
                        .stream()
                        .map(path -> entry.getKey().getMethodsCondition().getMethods() + " "
                                + path))
                .collect(Collectors.toSet());

        assertThat(exposed).containsExactlyInAnyOrder(
                "[GET] /api/v1/admin/expire/runs/stuck",
                "[POST] /api/v1/admin/expire/runs/{executionId}/recover");
    }

    /** 배치 메타의 낙관적 락 버전. 이 API 의 안전 계약이 이 값 위에 선다. */
    private long version(long executionId) {
        return jdbcClient
                .sql("SELECT VERSION FROM BATCH_JOB_EXECUTION WHERE JOB_EXECUTION_ID = :id")
                .param("id", executionId)
                .query(Long.class)
                .single();
    }
}
