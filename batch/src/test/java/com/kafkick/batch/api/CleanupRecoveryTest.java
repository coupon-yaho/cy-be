// 종료 표시를 못 남기고 죽은 정리 실행을 API 로 걷어내는 경로를 확인합니다.
package com.kafkick.batch.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.LocalDateTime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.batch.config.ExpireStepContext;
import com.kafkick.batch.config.RunningJobFixture;
import com.kafkick.batch.job.CleanupJobConfig;
import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>{@code BatchStuckExecution} 은 세 잡에 다 뜨는데 복구 경로는 둘뿐이었다.</b>
 * {@code cleanupJob} 만 손 SQL 이 유일한 길이었고 알림이 그 사실을 명시했다 — 이 클래스가
 * 그 경로를 잰다(CY-697).
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
class CleanupRecoveryTest {

    /** {@code batch.stuck-job-after-ms} 기본이 30분이다. 넉넉히 넘긴다. */
    private static final Duration DEAD = Duration.ofHours(2);

    @LocalServerPort
    private int port;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private CleanupRecoveryService recovery;

    @Autowired
    private com.kafkick.batch.config.RunningJobProbe runningJobs;

    @Autowired
    private org.springframework.context.ApplicationContext context;

    @Autowired
    private org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping
            mappings;

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
        return LocalDateTime.of(2026, 6, 1, 0, 0).plusHours(slot);
    }

    /**
     * <b>{@code ABANDONED} 가 아니라 {@code FAILED} 인 것이 계약이다.</b> 만료 쪽과 같은
     * 이유다 — {@code ABANDONED} 면 그 {@code JobInstance} 를 같은 파라미터로 영원히 못 돈다.
     */
    @Test
    @DisplayName("시체는 목록에 나오고 한 번에 FAILED 로 닫힌다")
    void listsAndRecoversAStuckRun() throws Exception {
        try (RunningJobFixture dead = RunningJobFixture.plant(
                jobRepository, jdbcClient, CleanupJobConfig.JOB_NAME, key(1), DEAD, DEAD)) {

            var listed = VerifyApiProbe.json(api().get("/api/v1/admin/cleanup/runs/stuck"))
                    .path("data");
            assertThat(listed).hasSize(1);
            assertThat(listed.get(0).path("executionId").asLong()).isEqualTo(dead.executionId());
            assertThat(listed.get(0).path("stalledSeconds").asLong())
                    .as("정말 죽었는지를 사람이 판단할 재료다 — 없으면 손 SQL 로 되돌아간다")
                    .isGreaterThan(DEAD.toSeconds() - 60);

            var recovered = api().post(
                    "/api/v1/admin/cleanup/runs/" + dead.executionId() + "/recover");
            assertThat(recovered.statusCode()).isEqualTo(200);
            assertThat(VerifyApiProbe.json(recovered).path("data").path("status").asText())
                    .isEqualTo("FAILED");

            var closed = jobRepository.getJobExecution(dead.executionId());
            assertThat(closed.getStatus()).isEqualTo(BatchStatus.FAILED);
            assertThat(closed.getEndTime()).isNotNull();
            assertThat(closed.getStepExecutions())
                    .as("Step 행도 함께 닫혀야 한다 — 손 SQL 은 그것을 STARTED 로 남겼다")
                    .allSatisfy(step -> assertThat(step.getStatus().isRunning()).isFalse());

            assertThat(VerifyApiProbe.json(api().get("/api/v1/admin/cleanup/runs/stuck"))
                    .path("data"))
                    .as("걷어낸 뒤에는 목록이 비어야 한다")
                    .isEmpty();
        }
    }

    /** <b>진도가 있는 실행은 안 걷는다.</b> 걷으면 살아 있는 정리를 FAILED 로 닫는다. */
    @Test
    @DisplayName("도는 실행은 목록에 없고 걷으려 하면 409 다")
    void refusesToRecoverALiveRun() throws Exception {
        try (RunningJobFixture alive = RunningJobFixture.plant(
                jobRepository, jdbcClient, CleanupJobConfig.JOB_NAME, key(2),
                DEAD, Duration.ZERO)) {

            assertThat(VerifyApiProbe.json(api().get("/api/v1/admin/cleanup/runs/stuck"))
                    .path("data"))
                    .as("진도가 있으면 시체가 아니다")
                    .isEmpty();

            var refused = api().post(
                    "/api/v1/admin/cleanup/runs/" + alive.executionId() + "/recover");
            assertThat(refused.statusCode()).isEqualTo(409);
            assertThat(VerifyApiProbe.json(refused).path("error").path("code").asText())
                    .isEqualTo("VERIFICATION-021");
            assertThat(jobRepository.getJobExecution(alive.executionId()).getStatus())
                    .as("거절했으면 상태를 안 건드렸어야 한다")
                    .isEqualTo(BatchStatus.STARTED);
        }
    }

    /**
     * <b>남의 잡은 404 다.</b> 알려 주면 인증 없는 이 API 가 실행 번호 공간을 훑는 수단이 된다.
     * 그리고 잡 이름을 안 보면 <b>이 경로가 만료를 걷어낼 수 있다</b> — 만료는 재고를 쓰는
     * 유일한 잡이라 그 사고가 훨씬 비싸다.
     */
    @Test
    @DisplayName("만료 잡의 실행은 이 경로에서 404 다")
    void refusesAnExpireExecution() throws Exception {
        try (RunningJobFixture expire = RunningJobFixture.plant(
                jobRepository, jdbcClient, ExpireStepContext.JOB_NAME, key(3), DEAD, DEAD)) {

            var refused = api().post(
                    "/api/v1/admin/cleanup/runs/" + expire.executionId() + "/recover");
            assertThat(refused.statusCode()).isEqualTo(404);
            assertThat(VerifyApiProbe.json(refused).path("error").path("code").asText())
                    .isEqualTo("VERIFICATION-020");
            assertThat(jobRepository.getJobExecution(expire.executionId()).getStatus())
                    .isEqualTo(BatchStatus.STARTED);
        }
    }

    /**
     * <b>재시도해도 된다.</b> 복구 API 가 재시도에서 에러를 내면 운영자는 첫 호출이
     * 실패했다고 읽고 손 SQL 로 되돌아간다 — 이 API 를 만든 이유가 사라진다.
     */
    @Test
    @DisplayName("두 번 불러도 200 이고 두 번째는 아무것도 안 쓴다")
    void isRetryable() throws Exception {
        try (RunningJobFixture dead = RunningJobFixture.plant(
                jobRepository, jdbcClient, CleanupJobConfig.JOB_NAME, key(4), DEAD, DEAD)) {

            assertThat(api().post("/api/v1/admin/cleanup/runs/" + dead.executionId()
                    + "/recover").statusCode()).isEqualTo(200);

            var again = api().post(
                    "/api/v1/admin/cleanup/runs/" + dead.executionId() + "/recover");
            assertThat(again.statusCode()).isEqualTo(200);
            assertThat(VerifyApiProbe.json(again).path("data").path("alreadyRecovered").asBoolean())
                    .as("이미 걷었다는 것을 말해야 운영자가 첫 호출을 의심하지 않는다")
                    .isTrue();
        }
    }


    /**
     * <b>트랜잭션 경계를 구조로 못 박는다.</b> 동시 요청 테스트는 겹침 자체를 단언하지
     * 못한다 — 겹치지 않아도 결과가 같아서 {@code @Transactional} 을 떼는 변이체가 확률적
     * 으로만 잡힌다. 프록시 존재를 직접 보면 결정적으로 잡힌다
     * ({@code ExpireRecoveryTest} 가 같은 이유로 같은 모양을 쓴다).
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
     * <b>조회 경로에도 경계가 있어야 한다.</b> 트랜잭션 밖이면 {@code DataSourceUtils} 가
     * {@code queryTimeout} 을 안 붙여 <b>조회가 다시 맨몸이 된다</b> — 이 PR 의 A 파트가
     * 통째로 no-op 이 되는데 흐름 테스트는 전부 초록이다.
     */
    @Test
    @DisplayName("admin 컨트롤러 셋이 트랜잭션 프록시다")
    void adminControllersRunInsideATransaction() {
        for (Object bean : java.util.List.of(
                context.getBean(CleanupAdminController.class),
                context.getBean(ExpireAdminController.class),
                context.getBean(VerifyTriggerController.class))) {
            assertThat(org.springframework.aop.support.AopUtils.isAopProxy(bean))
                    .as("%s 에 @Transactional 이 안 붙으면 조회에 데드라인이 없다",
                            bean.getClass().getSimpleName())
                    .isTrue();
        }
    }

    /**
     * <b>선점문이 되살아난 실행을 막는지 직접 잰다.</b> 판정과 쓰기 사이는 마이크로초라
     * 밖에서 못 벌린다 — 그래서 문장 자체를 잰다.
     */
    @Test
    @DisplayName("판정 뒤 되살아난 실행은 선점이 0행이다")
    void claimRefusesARevivedRun() throws Exception {
        try (RunningJobFixture dead = RunningJobFixture.plant(
                jobRepository, jdbcClient, CleanupJobConfig.JOB_NAME, key(5), DEAD, DEAD)) {

            LocalDateTime stuckBefore = runningJobs
                    .stuckExecutions(CleanupJobConfig.JOB_NAME).stream()
                    .filter(run -> run.execution().getId() == dead.executionId())
                    .findFirst().orElseThrow().stuckBefore();

            // ⚠️ 좌표계를 추측하지 않는다 — 선점문이 비교에 쓰는 값 자체를 기준으로 민다.
            int revived = jdbcClient.sql("UPDATE BATCH_STEP_EXECUTION SET LAST_UPDATED = :at "
                            + "WHERE JOB_EXECUTION_ID = :id")
                    .param("at", stuckBefore.plusMinutes(1))
                    .param("id", dead.executionId()).update();
            assertThat(revived).isEqualTo(1);

            assertThat(jdbcClient.sql(StuckRunClaim.CLAIM)
                    .param("id", dead.executionId())
                    .param("stuckBefore", stuckBefore)
                    .update())
                    .as("진도 조건이 없으면 살아 있는 정리를 FAILED 로 닫는다")
                    .isZero();
            assertThat(jobRepository.getJobExecution(dead.executionId()).getStatus())
                    .isEqualTo(BatchStatus.STARTED);
        }
    }

    /**
     * <b>트리거는 열지 않는다.</b> 이 컨트롤러가 여는 것은 조회와 복구 둘뿐이어야 한다 —
     * {@code docs/15} 가 verify 쪽에 세운 규율을 정리 쪽으로 옮긴다.
     */
    @Test
    @DisplayName("정리 admin 은 조회와 복구 둘만 연다")
    void exposesOnlyTwoEndpoints() {
        java.util.Set<String> exposed = mappings.getHandlerMethods().entrySet().stream()
                .filter(entry -> entry.getValue().getBeanType() == CleanupAdminController.class)
                .flatMap(entry -> entry.getKey().getPathPatternsCondition()
                        .getPatternValues().stream()
                        .map(path -> entry.getKey().getMethodsCondition().getMethods().stream()
                                .findFirst().map(Enum::name).orElse("?") + " " + path))
                .collect(java.util.stream.Collectors.toSet());

        assertThat(exposed)
                .as("정리 잡을 손으로 띄우는 경로가 생기면 슬롯 규율이 무너진다")
                .containsExactlyInAnyOrder(
                        "GET /api/v1/admin/cleanup/runs/stuck",
                        "POST /api/v1/admin/cleanup/runs/{executionId}/recover");
    }

    /** 없는 번호는 404 다 — 500 이 아니다. */
    @Test
    @DisplayName("없는 executionId 는 404 다")
    void refusesAnUnknownExecution() throws Exception {
        var refused = api().post("/api/v1/admin/cleanup/runs/999999/recover");
        assertThat(refused.statusCode()).isEqualTo(404);
        assertThat(VerifyApiProbe.json(refused).path("error").path("code").asText())
                .isEqualTo("VERIFICATION-020");
    }
}
