// 검증 트리거 API 가 실제로 잡을 띄우고, 두 축을 갈라 답하는지 확인합니다.
package com.kafkick.batch.api;

import static com.kafkick.batch.api.VerifyApiProbe.awaitUntil;
import static com.kafkick.batch.api.VerifyApiProbe.body;
import static com.kafkick.batch.api.VerifyApiProbe.error;
import static com.kafkick.batch.api.VerifyApiProbe.json;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.batch.config.RunningJobFixture;
import com.kafkick.batch.config.VerifyExecutorConfig;
import com.kafkick.batch.job.VerifyJobConfig;
import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.VerificationRunRepository;
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>지금까지 검증을 손으로 시작할 방법이 테스트 말고 없었다.</b> 이 클래스가 그 API 를 잰다.
 *
 * <p>여기서 지키는 계약은 넷이다.
 *
 * <ul>
 *   <li><b>202 는 {@code executionId} 다</b> — {@code runId} 는 가드 여덟을 통과한 뒤에야
 *       생겨서 접수 시점에 없다
 *   <li><b>두 축을 안 섞는다</b> — 잡이 돌았는가({@code status})와 데이터가 맞는가
 *       ({@code verdict}) 는 독립이다
 *   <li><b>실행기가 격리돼 있다</b> — 공용 {@link JobOperator} 를 비동기로 바꾸면
 *       만료 배치의 겹침 방지가 무너진다
 * </ul>
 *
 * <p><b>만료가 도는 중이면 거절하는 축은 여기 없다.</b> 이 클래스에는 실행 중인 만료가
 * 없어 그 가드를 구조적으로 밟을 수 없다 — {@code VerifyTriggerExpireGuardTest} 가 그 축을
 * 따로 진다.
 *
 * <p>{@code batch.scheduling.enabled=false} 로 띄우는 것은 진짜 만료 크론이 테스트 도중에
 * 발화해 위 가드에 걸리는 것을 막기 위해서다. 그 플래그가 <b>검증의 조건이었던 것은
 * CY-384 까지</b>다.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "server.port=0",
        "management.server.port=0",
        // 되읽기가 테스트 중 같은 DB 를 함께 읽지 않게 상한까지 늘린다.
        "batch.verify.metrics-refresh-ms=120000"
})
@Import(MySqlContainerConfig.class)
class VerifyTriggerApiTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 3, 1, 9, 0);

    /** {@code BatchStatus} 중 더 안 바뀌는 것들. STARTING·STARTED·STOPPING 은 진행 중이다. */
    private static final Set<String> TERMINAL =
            Set.of("COMPLETED", "FAILED", "STOPPED", "ABANDONED", "UNKNOWN");

    @LocalServerPort
    private int port;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private VerificationRunRepository runs;

    @Autowired
    @Qualifier(VerifyExecutorConfig.OPERATOR)
    private JobOperator verifyJobOperator;

    @Autowired
    @Qualifier("jobOperator")
    private JobOperator sharedJobOperator;

    @Autowired
    private com.kafkick.batch.config.RunningJobProbe runningJobs;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    @Qualifier("expireJob")
    private Job expireJob;

    private VerifyApiProbe probe;

    @BeforeEach
    void setUp() throws Exception {
        // 비동기 잡은 테스트 경계를 넘는다. 앞 테스트가 띄운 실행이 아직 도는 중에 행을
        // 지우면 그 잡의 finalizeRunStep 이 "실행 행이 사라졌습니다" 로 죽는다 —
        // 실제로 겪었고, 원인 테스트가 아니라 다음 테스트가 빨개진다.
        awaitNoRunningVerify();

        // 손으로 지우면 FK 순서를 틀린다(hourly_stats 가 verification_runs 를 문다).
        // 이 헬퍼가 그 순서를 알고 있고, 잡을 실제로 돌리는 테스트를 위해 만들어진 것이다 —
        // @RepositoryTest 와 달리 여기는 트랜잭션 밖이라 롤백이 없다.
        new VerificationSeed(jdbcClient).clear();
        // 배치 메타도 비운다. verification_runs 만 지우면 두 소스가 어긋나 —
        // nextAttempt 는 1 을 주는데 BATCH_JOB_INSTANCE 에는 그 파라미터 조합이
        // COMPLETED 로 남아 있어 "이미 완료된 인스턴스" 로 거절된다.
        new JobRepositoryTestUtils(jobRepository).removeJobExecutions();
        probe = new VerifyApiProbe(port);
    }

    /**
     * <b>남긴 것을 치우고 나간다.</b> {@code verifyJob} 은 {@code statsAggregateStep} 까지
     * 돌아 {@code hourly_stats} 등에 행을 심는데, 그것이 {@code verification_runs} 를 FK 로
     * 문다. 다음 <b>다른 클래스</b>가 {@code DELETE FROM verification_runs} 만 하면
     * 거기서 FK 위반으로 죽는다 — 실제로 {@code VerificationMetricExposureTest} 를 그렇게
     * 깨뜨렸다. 원인 테스트가 아니라 남의 테스트가 빨개지는 모양이다.
     */
    @AfterEach
    void tearDown() throws Exception {
        // ⚠️ **치우기를 기다림에 걸지 않는다.** 한때 `awaitNoRunningVerify()` 가 맨 앞에
        // 맨몸으로 있었는데, 그것이 90초 뒤 AssertionError 를 던지면 아래 두 줄에 도달하지
        // 못했다. 그러면 못 끝낸 실행이 DB 에 남아 **다음 테스트도 같은 자리에서** 90초를
        // 태우고 죽는다 — 21개짜리 클래스가 통째로 33분간 빨개졌다. 원인 하나가 전부를
        // 무너뜨리지 않게 치우기를 finally 로 내린다.
        try {
            awaitNoRunningVerify();
        } finally {
            new VerificationSeed(jdbcClient).clear();
            new JobRepositoryTestUtils(jobRepository).removeJobExecutions();
        }
    }

    /** 실행기가 스레드 하나라, 도는 것이 없어지면 다음 테스트가 안전하다. */
    private void awaitNoRunningVerify() throws Exception {
        awaitUntil(Duration.ofSeconds(90), "앞 테스트의 검증 잡이 안 끝났습니다", () -> {
            try {
                return verifyJobOperator.getRunningExecutions("verifyJob").isEmpty();
            } catch (Exception e) {
                // 아직 한 번도 안 돌아 잡 이름이 등록 전이면 도는 것도 없다.
                return true;
            }
        });
    }

    /**
     * <b>진도가 안 멈춘 실행을 손으로 심는다.</b> {@code jobRepository} 에 직접 쓰는 것이라
     * <b>이것을 돌리는 스레드가 없다</b> — 스스로 끝나지 않는다. 그래서 심은 쪽이
     * {@link #finish(JobExecution)} 로 반드시 닫아야 한다.
     *
     * <p>안 닫으면 {@code tearDown} 의 {@code awaitNoRunningVerify} 가 90초를 태우고
     * {@code AssertionError} 로 죽는데, 그 예외가 뒤의 {@code removeJobExecutions()} 를
     * 건너뛰게 만들어 이 행이 DB 에 남는다. 그러면 <b>남은 테스트 전부가</b> 같은 자리에서
     * 90초씩 태우며 빨개진다 — 원인 테스트가 아니라 남의 테스트가 죽는 모양이다.
     * 실제로 그렇게 21개짜리 클래스가 33분을 돌았다.
     */
    private JobExecution liveExecution(LocalDateTime asOf) throws Exception {
        JobParameters parameters = new JobParametersBuilder()
                .addLocalDateTime("asOf", asOf)
                .addString("dataset", "CLEAN")
                .addString("scope", "FULL")
                .addLong("attempt", 1L)
                .toJobParameters();
        JobExecution live = jobRepository.createJobExecution(
                jobRepository.createJobInstance(VerifyJobConfig.JOB_NAME, parameters),
                parameters,
                new org.springframework.batch.infrastructure.item.ExecutionContext());
        live.setStatus(BatchStatus.STARTED);
        // 방금 시작했다 = 진도가 멈추지 않았다.
        live.setStartTime(LocalDateTime.now());
        jobRepository.update(live);
        return live;
    }

    /** {@link #liveExecution} 이 심은 행을 도는 축에서 뺀다. {@code finally} 에서 부른다. */
    private void finish(JobExecution live) {
        live.setStatus(BatchStatus.ABANDONED);
        live.setEndTime(LocalDateTime.now());
        jobRepository.update(live);
    }

    /**
     * <b>공용 빈을 비동기로 바꾸면 만료가 자기 자신과 겹친다.</b> {@code ExpireScheduler} 는
     * 겹침 방지를 크론 트리거의 순차성에 기대는데, 비동기가 되면 그 전제가 사라진다.
     * 그쪽은 그 사고를 잡으려고 {@code status.isRunning()} 검사까지 심어 뒀다 —
     * <b>이 테스트는 그 검사가 발동할 일이 없게 만드는 쪽</b>을 지킨다.
     */
    @Test
    @DisplayName("verify 실행기와 공용 실행기가 다른 인스턴스다")
    void verifyOperatorIsIsolatedFromTheSharedOne() {
        assertThat(verifyJobOperator)
                .as("같은 빈이면 만료 배치가 비동기로 떠서 겹침 방지가 무너진다")
                .isNotSameAs(sharedJobOperator);
    }

    /**
     * <b>202 에 실리는 것은 {@code executionId} 다.</b> {@code runId} 를 미리 만들려면
     * 컨트롤러가 행을 먼저 넣어야 하는데, 그러면 가드에 걸려 죽은 실행도 {@code runId} 를
     * 갖게 되어 <i>"runId 없음 = 판정 못 냄"</i> 계약이 무너진다.
     */
    @Test
    @DisplayName("트리거는 202 와 executionId 를 준다 — runId 가 아니다")
    void triggerReturnsExecutionIdNotRunId() throws Exception {
        TriggerAccepted accepted = trigger(AS_OF);

        assertThat(accepted).isNotNull();
        assertThat(accepted.executionId())
                .as("Spring Batch 가 시작 즉시 주는 값이라 202 에 실을 수 있다")
                .isNotNull();
        assertThat(accepted.dataset())
                .as("안 주면 붙어 있는 스키마로 정한다 — 컨테이너는 CLEAN 이다")
                .isEqualTo(DatasetType.CLEAN);
        assertThat(accepted.scope()).isEqualTo(ScopeType.FULL);
        assertThat(accepted.attempt())
                .as("서버가 채운 값을 안 돌려주면 요청자가 자기가 뭘 돌렸는지 모른다")
                .isPositive();
    }

    /**
     * <b>{@code runId} 가 {@code null} 인 것도 정보다.</b> 빈 스키마라 리플레이할 것이 없어
     * 잡은 금방 끝나는데, 그 끝난 상태에서 {@code runId} 가 채워지는 것이 정상 경로다.
     */
    @Test
    @DisplayName("조회가 두 축을 갈라 답한다 — 잡 상태와 판정")
    void findSeparatesJobStatusFromVerdict() throws Exception {
        long executionId = trigger(AS_OF).executionId();

        awaitTerminal(executionId);

        VerifyRunView view = find(executionId);
        assertThat(view.status())
                .as("STARTED 에 머물면 비동기 실행기가 잡을 못 집은 것이다")
                .isIn("COMPLETED", "FAILED");
        assertThat(view.executionId()).isEqualTo(executionId);
        assertThat(view.runId())
                .as("가드를 통과했으면 runId 가 있어야 한다. null 이면 판정 단계에 못 갔다는 뜻")
                .isNotNull();
        assertThat(view.asOf())
                .as("판정의 기준 시각이 요청한 것과 같아야 한다")
                .isEqualTo(AS_OF);
    }

    /**
     * <b>{@code asOf} 만 기본값을 안 준다.</b> 나머지는 틀려도 잡이 거절하지만, {@code asOf} 는
     * 아무 값이나 성립해서 <b>조용히 다른 것을 판정한다.</b>
     */
    @Test
    @DisplayName("asOf 가 없으면 400 — 서버가 임의로 채우지 않는다")
    void rejectsARequestWithoutAsOf() throws Exception {
        assertThat(probe.post("/api/v1/admin/verify").statusCode())
                .as("서버가 now() 로 채우면 같은 요청이 매번 다른 것을 판정한다")
                .isEqualTo(400);
    }

    /**
     * <b>같은 {@code (asOf, dataset, scope)} 를 다시 부르면 attempt 가 올라간다.</b>
     * {@code uk_run_params} 가 넷을 묶어 유일성을 걸므로, 서버가 안 찾아 주면 사람이
     * 시드가 점유한 번호를 외우고 있어야 한다.
     */
    @Test
    @DisplayName("attempt 를 서버가 올려 준다 — 시드가 점유한 번호를 피한다")
    void assignsTheNextFreeAttempt() throws Exception {
        TriggerAccepted accepted = trigger(AS_OF);
        int first = accepted.attempt();
        awaitTerminal(accepted.executionId());

        int second = trigger(AS_OF).attempt();
        assertThat(second)
                .as("같은 번호를 다시 주면 INVALID_RUN_PARAMS 로 죽는다")
                .isGreaterThan(first);
    }

    /**
     * <b>없는 실행을 조회하면 404 다.</b> 200 에 빈 본문을 주면 클라이언트가 폴링을 영원히
     * 돈다 — <i>"아직 안 끝났다"</i> 와 <i>"그런 것 없다"</i> 가 같은 값이 된다.
     */
    @Test
    @DisplayName("없는 executionId 는 404 로 답한다")
    void answers404ForAnUnknownExecution() throws Exception {
        HttpResponse<String> response = probe.get("/api/v1/admin/verify/runs/999999");

        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(error(response).path("code").asText()).isEqualTo("VERIFICATION-014");
    }

    /**
     * <b>도는 검증은 못 멈춘다.</b> 이것이 CY-678 의 본체다.
     *
     * <p>{@code stop} 은 살아 있는 실행에도 먹는다 — {@code SimpleJobRepository} 가
     * {@code STOPPING} 을 즉시 {@code STOPPED} 로 올린다. 그 순간 트리거의 429 와
     * {@code ExpireScheduler}·{@code CleanupJobConfig} 의 물러나기가 <b>함께</b> 풀리는데,
     * 스레드는 아직 돈다. 그래서 진도가 있는 실행은 여기서 거절한다.
     */
    @Test
    @DisplayName("진도가 있는 실행은 못 멈춘다 — 만료·정리와의 상호 배제가 그 자리에서 꺼진다")
    void rejectsStoppingALiveExecution() throws Exception {
        JobExecution live = liveExecution(AS_OF.minusDays(2));
        try {
            HttpResponse<String> rejected =
                    probe.post("/api/v1/admin/verify/runs/" + live.getId() + "/stop");

            assertThat(rejected.statusCode())
                    .as("도는 검증을 멈추면 472초가 버려지는 데서 끝나지 않는다. 본문=%s",
                            rejected.body())
                    .isEqualTo(409);
            assertThat(error(rejected).path("code").asText()).isEqualTo("VERIFICATION-019");

            assertThat(jobRepository.getJobExecution(live.getId()).getStatus())
                    .as("거절했으면 상태를 건드리지 않았어야 한다 — STOPPING 을 만들면 "
                            + "그것만으로 만료·정리가 물러나기를 그만둔다")
                    .isEqualTo(BatchStatus.STARTED);
        } finally {
            finish(live);
        }
    }

    /**
     * <b>이미 끝난 것은 멈출 수 없다.</b> 사건이 아니라 상태다 — 500 으로 접으면 알림이
     * 붙는 날 소음이 된다.
     */
    @Test
    @DisplayName("끝난 실행을 멈추려 하면 409 다")
    void answers409WhenStoppingAFinishedExecution() throws Exception {
        long executionId = trigger(AS_OF).executionId();
        awaitTerminal(executionId);

        HttpResponse<String> response =
                probe.post("/api/v1/admin/verify/runs/" + executionId + "/stop");

        assertThat(response.statusCode()).isEqualTo(409);
        assertThat(error(response).path("code").asText()).isEqualTo("VERIFICATION-015");
    }

    /**
     * <b>에러도 한 형식으로 나간다.</b> 핸들러가 없으면 스프링 기본 오류 페이지가 나가는데
     * 그 본문은 성공 응답과 모양이 다르다 — 클라이언트가 두 형식을 다뤄야 한다.
     */
    @Test
    @DisplayName("에러 본문에 status·code·message·timestamp 가 있다")
    void errorsCarryTheSameShape() throws Exception {
        HttpResponse<String> response = probe.get("/api/v1/admin/verify/runs/999999");

        // 저장소 규약: 응답은 항상 ResponseEnvelope 다 — 성공도 실패도 같은 봉투.
        assertThat(json(response).path("success").asBoolean()).isFalse();
        assertThat(json(response).path("data").isNull()).isTrue();

        var body = error(response);
        assertThat(body.path("status").asInt()).isEqualTo(404);
        assertThat(body.path("code").asText()).isEqualTo("VERIFICATION-014");
        assertThat(body.path("message").asText()).isNotBlank();
        assertThat(body.path("timestamp").isMissingNode()).isFalse();
        assertThat(body.path("detail").isMissingNode())
                .as("detail 은 로그용이다. 클라이언트에 나가는 문구는 카탈로그 메시지뿐이고, "
                        + "이 API 는 앞단이 얇아 더 지켜야 한다")
                .isTrue();
    }

    /**
     * <b>실패한 시도가 {@code attempt} 를 태운다.</b> {@code verification_runs} 행은
     * 가드를 통과한 뒤에야 생기는데 {@code BATCH_JOB_INSTANCE} 는 시작 즉시 생긴다.
     * 앞 소스만 보면 같은 번호를 다시 주고, {@code preventRestart} 가 그것을 거절한다 —
     * <b>몇 번을 눌러도 400 이고 자기 치유가 없다.</b> 실측으로 확인한 상태다.
     */
    @Test
    @DisplayName("가드에 걸려 죽어도 같은 요청을 다시 접수한다")
    void staysUsableAfterAGuardRejection() throws Exception {
        // CLEAN 스키마에 CORRUPT 를 달라고 하면 rejectDatasetMismatch 가 잡는다.
        // seedRunId 를 줘야 접수 단계를 통과해 잡까지 간다.
        long first = triggerCorrupt().executionId();
        awaitTerminal(first);
        assertThat(find(first).status())
                .as("가드에 걸려야 이 테스트가 뜻을 갖는다")
                .isEqualTo("FAILED");

        HttpResponse<String> again = probe.post(
                "/api/v1/admin/verify?asOf=" + AS_OF + "&dataset=CORRUPT&seedRunId=1");
        assertThat(again.statusCode())
                .as("nextAttempt 가 verification_runs 만 보면 같은 번호를 다시 줘서 "
                        + "preventRestart 에 막힌다. 본문=%s", again.body())
                .isEqualTo(202);
    }

    /**
     * <b>실패 원인이 응답에 있어야 한다.</b> 없으면 트리거를 연 이유
     * (<i>"202 를 받아 놓고 폴링해야 원인을 안다"</i> 를 없애는 것)가 절반만 달성된다.
     */
    @Test
    @DisplayName("가드에 걸린 실행은 어느 Step 에서 왜 죽었는지 알려 준다")
    void reportsWhichStepFailed() throws Exception {
        long executionId = triggerCorrupt().executionId();
        awaitTerminal(executionId);

        VerifyRunView view = find(executionId);
        assertThat(view.failure())
                .as("getAllFailureExceptions() 는 DB 조회 객체에서 언제나 비어 있다 — "
                        + "영속되는 exitDescription 을 읽어야 한다")
                .isNotNull()
                .contains("startRunStep");
        assertThat(view.failure())
                .as("스택트레이스가 통째로 나가면 인증 없는 API 로 내부 구조가 샌다")
                .doesNotContain("\n")
                .hasSizeLessThan(600);
    }

    /**
     * <b>{@code CORRUPT} 는 {@code seedRunId} 없이 못 돈다.</b> 접수 단계에서 안 막으면
     * 202 를 준 뒤 잡 안에서 죽고, 그 실패가 {@code attempt} 를 태운다.
     */
    @Test
    @DisplayName("CORRUPT 에 seedRunId 가 없으면 접수 단계에서 400")
    void rejectsCorruptWithoutSeedRunId() throws Exception {
        HttpResponse<String> response = probe.post(
                "/api/v1/admin/verify?asOf=" + AS_OF + "&dataset=CORRUPT");

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(error(response).path("code").asText()).isEqualTo("VERIFICATION-002");
    }

    /**
     * <b>{@code fromTs} 는 성공 조합이 없다.</b> {@code FULL} 이면 {@code VerificationRun}
     * 생성자가 거부하고, 유일한 대안인 {@code INCREMENTAL} 은 가드가 막는다.
     */
    @Test
    @DisplayName("fromTs 는 접수 단계에서 400 — 성공 조합이 없다")
    void rejectsFromTsUntilIncrementalOpens() throws Exception {
        assertThat(probe.post("/api/v1/admin/verify?asOf=" + AS_OF + "&fromTs=" + AS_OF)
                .statusCode())
                .isEqualTo(400);
    }

    /**
     * <b>오프셋이 붙으면 조용히 절단된다.</b> {@code ISO.DATE_TIME} 은 그것을 파싱에
     * 성공시킨 뒤 지역 부분만 뽑는다 — JS 의 {@code toISOString()} 이 항상 {@code Z} 를
     * 붙이므로 프론트가 보내면 <b>아홉 시간이 밀린 시점으로 판정</b>하고 그 행에는 밀린
     * 값이 적혀 나중에 봐도 틀린 줄 모른다.
     */
    @Test
    @DisplayName("asOf 에 Z 나 오프셋이 붙으면 400 — 조용히 절단하지 않는다")
    void rejectsAnAsOfWithAnOffset() throws Exception {
        assertThat(probe.post("/api/v1/admin/verify?asOf=2026-03-01T09:00:00Z").statusCode())
                .as("절단해서 받으면 요청자가 의도한 것과 다른 시점을 판정한다")
                .isEqualTo(400);
    }

    /**
     * <b>타입이 안 맞는 값은 400 이다.</b> {@code MethodArgumentTypeMismatchException} 은
     * {@code ErrorResponse} 를 구현하지 않아 그냥 두면 500 으로 새고, 앞단이 얇은 이 API 에서
     * 누구나 ERROR 로그를 채울 수 있게 된다.
     */
    @Test
    @DisplayName("모르는 dataset 값은 400 — 500 이 아니다")
    void rejectsAnUnknownEnumValue() throws Exception {
        assertThat(probe.post("/api/v1/admin/verify?asOf=" + AS_OF + "&dataset=NOPE").statusCode())
                .isEqualTo(400);
    }

    /**
     * <b>{@code /verify/} 경로가 만료 배치를 멈추면 안 된다.</b> 중단 신호는 공유
     * {@code JobRepository} 에 쓰이므로 어느 operator 로 시작했든 멈춘다 — 이름 검사가
     * 없으면 재고를 쓰는 유일한 잡이 이 경로로 중단된다. 둘은 같은 실행 시퀀스를 공유해
     * 번호가 섞인다.
     */
    @Test
    @DisplayName("만료 잡의 executionId 는 이 경로에서 404 다")
    void refusesToTouchAnotherJobsExecution() throws Exception {
        // 만료 잡을 **실제로 돌리지 않는다.** 공용 operator 는 동기라 start 한 줄이 잡
        // 전체를 실행하고, 이 클래스는 트랜잭션 밖이라 롤백이 없다 — 지금은 데이터가
        // 0건이라 무해하지만 그 무해함은 우연이다. 시드를 심는 케이스가 하나라도 붙고
        // 실행 순서가 그 뒤로 잡히면 만료가 그 데이터를 바꾼다.
        //
        // 여기서 재는 것은 requireVerifyExecution 의 **잡 이름 검사**이지 만료의 동작이
        // 아니므로, 실행 행만 만들어 그 id 를 쓴다.
        JobParameters parameters =
                new JobParametersBuilder().addLocalDateTime("asOf", AS_OF).toJobParameters();
        JobExecution expire = jobRepository.createJobExecution(
                jobRepository.createJobInstance("expireJob", parameters),
                parameters,
                new org.springframework.batch.infrastructure.item.ExecutionContext());

        assertThat(probe.get("/api/v1/admin/verify/runs/" + expire.getId()).statusCode())
                .as("200 에 runId:null 을 주면 '판정 단계에 못 간 검증' 처럼 보인다 — 거짓이다")
                .isEqualTo(404);
        assertThat(probe.post("/api/v1/admin/verify/runs/" + expire.getId() + "/stop").statusCode())
                .as("만료를 중간에 멈추면 다음 검증의 판정 근거가 흔들린다")
                .isEqualTo(404);
    }

    /** 없는 실행을 멈추려 하면 404 다. GET 만 고치고 stop 을 빠뜨렸던 자리다. */
    @Test
    @DisplayName("없는 executionId 를 멈추려 하면 404 — 500 이 아니다")
    void answers404WhenStoppingAnUnknownExecution() throws Exception {
        assertThat(probe.post("/api/v1/admin/verify/runs/999999/stop").statusCode())
                .isEqualTo(404);
    }

    /**
     * <b>도는 실행이 있으면 429 다.</b> 이 축을 재는 테스트가 없으면
     * {@code rejectIfAlreadyRunning} 호출 한 줄을 지워도 전부 초록이다 — 1라운드에서 그
     * 수정을 넣고도 고정하지 않았던 자리다.
     *
     * <p><b>그리고 이것이 하드킬 잔존의 모양이다.</b> {@code getRunningExecutions} 는
     * 인메모리가 아니라 DB 의 {@code STATUS IN ('STARTING','STARTED','STOPPING')} 을 본다.
     * SIGKILL·OOM 으로 프로세스가 죽으면 {@code STARTED} 행이 영구히 남아 <b>그 뒤 모든
     * 트리거가 429</b> 가 되는데, 재기동으로도 안 풀린다 — 그래서 {@code abandon} 이 있다.
     */
    @Test
    @DisplayName("도는 실행이 있으면 429 — abandon 으로 풀린다")
    void refusesWhileAnExecutionIsRunningAndAbandonClearsIt() throws Exception {
        JobParameters parameters = new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF.minusDays(1))
                .addString("dataset", "CLEAN")
                .addString("scope", "FULL")
                .addLong("attempt", 1L)
                .toJobParameters();
        // 프로세스가 죽어 STARTED 로 남은 행을 그대로 재현한다.
        JobExecution stranded = jobRepository.createJobExecution(
                jobRepository.createJobInstance(VerifyJobConfig.JOB_NAME, parameters),
                parameters,
                new org.springframework.batch.infrastructure.item.ExecutionContext());
        stranded.setStatus(BatchStatus.STARTED);
        // **시작 시각을 과거로 둔다.** 하드킬은 두 시간 전에도 일어난다 — 그리고
        // `stop` 이 **진도가 멈춘 실행만** 받으므로(CY-678), 방금 만든 행으로는
        // 복구 경로를 못 밟는다. `RunningJobProbe.lastProgress` 는 StepExecution 이
        // 없으면 START_TIME 으로 폴백하므로 이것이 곧 "두 시간째 진도 없음" 이다.
        // 임계(batch.stuck-job-after-ms) 자체를 낮추는 길은 막혀 있다 — 기동 가드가
        // "가장 긴 Step 데드라인보다 커야 한다" 로 거절한다.
        stranded.setStartTime(LocalDateTime.now().minusHours(2));
        jobRepository.update(stranded);

        HttpResponse<String> blocked = probe.post("/api/v1/admin/verify?asOf=" + AS_OF);
        assertThat(blocked.statusCode())
                .as("도는 것이 있으면 접수를 거절해야 한다. 본문=%s", blocked.body())
                .isEqualTo(429);
        assertThat(error(blocked).path("code").asText()).isEqualTo("VERIFICATION-013");
        // 규약이 응답 문구를 카탈로그 메시지로 제한하므로 id 는 본문에 안 실린다.
        // 대신 조회 경로가 그것을 준다 — 없으면 abandon 을 부를 방법이 없다.
        assertThat(json(probe.get("/api/v1/admin/verify/runs/running")).path("data").toString())
                .as("막고 있는 id 를 얻을 경로가 없으면 사람이 DB 를 뒤져야 하는데 "
                        + "업무 포트는 기본으로 안 열려 있다")
                .contains(String.valueOf(stranded.getId()));

        // STOPPING 이 되기 전에는 못 버린다.
        assertThat(probe.post("/api/v1/admin/verify/runs/" + stranded.getId() + "/abandon")
                .statusCode())
                .as("STARTED 를 바로 버리면 살아 있을지도 모르는 실행을 죽인다")
                .isEqualTo(409);

        // stop → abandon 2단계.
        assertThat(probe.post("/api/v1/admin/verify/runs/" + stranded.getId() + "/stop")
                .statusCode()).isEqualTo(202);
        HttpResponse<String> abandoned =
                probe.post("/api/v1/admin/verify/runs/" + stranded.getId() + "/abandon");
        assertThat(abandoned.statusCode())
                .as("stop 이 이미 STOPPED 로 올려 429 는 풀렸지만, 그 실행은 '멈췄다' 로 "
                        + "남는다 — abandon 이 '없던 것으로 한다' 를 마저 한다. "
                        + "그리고 완료 동작이라 202 가 아니라 200 이다")
                .isEqualTo(200);
        assertThat(json(abandoned).path("data").path("status").asText()).isEqualTo("ABANDONED");

        assertThat(probe.post("/api/v1/admin/verify?asOf=" + AS_OF).statusCode())
                .as("버린 뒤에는 다시 접수돼야 한다. 안 그러면 하드킬 한 번에 트리거가 영구히 막힌다")
                .isEqualTo(202);
    }

    /**
     * <b>이미 끝난 실행은 버릴 것이 없다.</b> Spring Batch 는 {@code FAILED}·{@code STOPPED}
     * 까지 통과시켜 {@code ABANDONED} 로 덮어쓰고 {@code END_TIME} 을 현재로 다시 쓴다 —
     * 이 저장소는 실행 이력을 판정 근거로 삼으므로 그것은 <b>증거를 조용히 바꾸는 일</b>이다.
     * 그리고 끝난 실행은 애초에 트리거를 막지도 않는다.
     */
    @Test
    @DisplayName("끝난 실행을 버리려 하면 409 — 이력을 덮어쓰지 않는다")
    void refusesToAbandonAFinishedExecution() throws Exception {
        long executionId = trigger(AS_OF).executionId();
        awaitTerminal(executionId);

        HttpResponse<String> response =
                probe.post("/api/v1/admin/verify/runs/" + executionId + "/abandon");

        assertThat(response.statusCode()).isEqualTo(409);
        assertThat(error(response).path("code").asText()).isEqualTo("VERIFICATION-016");
    }

    /**
     * <b>헤더의 상태와 본문의 {@code status} 가 같아야 한다.</b> {@code ErrorResponse.of} 는
     * 본문 상태를 {@code errorCode.getStatus()} 에서 읽으므로, 실제 상태와 다른 코드를 고르면
     * <b>헤더와 본문이 갈린다</b> — 클라이언트가 둘 중 무엇을 믿어야 할지 모른다.
     *
     * <p>도메인 예외는 코드 자신이 상태를 들고 있어 어긋날 여지가 없다. 위험한 것은
     * <b>스프링이 상태를 정한 웹 예외</b>를 우리 카탈로그 코드로 옮길 때다.
     */
    @Test
    @DisplayName("헤더와 본문의 status 가 같다")
    void keepsTheBodyStatusInSyncWithTheHeader() throws Exception {
        // 400 — 스프링이 정한 상태(파라미터 누락)를 우리 코드로 옮기는 경로
        HttpResponse<String> missing = probe.post("/api/v1/admin/verify");
        assertThat(missing.statusCode()).isEqualTo(400);
        assertThat(error(missing).path("status").asInt()).isEqualTo(400);

        // 404 · 409 — 도메인 코드가 상태를 들고 있는 경로
        HttpResponse<String> notFound = probe.get("/api/v1/admin/verify/runs/999999");
        assertThat(notFound.statusCode()).isEqualTo(404);
        assertThat(error(notFound).path("status").asInt()).isEqualTo(404);
    }

    private TriggerAccepted triggerCorrupt() throws Exception {
        HttpResponse<String> response = probe.post(
                "/api/v1/admin/verify?asOf=" + AS_OF + "&dataset=CORRUPT&seedRunId=1");
        assertThat(response.statusCode())
                .as("접수는 통과해야 한다 — 잡 안의 가드가 잡는 것을 보려는 것이다. 본문=%s",
                        response.body())
                .isEqualTo(202);
        return body(response, TriggerAccepted.class);
    }

    private TriggerAccepted trigger(LocalDateTime asOf) throws Exception {
        HttpResponse<String> response =
                probe.post("/api/v1/admin/verify?asOf=" + asOf);
        assertThat(response.statusCode())
                .as("접수는 202 다. 200 이면 동기로 끝났다는 뜻이고, 그러면 300만 전수에서 "
                        + "HTTP 타임아웃이 판정 시간의 상한을 대신 정하게 된다. 본문=%s",
                        response.body())
                .isEqualTo(202);
        return body(response, TriggerAccepted.class);
    }

    private VerifyRunView find(long executionId) throws Exception {
        HttpResponse<String> response =
                probe.get("/api/v1/admin/verify/runs/" + executionId);
        assertThat(response.statusCode()).isEqualTo(200);
        return body(response, VerifyRunView.class);
    }



    /**
     * <b>선점문이 되살아난 실행을 거부한다.</b> {@code requireStuck} 의 판정과
     * {@code stop} 의 쓰기 사이에 청크가 커밋되면 그 실행은 <b>살아 있다</b>. 그때 멈추면
     * 스레드는 도는데 DB 는 {@code STOPPED} 가 되고, 그 순간부터 만료·정리가 이 실행의
     * 입력을 건드린다 — V1·V3·V5 가 반쯤 쓰인 상태를 읽고 조용히 틀린 답을 낸다.
     *
     * <p><b>흐름으로는 못 잰다.</b> 판정과 쓰기 사이는 마이크로초라 밖에서 벌릴 수 없다.
     * 그래서 {@code ExpireRecoveryTest.claimRefusesARevivedRun} 과 같이 <b>문장 자체</b>를
     * 잰다.
     *
     * <p>⚠️ <b>좌표계를 추측하지 않는다.</b> {@code LAST_UPDATED} 는 프레임워크가 JVM 기본
     * 존으로 찍고 MySQL 의 {@code NOW()} 는 컨테이너 존이라 둘이 아홉 시간 어긋난다.
     * 그래서 선점문이 비교에 쓰는 값({@code stuckBefore}) 자체를 기준으로 민다.
     */
    @Test
    @DisplayName("선점문이 되살아난 실행을 거부한다 — 판정과 쓰기 사이의 창을 닫는다")
    void claimRefusesARevivedRun() throws Exception {
        try (RunningJobFixture dead = RunningJobFixture.plant(
                jobRepository, jdbcClient, VerifyJobConfig.JOB_NAME,
                LocalDateTime.of(2026, 5, 1, 0, 0).plusHours(41),
                Duration.ofHours(2), Duration.ofHours(2))) {

            LocalDateTime stuckBefore = runningJobs
                    .stuckExecutions(VerifyJobConfig.JOB_NAME).stream()
                    .filter(run -> run.execution().getId() == dead.executionId())
                    .findFirst().orElseThrow().stuckBefore();

            int revived = jdbcClient.sql("UPDATE BATCH_STEP_EXECUTION SET LAST_UPDATED = :at "
                            + "WHERE JOB_EXECUTION_ID = :id")
                    .param("at", stuckBefore.plusMinutes(1))
                    .param("id", dead.executionId()).update();
            assertThat(revived).as("픽스처가 Step 을 심었어야 한다").isEqualTo(1);

            int claimed = jdbcClient.sql(VerifyStopService.CLAIM)
                    .param("id", dead.executionId())
                    .param("stuckBefore", stuckBefore)
                    .update();

            assertThat(claimed)
                    .as("진도 조건이 없으면 살아 있는 검증을 STOPPED 로 올린다")
                    .isZero();
            assertThat(jobRepository.getJobExecution(dead.executionId()).getStatus())
                    .isEqualTo(BatchStatus.STARTED);
        }
    }

    /**
     * 잡이 비동기라 <b>종단 상태</b>까지 기다린다. 못 가면 {@code AssertionError} 로 끝난다.
     *
     * <p><b>"STARTED 가 아니다" 로 판정하면 안 된다.</b> 시작 전 상태인 {@code STARTING} 을
     * 종료로 오인해서, 느린 러너에서만 아직 안 끝난 잡의 상태를 단언하게 된다 — CI 에서만
     * 깨지는 플레이크를 실제로 그렇게 만들었다. 종단 목록을 명시한다.
     */
    private void awaitTerminal(long executionId) throws Exception {
        awaitUntil(Duration.ofSeconds(90), "잡이 끝나지 않았습니다. executionId=" + executionId,
                () -> {
                    try {
                        return TERMINAL.contains(find(executionId).status());
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                });
    }
}
