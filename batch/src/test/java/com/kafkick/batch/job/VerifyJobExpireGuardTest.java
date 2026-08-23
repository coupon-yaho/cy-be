package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

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
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.batch.config.RunningJobFixture;
import com.kafkick.batch.config.RunningJobProbe;
import com.kafkick.core.coupon.IssuanceEventType;
import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>만료가 도는 중이면 검증하지 않는다 — 스케줄러 플래그와 무관하게.</b>
 *
 * <p>만료는 재고를 쓰는 유일한 배치다. 검증 중에 지나가면 V3 가 갱신된 행을 조용히 빼고,
 * 같은 {@code asOf} 두 실행이 다른 답을 낸다.
 *
 * <p><b>예전에는 {@code batch.scheduling.enabled} 를 봤다.</b> 그것은 <i>"만료 스케줄러 빈이
 * 만들어졌는가"</i>라 운영처럼 늘 켜 두는 환경에서는 <b>검증이 영영 못 돌았다.</b>
 * 그래서 이 클래스는 <b>스케줄러를 켜 둔 채</b> 돈다 — 플래그가 켜져 있어도 만료가 실제로
 * 안 돌면 검증이 지나가야 한다는 것이 이 티켓의 본체이고, 그것을 두 번째 테스트가 잰다.
 * 크론은 먼 미래로 밀어 진짜 만료가 끼어들지 않게 한다.
 *
 * <p><b>시체 판정은 나이가 아니라 진도로 한다.</b> 그래서 <i>"오래 도는 만료"</i>와
 * <i>"죽은 만료"</i>가 갈리는지를 두 테스트로 따로 잰다 — 그 둘을 나이로 자르면
 * <b>가드가 가장 필요한 순간(만료가 밀려 오래 도는 날)에 스스로 꺼진다.</b>
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=true",
        "batch.schedule.expire-cron=0 0 0 1 1 *",
        // 테스트가 이 값을 직접 계산에 쓴다. 기본값(10분)에 기대면 기본값이 바뀔 때
        // 이 클래스가 왜 깨지는지 알 수 없다.
        // step-timeout(600000)보다 커야 한다 — RunningJobProbe 생성자가 검사한다.
        "batch.stuck-job-after-ms=1800000"
})
@Import(MySqlContainerConfig.class)
class VerifyJobExpireGuardTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);
    private static final String GUARD_MESSAGE = "만료 배치가 실행 중이라 검증할 수 없습니다";
    private static final Duration STUCK_AFTER = Duration.ofMinutes(30);

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private Job verifyJob;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private RunningJobProbe runningJobs;

    @Autowired
    private JdbcClient jdbcClient;

    @BeforeEach
    void setUp() {
        new JobRepositoryTestUtils(jobRepository).removeJobExecutions();
        new VerificationSeed(jdbcClient).clear();
    }

    @Test
    @DisplayName("만료가 도는 중이면 검증을 시작하지 않는다")
    void rejectWhileExpireIsRunning() throws Exception {
        seedOneIssuance();

        try (RunningJobFixture expire = plantExpire(AS_OF.minusDays(1))) {
            JobExecution execution = jobOperator.start(verifyJob, verifyParameters(1L));

            assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
            assertThat(failureMessagesOf(execution))
                    .as("가드가 죽으면 여기가 COMPLETED 로 바뀐다")
                    .anyMatch(message -> message.contains(GUARD_MESSAGE));
            assertThat(failureMessagesOf(execution))
                    .as("막고 있는 실행 id 를 문구에 담아야 운영자가 무엇을 기다리는지 안다")
                    .anyMatch(message -> message.contains(String.valueOf(expire.executionId())));
            assertThat(runRowCount())
                    .as("가드는 실행 행을 만들기 전에 걸려야 한다 — 뒤면 attempt 를 태운다")
                    .isZero();
        }
    }

    /**
     * <b>이 티켓이 여는 문이다.</b> 이 테스트가 없으면 가드를 실행 중 검사로 바꾼 의미가
     * 하나도 증명되지 않는다 — 위 테스트만으로는 예전 플래그 검사도 똑같이 통과한다.
     */
    @Test
    @DisplayName("스케줄러가 켜져 있어도 만료가 안 돌면 검증이 끝까지 간다")
    void passWhenNoExpireIsRunningEvenThoughSchedulingIsOn() throws Exception {
        seedOneIssuance();

        JobExecution execution = jobOperator.start(verifyJob, verifyParameters(2L));

        assertThat(failureMessagesOf(execution))
                .as("batch.scheduling.enabled 는 이제 이 가드와 무관하다")
                .noneMatch(message -> message.contains(GUARD_MESSAGE));
        assertThat(execution.getStatus())
                .as("정상 발급 한 건짜리 CLEAN 셋이라 전수 검증이 끝까지 간다. 여기가 FAILED 면 "
                        + "문이 열린 것이 아니라 다른 곳이 막힌 것이다")
                .isEqualTo(BatchStatus.COMPLETED);
        assertThat(runRowCount()).isEqualTo(1);
    }

    /**
     * <b>느린 만료를 죽었다고 판정하면 안 된다.</b> 한때 이 판정이 나이 기준이었고, 임계로
     * {@code BatchJobRunningTooLong} 의 300초를 그대로 썼다 — 그런데 그 알림이 우는 상태는
     * 정의상 <b>"살아서 느리게 돌고 있다"</b>이다. 즉 <b>가드가 가장 필요한 순간에 정확히
     * 꺼지는</b> 구조였다. 300만 적재 직후 첫 만료가 그 상황이다.
     */
    @Test
    @DisplayName("오래 돌아도 진도가 나가는 만료는 검증을 막는다")
    void longRunningButProgressingExpireStillBlocks() throws Exception {
        seedOneIssuance();

        // 한 시간 전에 시작했지만 방금 청크를 커밋했다.
        try (RunningJobFixture slowExpire = RunningJobFixture.plant(
                jobRepository, jdbcClient, ExpireJobConfig.JOB_NAME, AS_OF.minusDays(2),
                Duration.ofHours(1), Duration.ZERO)) {

            JobExecution execution = jobOperator.start(verifyJob, verifyParameters(3L));

            assertThat(failureMessagesOf(execution))
                    .as("실행 id=" + slowExpire.executionId()
                            + " — 시작한 지 오래됐다는 것과 죽었다는 것은 다른 사건이다")
                    .anyMatch(message -> message.contains(GUARD_MESSAGE));
            assertThat(runRowCount()).isZero();
        }
    }

    /**
     * 프로세스가 종료 표시를 못 남기고 죽으면 {@code STARTED} 행이 <b>영원히</b> 남는다 —
     * 조회에 {@code END_TIME} 검사도 시간 상한도 없기 때문이다. 상한이 없으면
     * {@code docker compose down} 이 만료 한복판에 한 번 걸린 것만으로 <b>검증이 그 뒤로
     * 영영 거절된다.</b> 게다가 만료 실행에는 {@code abandon} 경로가 없다.
     */
    @Test
    @DisplayName("진도가 멈춘 만료 실행은 검증을 막지 않는다")
    void stuckExecutionDoesNotBlock() throws Exception {
        seedOneIssuance();

        Duration stalled = STUCK_AFTER.plusSeconds(30);
        try (RunningJobFixture corpse = RunningJobFixture.plant(
                jobRepository, jdbcClient, ExpireJobConfig.JOB_NAME, AS_OF.minusDays(3),
                stalled, stalled)) {
            JobExecution execution = jobOperator.start(verifyJob, verifyParameters(4L));

            assertThat(failureMessagesOf(execution))
                    .as("실행 id=" + corpse.executionId() + " 은 진도가 멈췄다")
                    .noneMatch(message -> message.contains(GUARD_MESSAGE));
            assertThat(runRowCount()).isEqualTo(1);
        }
    }

    /**
     * <b>{@code STARTING} 에서 죽으면 Step 이 하나도 없다.</b> 그때 진도를 물을 곳이 없으므로
     * 프로브가 실행 시작 시각으로 물러난다. 그 폴백이 없으면 이런 행이 <b>영원히</b> 막고,
     * 만료에는 해제 경로가 없어 SQL 을 직접 치는 수밖에 없다.
     */
    @Test
    @DisplayName("Step 이 없는 실행은 시작 시각으로 나이를 잰다")
    void executionWithoutStepsFallsBackToStartTime() {
        LocalDateTime longAgo = LocalDateTime.now().minus(STUCK_AFTER).minusSeconds(30);

        try (RunningJobFixture fresh = RunningJobFixture.plantWithoutStep(
                     jobRepository, ExpireJobConfig.JOB_NAME, AS_OF.minusDays(4),
                     LocalDateTime.now());
             RunningJobFixture corpse = RunningJobFixture.plantWithoutStep(
                     jobRepository, ExpireJobConfig.JOB_NAME, AS_OF.minusDays(5), longAgo)) {

            assertThat(runningJobs.blockingExecutions(ExpireJobConfig.JOB_NAME))
                    .as("방금 뜬 것은 막고, 진도 없이 오래된 것은 무시한다")
                    .containsExactly(fresh.executionId());
            assertThat(corpse.executionId()).isNotEqualTo(fresh.executionId());
        }
    }

    /**
     * 문구에 싣는 값이라 순서가 흔들리면 같은 상황에서 다른 메시지가 나온다.
     * {@code javadoc} 이 그 이유까지 적었으므로 계약으로 본다.
     *
     * <p>⚠️ <b>이 테스트만으로는 정렬을 지워도 초록일 수 있다</b> — 조회가 이미 id 순으로
     * 돌려주면 우연히 통과한다. 그래서 <b>나중에 심은 것의 id 가 더 크다</b>는 것을 함께
     * 단언해, 최소한 "역순으로 나오면 빨개진다" 를 보장한다.
     */
    @Test
    @DisplayName("막는 실행이 둘이면 id 오름차순으로 준다")
    void blockingExecutionsAreSortedById() {
        try (RunningJobFixture first = plantExpire(AS_OF.minusDays(6));
             RunningJobFixture second = plantExpire(AS_OF.minusDays(7))) {

            assertThat(second.executionId())
                    .as("나중에 심은 것이 더 큰 id 를 받아야 이 테스트가 정렬을 잰다")
                    .isGreaterThan(first.executionId());

            assertThat(runningJobs.blockingExecutions(ExpireJobConfig.JOB_NAME))
                    .containsExactly(first.executionId(), second.executionId());
        }
    }

    /** 방금 떠서 방금 진도를 낸 만료. */
    private RunningJobFixture plantExpire(LocalDateTime key) {
        return RunningJobFixture.plant(jobRepository, jdbcClient, ExpireJobConfig.JOB_NAME, key);
    }

    private void seedOneIssuance() {
        VerificationSeed seed = new VerificationSeed(jdbcClient);
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED);
        seed.history(issuanceId, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, AS_OF.minusHours(1));
    }

    /** {@code attempt} 를 갈라 준다. Spring Batch 는 같은 파라미터로 두 번 돌지 않는다. */
    private static JobParameters verifyParameters(long attempt) {
        return new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .addString("scope", "FULL")
                .addString("dataset", "CLEAN")
                .addLong("attempt", attempt)
                .toJobParameters();
    }

    private int runRowCount() {
        return jdbcClient.sql("SELECT COUNT(*) FROM verification_runs")
                .query(Integer.class).single();
    }

    private static List<String> failureMessagesOf(JobExecution execution) {
        List<String> messages = new ArrayList<>();
        for (Throwable failure : execution.getAllFailureExceptions()) {
            for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
                messages.add(String.valueOf(cause.getMessage()));
            }
        }
        return messages;
    }
}
