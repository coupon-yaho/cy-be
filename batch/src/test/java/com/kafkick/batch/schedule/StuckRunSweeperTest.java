// 진도가 멈춘 실행이 사람 손 없이 걷히는지 확인합니다.
package com.kafkick.batch.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import io.micrometer.core.instrument.MeterRegistry;

import com.kafkick.batch.api.StuckRunSweepService;
import com.kafkick.batch.config.ExpireStepContext;
import com.kafkick.batch.config.RunningJobFixture;
import com.kafkick.batch.config.RunningJobProbe;
import com.kafkick.batch.config.RunningJobProbe.StuckRun;
import com.kafkick.batch.job.CleanupJobConfig;
import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>탐지는 세 겹이었는데 조치가 없었다.</b> 회수 경로 다섯의 호출자를 전수로 세면 전부
 * 컨트롤러다 — 새벽에 배치가 하드킬로 죽으면 알림만 울고 아무 일도 안 일어난다.
 * 사전예약 PRD FR-C-01 의 수용 기준이 <i>"자동으로 해소됩니다"</i> 인 자리다.
 *
 * <p><b>스케줄러를 켜지 않고 손으로 부른다.</b> 켜면 공유 컨테이너에서 60초마다 돌면서
 * <b>다른 테스트가 심어 둔 시체를 걷는다</b> — 그쪽 테스트가 이유 없이 빨개진다.
 * 배선 자체는 {@link WhenScheduled} 가 따로 잰다.
 */
@SpringBootTest(properties = {
        "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        // 되읽기가 같은 배치 메타를 훑는다. 심은 행과 섞이지 않게 창을 닫는다.
        "batch.metrics.expire-pending-initial-delay-ms=3600000",
        "batch.metrics.run-refresh-ms=120000"
})
@Import(MySqlContainerConfig.class)
class StuckRunSweeperTest {

    /** {@code batch.stuck-job-after-ms} 기본이 30분이다. 넉넉히 넘긴다. */
    private static final Duration DEAD = Duration.ofHours(2);

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private StuckRunSweepService sweepService;

    @Autowired
    private RunningJobProbe runningJobs;

    @Autowired
    private List<Job> jobs;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private MeterRegistry registry;

    private StuckRunSweeper sweeperWithCap(int cap) {
        return sweeper(sweepService, runningJobs, cap);
    }

    private StuckRunSweeper sweeper(StuckRunSweepService service, RunningJobProbe probe,
            int cap) {
        return sweeper(service, probe, cap, true);
    }

    private StuckRunSweeper sweeper(StuckRunSweepService service, RunningJobProbe probe,
            int cap, boolean enabled) {
        return new StuckRunSweeper(service, probe, jobs, transactionManager, cap, enabled,
                60_000L, 5);
    }

    private double counter(String name) {
        return registry.find(name).counter() == null ? 0d
                : registry.find(name).counter().count();
    }

    private BatchStatus statusOf(long executionId) {
        return jobRepository.getJobExecution(executionId).getStatus();
    }

    /**
     * <b>이 테스트가 이 티켓의 전부다.</b> 아무도 API 를 안 눌렀는데 시체가 닫혀야 한다.
     *
     * <p><b>{@code FAILED} 인 것까지 본다.</b> {@code ABANDONED} 로 닫으면
     * {@code TaskExecutorJobLauncher} 가 그것을 {@code COMPLETED} 와 같이 막아
     * <b>그 {@code JobInstance} 를 같은 파라미터로 영원히 못 돌린다</b> — 만료는
     * {@code asOf} 가 식별 파라미터라 그 크론 슬롯이 통째로 사라진다.
     * 상태 이름 하나가 그 차이를 진다.
     */
    @Test
    @DisplayName("스윕 한 번이 시체를 FAILED 로 닫는다 — 아무도 안 눌렀다")
    void sweepClosesTheCorpseWithoutAnyone() {
        try (RunningJobFixture corpse = RunningJobFixture.plant(jobRepository, jdbcClient,
                ExpireStepContext.JOB_NAME, LocalDateTime.now(), DEAD, DEAD)) {
            assertThat(statusOf(corpse.executionId()))
                    .as("전제가 무너지면 아래 단언이 아무것도 안 잰다")
                    .isEqualTo(BatchStatus.STARTED);

            sweeperWithCap(20).sweep();

            assertThat(statusOf(corpse.executionId()))
                    .as("ABANDONED 면 그 JobInstance 를 같은 asOf 로 영원히 못 돌린다")
                    .isEqualTo(BatchStatus.FAILED);
            assertThat(jobRepository.getJobExecution(corpse.executionId()).getEndTime())
                    .as("END_TIME 이 없으면 findRunningJobExecutions 는 안 걸러도 "
                            + "다음 스윕이 같은 행을 또 만난다")
                    .isNotNull();
            assertThat(runningJobs.stuckExecutions(ExpireStepContext.JOB_NAME))
                    .as("걷었으면 판정에서 사라져야 한다 — 그래야 알림도 내려간다")
                    .noneMatch(run -> run.execution().getId() == corpse.executionId());
        }
    }

    /**
     * <b>스윕이 지켜야 할 반대편이다.</b> 진도가 나가는 잡을 걷으면 그것은 복구가 아니라
     * 사고다 — 자동이라 사람이 막을 기회도 없다.
     *
     * <p>{@code startedAgo} 는 {@code DEAD} 인데 {@code progressAgo} 는 0 이다.
     * <b>오래 돌지만 살아 있는 잡</b>이고, 판정이 시작 시각이 아니라 마지막 진도를 본다는
     * 것이 이 두 인자가 갈리는 이유다.
     */
    @Test
    @DisplayName("오래 돌아도 진도가 있으면 안 건드린다")
    void sweepLeavesALiveRunAlone() {
        try (RunningJobFixture live = RunningJobFixture.plant(jobRepository, jdbcClient,
                ExpireStepContext.JOB_NAME, LocalDateTime.now(), DEAD, Duration.ZERO)) {

            sweeperWithCap(20).sweep();

            assertThat(statusOf(live.executionId()))
                    .as("두 시간째 도는 만료를 스윕이 죽이면 그날 만료가 통째로 밀린다")
                    .isEqualTo(BatchStatus.STARTED);
        }
    }

    /**
     * <b>상한이 정말 끊는지.</b> 안 끊으면 배치 메타 전체가 시체인 날 한 주기가 수백 건의
     * 트랜잭션을 열고, 그동안 손으로 걷으려는 운영자가 락 대기에 걸린다.
     *
     * <p>둘을 <b>서로 다른 잡</b>에 심는다. 한 잡 안에서 자르는 것과 잡을 넘어가며 자르는
     * 것은 다른 갈래이고, 바깥 루프의 예산 검사는 후자에서만 탄다.
     */
    @Test
    @DisplayName("상한을 넘으면 그 수만큼만 걷고 나머지는 다음 주기로 넘긴다")
    void sweepStopsAtTheCap() {
        LocalDateTime key = LocalDateTime.now();
        try (RunningJobFixture first = RunningJobFixture.plant(jobRepository, jdbcClient,
                CleanupJobConfig.JOB_NAME, key, DEAD, DEAD);
                RunningJobFixture second = RunningJobFixture.plant(jobRepository, jdbcClient,
                        ExpireStepContext.JOB_NAME, key, DEAD, DEAD)) {

            sweeperWithCap(1).sweep();

            // 이름 순 정렬이라 cleanupJob 이 먼저다. 그 순서를 못 박는 것이 아니라
            // **정확히 하나만** 닫혔는지를 본다 — 순서를 단언하면 잡이 늘 때 깨진다.
            assertThat(List.of(statusOf(first.executionId()), statusOf(second.executionId())))
                    .as("상한이 안 먹으면 둘 다 FAILED 다")
                    .containsExactlyInAnyOrder(BatchStatus.FAILED, BatchStatus.STARTED);
        }
    }

    /**
     * <b>상한의 안쪽 분기.</b> 위 테스트는 잡을 넘어갈 때만 자르므로 <b>한 잡 안에서
     * 자르는 갈래는 한 번도 안 탄다</b> — 그 분기를 지워도 위 테스트는 초록이다.
     * 시체가 몰리는 것은 대개 한 잡이라(그 잡이 매 회차 죽는다) 실전에서 먼저 타는 쪽이
     * 오히려 이쪽이다.
     */
    @Test
    @DisplayName("한 잡 안에서도 상한에서 끊는다")
    void sweepStopsAtTheCapInsideASingleJob() {
        LocalDateTime now = LocalDateTime.now();
        try (RunningJobFixture first = RunningJobFixture.plant(jobRepository, jdbcClient,
                CleanupJobConfig.JOB_NAME, now, DEAD, DEAD);
                RunningJobFixture second = RunningJobFixture.plant(jobRepository, jdbcClient,
                        CleanupJobConfig.JOB_NAME, now.plusSeconds(1), DEAD, DEAD)) {

            sweeperWithCap(1).sweep();

            assertThat(List.of(statusOf(first.executionId()), statusOf(second.executionId())))
                    .as("안쪽 분기가 없으면 한 잡의 시체를 전부 걷어 둘 다 FAILED 다")
                    .containsExactlyInAnyOrder(BatchStatus.FAILED, BatchStatus.STARTED);
        }
    }

    /**
     * <b>한 잡에서 끊겨도 나머지를 본다.</b> 배치 메타 조회가 데드라인을 넘겨 죽는 것이
     * 다른 잡의 시체를 못 걷을 이유가 아니다. 실제로 그 조회는 시체 수에 비례해 비싸지므로
     * (실행마다 DAO 세 번), <b>가장 아픈 잡에서 먼저 끊긴다.</b>
     */
    @Test
    @DisplayName("한 잡의 조회가 던져도 나머지 잡은 계속 본다")
    void oneFailingJobDoesNotStopTheRest() {
        RunningJobProbe flaky = mock(RunningJobProbe.class);
        when(flaky.stuckExecutions(anyString())).thenReturn(List.of());
        when(flaky.stuckExecutions(eq(CleanupJobConfig.JOB_NAME)))
                .thenThrow(new IllegalStateException("배치 메타 조회가 끊겼다"));

        try (RunningJobFixture corpse = RunningJobFixture.plant(jobRepository, jdbcClient,
                ExpireStepContext.JOB_NAME, LocalDateTime.now(), DEAD, DEAD)) {
            StuckRun planted = runningJobs.stuckExecutions(ExpireStepContext.JOB_NAME).stream()
                    .filter(run -> run.execution().getId() == corpse.executionId())
                    .findFirst()
                    .orElseThrow();
            when(flaky.stuckExecutions(eq(ExpireStepContext.JOB_NAME)))
                    .thenReturn(List.of(planted));

            sweeper(sweepService, flaky, 20).sweep();

            assertThat(statusOf(corpse.executionId()))
                    .as("cleanupJob 이 던졌다고 expireJob 의 시체가 남으면 안 된다")
                    .isEqualTo(BatchStatus.FAILED);
        }
    }

    /**
     * <b>선점에 지는 것은 오류가 아니다.</b> 스윕은 아무도 시키지 않았으므로, 그 사이 잡이
     * 진도를 냈거나 운영자가 먼저 눌렀으면 그것이 정상이다. 던지면 그 한 건이 나머지
     * 스윕을 끊는다 — 형제({@code ExpireRecoveryService})가 던지는 것과 답이 갈리는 자리다.
     */
    @Test
    @DisplayName("이미 닫힌 실행에는 두 번째 스윕이 조용히 아무것도 안 한다")
    void losingTheClaimIsNotAnError() {
        try (RunningJobFixture corpse = RunningJobFixture.plant(jobRepository, jdbcClient,
                ExpireStepContext.JOB_NAME, LocalDateTime.now(), DEAD, DEAD)) {
            JobExecution before = jobRepository.getJobExecution(corpse.executionId());
            StuckRun planted = runningJobs.stuckExecutions(ExpireStepContext.JOB_NAME).stream()
                    .filter(run -> run.execution().getId() == corpse.executionId())
                    .findFirst()
                    .orElseThrow();

            assertThat(sweepService.recover(before.getId(), planted.stuckBefore()))
                    .as("첫 번째는 걷어야 한다")
                    .isTrue();
            assertThat(sweepService.recover(before.getId(), planted.stuckBefore()))
                    .as("두 번째는 던지지 않고 false 다")
                    .isFalse();
        }
    }

    /**
     * <b>돌연변이가 살아남아서 생긴 테스트다.</b> {@code recover} 의 선점문에서
     * {@code stuckBefore} 를 <b>먼 미래로 바꿔도</b> 위 테스트가 전부 초록이었다 —
     * 살아 있는 실행은 {@code stuckExecutions} 필터에 걸려 서비스까지 <b>오지도 않기</b>
     * 때문이다. 즉 <b>자바 검사만 재고 SQL 조건은 한 번도 안 쟀다.</b>
     *
     * <p>그런데 그 SQL 조건이 지키는 것은 자바 검사가 못 지키는 구간이다 — 판정과 쓰기
     * 사이에 실행이 되살아나는 창({@code StuckRunClaim} 이 적어 둔 사실: 락 대기가 풀리는
     * 경우가 그렇다). 그 창을 밖에서 결정적으로 만들 수는 없으므로, <b>서비스를 직접 부르되
     * 판정이 안 덮는 시각을 준다</b> — 되살아난 실행이 선점문에 오는 것과 같은 모양이다.
     */
    @Test
    @DisplayName("판정이 안 덮는 시각으로 부르면 선점이 살아 있는 실행을 안 닫는다")
    void theClaimItselfRefusesARunThatHasProgressed() {
        try (RunningJobFixture live = RunningJobFixture.plant(jobRepository, jdbcClient,
                ExpireStepContext.JOB_NAME, LocalDateTime.now(), DEAD, Duration.ZERO)) {

            // 이 실행의 마지막 진도는 "방금" 이다. 한 시간 전을 컷오프로 주면 선점문의
            // `진도 <= :stuckBefore` 가 거짓이라 0행이어야 한다.
            boolean closed = sweepService.recover(live.executionId(),
                    LocalDateTime.now().minusHours(1));

            assertThat(closed)
                    .as("선점문이 진도 조건을 안 걸면 여기가 true 가 되고, 그것이 곧 "
                            + "되살아난 잡을 스윕이 죽이는 경로다")
                    .isFalse();
            assertThat(statusOf(live.executionId())).isEqualTo(BatchStatus.STARTED);
        }
    }

    /**
     * <b>바깥 예산 검사가 무엇을 지는지.</b> 이것도 돌연변이가 살아남아서 생겼다 —
     * 지워도 결과는 같다(안쪽 검사가 곧바로 끊으므로). 그래서 이 검사가 지는 것은
     * <b>정정이 아니라 질의</b>다: {@code stuckExecutions} 는 실행마다 DAO 를 세 번,
     * Step 마다 한 번 더 부르는 비싼 조회라, 예산이 0 인데 남은 잡마다 그것을 부르는 것은
     * <b>배치 메타가 이미 아픈 날</b>에 부담을 더한다.
     *
     * <p>그래서 <b>결과가 아니라 호출</b>을 잰다. 결과로는 이 분기를 못 가른다.
     */
    @Test
    @DisplayName("예산이 떨어지면 남은 잡은 조회조차 안 한다")
    void anExhaustedBudgetSkipsTheRemainingQueries() {
        RunningJobProbe counting = mock(RunningJobProbe.class);
        when(counting.stuckExecutions(anyString())).thenReturn(List.of());

        try (RunningJobFixture corpse = RunningJobFixture.plant(jobRepository, jdbcClient,
                CleanupJobConfig.JOB_NAME, LocalDateTime.now(), DEAD, DEAD)) {
            StuckRun planted = runningJobs.stuckExecutions(CleanupJobConfig.JOB_NAME).stream()
                    .filter(run -> run.execution().getId() == corpse.executionId())
                    .findFirst()
                    .orElseThrow();
            // 이름 순으로 cleanupJob 이 먼저다. 그것 하나로 예산을 다 쓴다.
            when(counting.stuckExecutions(eq(CleanupJobConfig.JOB_NAME)))
                    .thenReturn(List.of(planted));

            sweeper(sweepService, counting, 1)
                    .sweep();

            verify(counting).stuckExecutions(CleanupJobConfig.JOB_NAME);
            verify(counting, never()).stuckExecutions(ExpireStepContext.JOB_NAME);
        }
    }

    /**
     * <b>회수 하나가 던지면 그 잡의 나머지가 어떻게 되나.</b> 한때 {@code try/catch} 가
     * <b>잡 단위</b>였다 — 그러면 이 한 건이 그 잡의 나머지 시체를 전부 이번 주기에서
     * 밀어내고, 목록이 id 오름차순이고 롤백으로 선점이 되돌아가므로 <b>다음 주기도 같은
     * 자리에서 끊긴다.</b> id 가 큰 시체는 영구히 안 걷힌다.
     *
     * <p>기존 {@code oneFailingJobDoesNotStopTheRest} 는 <b>조회</b>만 던지게 해서 이 갈래를
     * 못 잡았다 — 잡 단위 catch 와 건 단위 catch 를 구분하지 못한다.
     */
    @Test
    @DisplayName("회수 하나가 던져도 같은 잡의 다음 시체를 계속 본다")
    void oneFailingRecoveryDoesNotStopTheSameJob() {
        LocalDateTime now = LocalDateTime.now();
        try (RunningJobFixture first = RunningJobFixture.plant(jobRepository, jdbcClient,
                CleanupJobConfig.JOB_NAME, now, DEAD, DEAD);
                RunningJobFixture second = RunningJobFixture.plant(jobRepository, jdbcClient,
                        CleanupJobConfig.JOB_NAME, now.plusSeconds(1), DEAD, DEAD)) {

            long firstId = Math.min(first.executionId(), second.executionId());
            long secondId = Math.max(first.executionId(), second.executionId());

            StuckRunSweepService flaky = mock(StuckRunSweepService.class);
            when(flaky.recover(eq(firstId), any()))
                    .thenThrow(new IllegalStateException("이 한 건이 터졌다"));
            when(flaky.recover(eq(secondId), any())).thenReturn(true);

            sweeper(flaky, runningJobs, 20).sweep();

            verify(flaky).recover(eq(secondId), any());
            verify(flaky).recordFailure();
        }
    }

    /**
     * <b>지표가 정말 오르나.</b> 저장소에 {@code cy_batch_stuck_recovered_total} 의 증가를
     * 단언하는 자리가 하나도 없었다 — {@code increment()} 를 지우는 돌연변이가 전 테스트
     * 초록으로 살아남고, 그러면 {@code BatchStuckAutoRecovered} 가 영원히 안 운다.
     * {@code BatchMetricExposureTest} 는 이름의 <b>존재</b>만 보지 값은 안 본다.
     *
     * <p>커밋 뒤에 오르는 축은 {@link #theCounterDoesNotMoveWhenTheWriteIsRolledBack()} 가
     * 따로 진다 — 이 테스트만으로는 {@code countAfterCommit()} 을 그 자리 증가로 바꾸는
     * 돌연변이가 살아남는다(성공 경로에서는 두 구현이 같은 값을 낸다).
     */
    @Test
    @DisplayName("걷은 만큼 지표가 오른다")
    void theCounterFollowsWhatWasActuallyClosed() {
        double before = counter("cy_batch_stuck_recovered_total");
        try (RunningJobFixture corpse = RunningJobFixture.plant(jobRepository, jdbcClient,
                ExpireStepContext.JOB_NAME, LocalDateTime.now(), DEAD, DEAD)) {

            sweeperWithCap(20).sweep();

            assertThat(counter("cy_batch_stuck_recovered_total"))
                    .as("걷었는데 안 오르면 알림이 영원히 안 운다")
                    .isEqualTo(before + 1);
            assertThat(statusOf(corpse.executionId())).isEqualTo(BatchStatus.FAILED);
        }
    }

    /**
     * <b>롤백된 회수가 지표를 올리면 안 된다.</b> {@code recover} 는 {@code REQUIRED} 라
     * 바깥 트랜잭션이 있으면 그것에 합류한다 — 바깥이 뒤집히면 <b>배치 메타에 아무것도
     * 안 남는데</b> 그 자리에서 센 카운터는 이미 올라가 있다. 그러면
     * {@code BatchStuckAutoRecovered} 가 <i>"걷었다"</i> 고 말하는데 그 행은 그대로
     * {@code STARTED} 다 — 이 클래스의 논거(<i>"자동 조치가 조용하면 사고를 덮는다"</i>)의
     * <b>반대 방향</b>이고, 안 고친 것을 고쳤다고 말하는 쪽이 더 나쁘다.
     *
     * <p>성공 경로만으로는 두 구현이 같은 값을 내므로 이 갈래가 유일한 판별점이다.
     */
    @Test
    @DisplayName("회수가 롤백되면 지표가 안 오른다")
    void theCounterDoesNotMoveWhenTheWriteIsRolledBack() {
        double before = counter("cy_batch_stuck_recovered_total");
        try (RunningJobFixture corpse = RunningJobFixture.plant(jobRepository, jdbcClient,
                ExpireStepContext.JOB_NAME, LocalDateTime.now(), DEAD, DEAD)) {
            StuckRun planted = runningJobs.stuckExecutions(ExpireStepContext.JOB_NAME).stream()
                    .filter(run -> run.execution().getId() == corpse.executionId())
                    .findFirst()
                    .orElseThrow();

            // 바깥 트랜잭션을 열고 회수까지 시킨 뒤 뒤집는다. recover 는 REQUIRED 라
            // 여기 합류하므로 그 쓰기도 함께 사라진다.
            assertThatThrownBy(() -> new TransactionTemplate(transactionManager).execute(
                    status -> {
                        sweepService.recover(corpse.executionId(), planted.stuckBefore());
                        throw new IllegalStateException("바깥이 뒤집힌다");
                    }))
                    .isInstanceOf(IllegalStateException.class);

            assertThat(statusOf(corpse.executionId()))
                    .as("전제가 무너지면 아래 단언이 아무것도 안 잰다 — 롤백이 안 됐다는 뜻이다")
                    .isEqualTo(BatchStatus.STARTED);
            assertThat(counter("cy_batch_stuck_recovered_total"))
                    .as("안 걷혔는데 오르면 알림이 '고쳤다' 고 거짓말한다")
                    .isEqualTo(before);
        }
    }

    /**
     * <b>상한 경고가 마지막 잡에서도 나오나.</b> 바깥 검사에만 로그를 두면 이름 순 마지막
     * 잡에서 예산이 떨어질 때 <b>한 줄도 안 남는다</b> — 그런데 알림 설명이 운영자에게
     * 그 로그를 보라고 시킨다. 시체가 몰리는 것은 대개 한 잡이라 이쪽이 흔한 경우다.
     *
     * <p>로그를 단언하는 대신 <b>예산이 정말 거기서 끊겼는지</b>를 잰다 — 마지막 잡
     * (이름 순으로 {@code verifyJob})에 둘을 심고 상한을 1 로 준다.
     */
    @Test
    @DisplayName("이름 순 마지막 잡에서 예산이 떨어져도 상한이 끊는다")
    void theCapHoldsOnTheLastJobToo() {
        String last = jobs.stream().map(Job::getName).sorted()
                .reduce((a, b) -> b)
                .orElseThrow();
        LocalDateTime now = LocalDateTime.now();
        try (RunningJobFixture first = RunningJobFixture.plant(jobRepository, jdbcClient,
                last, now, DEAD, DEAD);
                RunningJobFixture second = RunningJobFixture.plant(jobRepository, jdbcClient,
                        last, now.plusSeconds(1), DEAD, DEAD)) {

            sweeperWithCap(1).sweep();

            assertThat(List.of(statusOf(first.executionId()), statusOf(second.executionId())))
                    .as("마지막 잡에서는 바깥 검사가 한 번도 안 탄다")
                    .containsExactlyInAnyOrder(BatchStatus.FAILED, BatchStatus.STARTED);
        }
    }

    /**
     * <b>앞 잡이 상한을 계속 먹으면 뒤 잡은 어떻게 되나.</b> 고정 순서 + 전역 상한이면
     * <b>뒤 잡은 조회조차 안 되고</b>, 예산이 0 이라 조회를 건너뛰므로 그 사실이
     * 지표에도 안 잡힌다 — 그 잡의 시체는 자동 회수 대상에서 조용히 빠진다.
     *
     * <p>여기서 재는 것은 <b>두 번째 주기가 다른 잡부터 보는가</b>다. 상한 1 로 두 주기를
     * 돌리면 회전이 없을 때 같은 잡을 두 번 보고, 있으면 각각 한 번씩 본다.
     */
    @Test
    @DisplayName("주기마다 시작 잡을 돌려 뒤쪽 잡이 안 굶는다")
    void theStartingJobRotatesSoLaterJobsAreNotStarved() {
        RunningJobProbe counting = mock(RunningJobProbe.class);
        when(counting.stuckExecutions(anyString())).thenReturn(List.of());

        LocalDateTime now = LocalDateTime.now();
        String first = jobs.stream().map(Job::getName).sorted().findFirst().orElseThrow();
        try (RunningJobFixture corpse = RunningJobFixture.plant(jobRepository, jdbcClient,
                first, now, DEAD, DEAD)) {
            StuckRun planted = runningJobs.stuckExecutions(first).stream()
                    .filter(run -> run.execution().getId() == corpse.executionId())
                    .findFirst()
                    .orElseThrow();
            // 첫 잡이 매 주기 상한을 통째로 먹는다.
            when(counting.stuckExecutions(eq(first))).thenReturn(List.of(planted));

            StuckRunSweeper sweeper = sweeper(sweepService, counting, 1);
            sweeper.sweep();
            sweeper.sweep();

            String second = jobs.stream().map(Job::getName).sorted().skip(1)
                    .findFirst().orElseThrow();
            verify(counting).stuckExecutions(second);
        }
    }

    /**
     * <b>상한에 걸린 것을 지표가 말하나.</b> 시체가 안 줄어드는 원인이 셋인데
     * (꺼짐 · 회수 실패 · <b>처리 용량 부족</b>) 앞의 둘만 계측이 있었다. 그러면
     * {@code BatchStuckExecution} 이 <i>"둘 다 정상인데 뜬다"</i> 를 곧바로
     * <i>"실행이 되살아난다"</i> 로 단정하고, 운영자가 임계를 만지러 간다 — 정작 필요한
     * 것은 상한을 올리는 것이다.
     */
    @Test
    @DisplayName("상한에 걸리면 그것도 지표로 남는다")
    void hittingTheCapIsCounted() {
        double before = counter("cy_batch_stuck_sweep_capped_total");
        LocalDateTime now = LocalDateTime.now();
        try (RunningJobFixture a = RunningJobFixture.plant(jobRepository, jdbcClient,
                CleanupJobConfig.JOB_NAME, now, DEAD, DEAD);
                RunningJobFixture b = RunningJobFixture.plant(jobRepository, jdbcClient,
                        CleanupJobConfig.JOB_NAME, now.plusSeconds(1), DEAD, DEAD)) {

            sweeperWithCap(1).sweep();

            assertThat(counter("cy_batch_stuck_sweep_capped_total"))
                    .as("로그만 남기면 관제가 용량 부족을 회수 실패와 못 가른다")
                    .isGreaterThan(before);
        }
    }

    /**
     * <b>주기가 알림 창보다 길면 기동을 거절한다.</b> {@code BatchStuckExecution} 이
     * 10분에 뜨는데 주기가 그보다 길면 <b>시체가 한 번도 안 걷힌 채</b> 그 알림이 뜬다 —
     * 그런데 그 알림의 진단문은 <i>"지표가 정상이면 실행이 매 주기 되살아나는 것"</i> 이라고
     * 말한다. 설정이 그 말을 거짓으로 만드는 상태를 기동에서 끊는다.
     */
    @Test
    @DisplayName("주기가 알림 창의 절반을 넘으면 기동을 거절한다")
    void rejectsAnIntervalThatOutlivesTheAlertWindow() {
        assertThatThrownBy(() -> new StuckRunSweeper(sweepService, runningJobs, jobs,
                transactionManager, 20, true, 600_000L, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("BatchStuckExecution");
        assertThatThrownBy(() -> new StuckRunSweeper(sweepService, runningJobs, jobs,
                transactionManager, 20, true, 0L, 5))
                .as("0 은 주기가 아니다 — 스프링에 넘기기 전에 우리가 끊는다")
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>0 으로 끄는 길을 안 연다.</b> 스케줄러는 도는데 아무것도 안 하는 상태가 되고,
     * 그것을 알림에 말해 주는 것이 아무것도 없다 — {@code CleanupScheduler} 가 크론
     * {@code "-"} 를 거절하는 것과 같은 근거다.
     */
    @Test
    @DisplayName("상한을 0 으로 주면 기동을 거절한다")
    void rejectsACapThatDisablesTheSweepSilently() {
        assertThatThrownBy(() -> sweeperWithCap(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.stuck-sweep.enabled=false");
    }

    /**
     * <b>끄는 손잡이가 정말 조치만 끄나.</b> 이것이 있어야 운영자가
     * {@code batch.scheduling.enabled} 를 내리지 않는다 — 그 스위치는 1분마다 도는
     * 회차 전이까지 함께 무는 사용자 대면 동작이라 운영 중에 못 쓴다.
     *
     * <p><b>탐지는 살아 있어야 한다.</b> 끈 뒤에도 그 실행이 시체 목록에 남아야
     * {@code BatchStuckExecution} 이 계속 울고, 그때 {@code cy_batch_stuck_sweep_enabled} 가
     * <i>왜 아무도 안 걷는지</i>를 말한다. 조치와 탐지가 함께 꺼지면 그것이 사고다 —
     * {@code batch.stuck-job-after-ms} 를 올리는 방법을 버린 이유가 정확히 그것이다.
     */
    @Test
    @DisplayName("꺼 두면 조치만 멈추고 탐지는 그대로다")
    void disablingStopsTheActionButNotTheDetection() {
        try (RunningJobFixture corpse = RunningJobFixture.plant(jobRepository, jdbcClient,
                ExpireStepContext.JOB_NAME, LocalDateTime.now(), DEAD, DEAD)) {

            sweeper(sweepService, runningJobs, 20, false).sweep();

            assertThat(statusOf(corpse.executionId()))
                    .as("꺼져 있는데 걷으면 손잡이가 아무것도 아니다")
                    .isEqualTo(BatchStatus.STARTED);
            assertThat(runningJobs.stuckExecutions(ExpireStepContext.JOB_NAME))
                    .as("탐지까지 꺼지면 아무도 이 행을 모른다")
                    .anyMatch(run -> run.execution().getId() == corpse.executionId());
        }
    }

    /**
     * <b>게이지가 조건과 같은 규칙으로 읽는가.</b> {@code @Value boolean} 은 스프링의
     * {@code StringToBooleanConverter} 를 타서 {@code true·on·yes·1} 을 전부 참으로 보는데
     * (7.0.8 바이트코드로 확인), {@code @ConditionalOnProperty(havingValue = "true")} 는
     * <b>문자열 {@code "true"} 만</b> 받는다.
     *
     * <p>둘이 갈리면 {@code BATCH_SCHEDULING_ENABLED=1} 인 형상에서 <b>스케줄 작업은
     * 안 뜨는데 게이지만 1</b> 이 되고, {@code BatchStuckSweepDisabled} 까지 함께 침묵한다 —
     * <b>안 도는 것을 돈다고 말하는</b> 방향이라 조용한 실패보다 나쁘다.
     */
    @Nested
    @SpringBootTest(properties = {
            "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
            "spring.batch.job.enabled=false",
            // @ConditionalOnProperty 는 이것을 참으로 안 본다. @Value boolean 은 본다.
            "batch.scheduling.enabled=1",
            "batch.stuck-sweep.enabled=true"
    })
    @Import(MySqlContainerConfig.class)
    @DisplayName("스케줄링 스위치가 1 일 때")
    class WhenTheSwitchIsNotLiterallyTrue {

        @Autowired
        private ApplicationContext context;

        @Autowired
        private MeterRegistry registry;

        @Test
        @DisplayName("작업이 안 뜨면 무장 게이지도 0 이다")
        void theGaugeAgreesWithTheCondition() {
            assertThat(context.getBeansOfType(StuckRunSweeper.class))
                    .as("전제가 무너지면 아래 단언이 아무것도 안 잰다 — "
                            + "@ConditionalOnProperty 는 \"1\" 을 참으로 안 본다")
                    .isEmpty();

            assertThat(registry.find("cy_batch_stuck_sweep_enabled").gauge())
                    .as("게이지가 없으면 알림이 갈래를 못 가른다")
                    .isNotNull();
            assertThat(registry.find("cy_batch_stuck_sweep_enabled").gauge().value())
                    .as("빈이 없는데 1 이면 안 도는 것을 돈다고 말하는 것이다")
                    .isZero();
        }
    }

    /**
     * <b>가드가 있는데 안 도는 상태가 가드가 없는 것보다 나쁘다.</b> 위 테스트들은 전부
     * 손으로 부른 것이라, {@code @ConditionalOnProperty} 를 잘못 적어 빈이 아예 안 생겨도
     * <b>전부 초록</b>이다. 여기서 재는 것은 <b>운영 형상에서 이 작업이 실제로 등록되는가</b>다.
     *
     * <p>⚠️ {@code batch.stuck-job-after-ms} 를 크게 준다. 안 그러면 이 컨텍스트가 뜨는
     * 순간 {@code fixedDelay} 의 첫 발화가 <b>공유 컨테이너의 다른 테스트가 심어 둔 시체를
     * 걷는다</b> — 그쪽이 이유 없이 빨개지고 원인을 찾는 데 오래 걸린다.
     */
    @Nested
    @SpringBootTest(properties = {
            "spring.config.location=classpath:/resolved/application.yml,classpath:/application.yml",
            "spring.batch.job.enabled=false",
            "batch.scheduling.enabled=true",
            "batch.schedule.expire-cron=0 0 0 1 1 *",
            "batch.schedule.cleanup-cron=0 0 0 1 1 *",
            "batch.schedule.verify-cron=0 0 0 1 1 *",
            "batch.metrics.expire-sla-seconds=999999999",
            "batch.metrics.cleanup-sla-seconds=999999999",
            "batch.metrics.verify-sla-seconds=999999999",
            // 남의 시체를 안 건드리게 조치를 끄고 판정도 미룬다. 재는 것은 **등록**뿐이고,
            // enabled 가 @Value 라 꺼도 작업은 그대로 등록된다 — 그 선택이 여기서 값을 한다.
            // ⚠️ 주기를 크게 주는 방법은 이제 못 쓴다. 생성자가 알림 창(10분)의 절반을
            //    넘는 주기를 거절한다 — 그 가드가 이 줄을 실제로 한 번 거절했다.
            "batch.stuck-sweep.enabled=false",
            "batch.stuck-job-after-ms=86400000"
    })
    @Import(MySqlContainerConfig.class)
    @DisplayName("스케줄러를 켰을 때")
    class WhenScheduled {

        @Autowired
        private ApplicationContext context;

        @Test
        @DisplayName("스윕이 스케줄 작업으로 등록된다")
        void theSweepIsActuallyRegistered() {
            assertThat(context.getBeansOfType(StuckRunSweeper.class))
                    .as("@ConditionalOnProperty 가 어긋나면 빈이 안 생기고, 그래도 위 "
                            + "테스트들은 전부 초록이다")
                    .isNotEmpty();

            assertThat(context.getBean(ScheduledAnnotationBeanPostProcessor.class)
                    .getScheduledTasks())
                    .as("빈만 있고 @Scheduled 가 안 붙으면 아무도 부르지 않는다")
                    .anyMatch(task -> String.valueOf(task.getTask().toString())
                            .contains("StuckRunSweeper"));
        }
    }
}
