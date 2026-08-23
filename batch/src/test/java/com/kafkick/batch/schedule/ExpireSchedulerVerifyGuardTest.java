// 검증이 도는 동안에는 만료 스케줄러가 그 슬롯을 건너뛰는지 확인합니다.
package com.kafkick.batch.schedule;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.batch.config.ExpireMetrics;
import com.kafkick.batch.config.RunningJobFixture;
import com.kafkick.batch.config.RunningJobProbe;
import com.kafkick.batch.job.VerifyJobConfig;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>배제는 양방향이어야 한다.</b> 검증만 만료를 피하면, 접수는 됐는데 그 뒤 크론이 발화해
 * {@code assertFrozenStep} 이 판정을 버리는 창이 남는다. 검증 소요(수 분)가 크론 주기(5분)에
 * 가까워 <b>그 창은 예외가 아니라 대부분</b>이고, 버려진 실행은 {@code attempt} 를 태울 뿐
 * 아니라 만료가 찍은 {@code updated_at} 때문에 <b>그 {@code asOf} 를 영구히 못 쓰게</b> 만든다.
 *
 * <p><b>대상이 유실되지는 않는다.</b> 스케줄러는 주기마다 {@code asOf} 를 새로 잡고 만료는
 * {@code expires_at < asOf} 를 {@code id > 0} 부터 훑으므로, 건너뛴 슬롯의 몫을 다음에 도는
 * 슬롯이 통째로 가져간다.
 *
 * <p><b>그래도 상한이 있다</b>({@code max-expire-skips}). 매 슬롯마다 다시 묻기 때문에,
 * 검증이 주기보다 오래 돌면 그 사이 슬롯이 <b>전부</b> 죽는다 — 한때 여기에
 * <i>"최대 지연이 정확히 크론 주기 하나"</i> 라고 적었는데 거짓이었다. 상한을 두면 최대 지연이
 * <b>{@code (상한 + 1) × 크론 주기}</b> 로 <b>구조적으로</b> 정해진다.
 *
 * <p>진짜 크론에 기대지 않고 {@code expire()} 를 직접 부른다. 5분을 기다리는 테스트는
 * CI 에서 못 쓰고, 이 클래스가 재려는 것은 발화 시각이 아니라 <b>가드의 유무</b>다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=true",
        "batch.schedule.expire-cron=0 0 0 1 1 *",
        // step-timeout(600000)보다 커야 한다 — RunningJobProbe 생성자가 검사한다.
        "batch.stuck-job-after-ms=1800000"
})
@Import(MySqlContainerConfig.class)
class ExpireSchedulerVerifyGuardTest {

    private static final LocalDateTime KEY = LocalDateTime.of(2026, 2, 1, 9, 0);
    private static final int MAX_SKIPS = 2;

    @Autowired
    @Qualifier("expireJob")
    private Job expireJob;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private RunningJobProbe runningJobs;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private ExpireMetrics metrics;

    @Test
    @DisplayName("검증이 도는 중이면 만료 슬롯을 건너뛴다")
    void skipsTheSlotWhileVerifyIsRunning() {
        List<JobParameters> started = new ArrayList<>();

        try (RunningJobFixture verify = RunningJobFixture.plant(
                jobRepository, jdbcClient, VerifyJobConfig.JOB_NAME, KEY)) {

            scheduler(started).expire();

            assertThat(started)
                    .as("검증 실행 id=" + verify.executionId() + " 가 도는 동안 만료가 뜨면 "
                            + "판정 근거가 판정 도중에 바뀐다")
                    .isEmpty();
        }
    }

    @Test
    @DisplayName("검증이 안 돌면 만료가 평소대로 뜬다")
    void startsNormallyWhenNothingIsVerifying() {
        new JobRepositoryTestUtils(jobRepository).removeJobExecutions();
        List<JobParameters> started = new ArrayList<>();

        scheduler(started).expire();

        assertThat(started)
                .as("가드가 너무 넓으면 만료가 조용히 안 도는 상태가 된다 — "
                        + "재고를 쓰는 유일한 잡에서 가장 나쁜 결말이다")
                .hasSize(1);
    }

    /**
     * <b>건너뛰기에 상한이 없으면 만료가 굶는다.</b> 한때 <i>"최대 지연이 정확히 크론 주기
     * 하나"</i> 라고 적었는데 거짓이었다 — 매 슬롯마다 다시 묻기 때문에, 검증이 주기보다 오래
     * 돌면 그 사이 슬롯이 <b>전부</b> 죽는다. 상한을 두면 최대 지연이
     * {@code (상한 + 1) × 크론 주기} 로 <b>구조적으로</b> 정해진다 — 검증 소요 실측이 필요 없다.
     *
     * <p>상한을 넘으면 만료를 돌린다. 재고는 운영의 진실이고 검증 실행은 진단이다.
     */
    @Test
    @DisplayName("연속 상한을 넘으면 검증이 돌고 있어도 만료를 돌린다")
    void runsAnywayOnceTheSkipLimitIsReached() {
        List<JobParameters> started = new ArrayList<>();

        try (RunningJobFixture verify = RunningJobFixture.plant(
                jobRepository, jdbcClient, VerifyJobConfig.JOB_NAME, KEY.plusHours(1))) {

            ExpireScheduler scheduler = scheduler(started);
            for (int slot = 0; slot < MAX_SKIPS; slot++) {
                scheduler.expire();
            }
            assertThat(started)
                    .as("상한 안에서는 건너뛴다. 검증 실행 id=" + verify.executionId())
                    .isEmpty();

            scheduler.expire();

            assertThat(started)
                    .as("상한을 넘으면 재고 쪽을 택한다 — 안 그러면 만료가 무한히 굶는다")
                    .hasSize(1);
        }
    }

    /**
     * 잡을 진짜로 돌리지 않는다. {@code expireJob} 을 띄우면 이 클래스가 만료의 소요와
     * 데이터에 기대게 되는데, 여기서 재려는 것은 <b>뜨느냐 안 뜨느냐</b> 하나다.
     */
    private ExpireScheduler scheduler(List<JobParameters> started) {
        Clock fixed = Clock.fixed(
                LocalDateTime.of(2026, 2, 1, 9, 5)
                        .atZone(ZoneId.systemDefault()).toInstant(),
                ZoneId.systemDefault());

        JobOperator recording = (JobOperator) Proxy.newProxyInstance(
                JobOperator.class.getClassLoader(),
                new Class<?>[] {JobOperator.class},
                (proxy, method, args) -> {
                    if ("start".equals(method.getName()) && args.length == 2) {
                        started.add((JobParameters) args[1]);
                        JobExecution execution = new JobExecution(
                                1L, new JobInstance(1L, "expireJob"), new JobParameters());
                        execution.setStatus(BatchStatus.COMPLETED);
                        return execution;
                    }
                    return null;
                });

        return new ExpireScheduler(recording, expireJob, new TimeProvider(fixed),
                "0 */5 * * * *", runningJobs, metrics, MAX_SKIPS,
                // 상한 2 · 5분 크론이면 최악 지연 900초라 SLA 를 넉넉히 올려 준다 —
                // 이 클래스가 재는 것은 SLA 가드가 아니라 슬롯 건너뛰기다.
                2_000L, 60_000L);
    }
}
