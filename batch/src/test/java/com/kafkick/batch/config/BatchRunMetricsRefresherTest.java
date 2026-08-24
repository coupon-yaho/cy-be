// 마지막 성공 시각과 멈춘 실행 수가 배치 메타에서 제대로 나오는지 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.batch.job.CleanupJobConfig;
import com.kafkick.batch.job.ExpireJobConfig;
import com.kafkick.batch.job.VerifyJobConfig;
import com.kafkick.storage.db.MySqlContainerConfig;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * <b>이 축의 값은 재기동을 견디는 데 있다.</b> JVM 안 카운터로 냈다면 배포 직후 최대 한
 * 주기 동안 <i>"모름"</i> 인데, 만료를 배치 창(일 1회)으로 옮기면 그것이 <b>하루</b>다 —
 * 알림이 데이터가 아니라 배포 시각을 보고 운다. 그래서 배치 메타에서 읽는다.
 *
 * <p><b>존을 함께 못 박는다.</b> {@code END_TIME} 은 프레임워크가 JVM 기본 존으로 찍고
 * 프로메테우스의 {@code time()} 은 UTC 에폭이라, 변환 존이 갈리면 KST 기기에서 아홉 시간
 * 어긋난다. 그 방향이 <b>"아홉 시간 전에 성공했다"</b> 라 알림이 늦게 우는 쪽이다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        // 되읽기를 손으로 부른다. 주기에 기대면 CI 가 느린 날 무작위로 깨진다.
        // 상한(120000)까지 밀어 둔다 — 그 위는 생성자가 거절한다.
        "batch.metrics.run-refresh-ms=120000"
})
@Import(MySqlContainerConfig.class)
class BatchRunMetricsRefresherTest {

    private static final LocalDateTime KEY = LocalDateTime.of(2026, 5, 1, 9, 0);

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private BatchRunMetricsRefresher refresher;

    @Autowired
    private MeterRegistry registry;

    @BeforeEach
    void setUp() {
        new JobRepositoryTestUtils(jobRepository).removeJobExecutions();
    }

    @Test
    @DisplayName("성공한 적이 없으면 0 이 아니라 NaN 이다")
    void neverSucceededIsNotZero() {
        refresher.refresh();

        assertThat(lastSuccess(ExpireJobConfig.JOB_NAME))
                .as("0 은 1970년이라 '아주 오래 안 돌았다' 가 된다 — 갓 뜬 서버가 곧바로 운다")
                .isNaN();
    }

    @Test
    @DisplayName("마지막 COMPLETED 의 종료 시각을 JVM 존 그대로 에폭으로 낸다")
    void reportsLastCompletedEndTimeInTheZoneItWasWritten() {
        try (RunningJobFixture done = RunningJobFixture.plantCompleted(
                jobRepository, ExpireJobConfig.JOB_NAME, KEY, Duration.ofMinutes(3))) {

            refresher.refresh();

            assertThat(lastSuccess(ExpireJobConfig.JOB_NAME))
                    .as("UTC 로 바꾸면 KST 기기에서 아홉 시간 어긋나고, 그 방향이 "
                            + "'아홉 시간 전에 성공했다' 라 알림이 늦게 운다")
                    .isEqualTo(done.endTime().atZone(ZoneId.systemDefault()).toEpochSecond());
        }
    }

    /**
     * <b>판정이 FAIL 이어도 잡은 성공이다.</b> 이 축이 묻는 것은 <i>"돌긴 했나"</i> 이고,
     * 데이터가 틀렸다는 사실은 {@code cy_verification_verdict} 가 따로 진다. 둘을 섞으면
     * 오염셋을 한 번 돌리는 것만으로 <b>"검증이 안 돈다"</b> 알림이 뜬다.
     */
    @Test
    @DisplayName("검증은 판정 결과와 무관하게 COMPLETED 면 성공으로 센다")
    void verifySuccessIgnoresTheVerdict() {
        try (RunningJobFixture done = RunningJobFixture.plantCompleted(
                jobRepository, VerifyJobConfig.JOB_NAME, KEY, Duration.ofMinutes(2))) {

            refresher.refresh();

            assertThat(lastSuccess(VerifyJobConfig.JOB_NAME))
                    .as("실행 id=" + done.executionId())
                    .isEqualTo(done.endTime().atZone(ZoneId.systemDefault()).toEpochSecond());
        }
    }

    /**
     * <b>7일 창은 새 판정 경계다 — 문장으로만 두면 안 된다.</b>
     *
     * <p>그 조건은 부등호 방향·{@code NOW()} 의 세션 존·{@code DATE_SUB} 단위를 한 번에
     * 틀릴 수 있는 자리이고, {@code docs/13} §6 의 C 가 <b>손댈 것이 확정된 줄</b>이다.
     * 예컨대 {@code INTERVAL 7 HOUR} 로 오타를 내면 일 1회 만료에서 창이 늘 비어
     * 게이지가 영구 {@code NaN} 이고 알림이 매일 뜨는데, 이 테스트가 없으면 초록이다.
     */
    @Test
    @DisplayName("창 밖의 성공은 없는 것으로 본다")
    void successOlderThanTheWindowIsUnknown() {
        try (RunningJobFixture old = RunningJobFixture.plantCompleted(
                jobRepository, ExpireJobConfig.JOB_NAME, KEY, Duration.ofDays(8))) {

            refresher.refresh();

            assertThat(lastSuccess(ExpireJobConfig.JOB_NAME))
                    .as("실행 id=" + old.executionId() + " 은 7일 창 밖이다. 창 밖은 어차피 "
                            + "SLA 위반이라 NaN 과 같은 판정이고, 창을 둔 이유는 조회가 "
                            + "배치 메타 이력 전체에 비례해 자라는 것을 막기 위해서다")
                    .isNaN();
        }
    }

    @Test
    @DisplayName("창 안의 성공은 그대로 낸다")
    void successInsideTheWindowIsReported() {
        try (RunningJobFixture recent = RunningJobFixture.plantCompleted(
                jobRepository, ExpireJobConfig.JOB_NAME, KEY, Duration.ofDays(6))) {

            refresher.refresh();

            assertThat(lastSuccess(ExpireJobConfig.JOB_NAME))
                    .as("경계 안쪽(6일)은 값이 나와야 한다 — 창을 너무 좁히면 정상 상태가 "
                            + "NaN 이 되어 알림이 매일 뜬다")
                    .isEqualTo(recent.endTime().atZone(ZoneId.systemDefault()).toEpochSecond());
        }
    }

    /**
     * <b>물러난 실행은 성공이 아니다.</b> 정리 잡은 검증이 도는 중이면 <b>검증 파생 행을</b>
     * 한 행도 안 걷고 {@code COMPLETED} 로 닫힌다(배치 메타는 그래도 걷는다 — CY-436).
     * 실패가 아니므로 상태를 바꿀 수 없지만, 그것을 성공으로 세면 <i>"매일 돌았고 매일
     * asof_state 를 하나도 안 지웠다"</i> 가 모든 알림을 초록으로 통과한다.
     */
    @Test
    @DisplayName("YIELDED 로 닫힌 실행은 마지막 성공으로 안 센다")
    void yieldedExecutionIsNotSuccess() {
        try (RunningJobFixture yielded = RunningJobFixture.plantCompleted(
                jobRepository, CleanupJobConfig.JOB_NAME, KEY, Duration.ofHours(1))) {
            setExitCode(yielded.executionId(), CleanupJobConfig.YIELDED_EXIT_CODE);

            refresher.refresh();

            assertThat(lastSuccess(CleanupJobConfig.JOB_NAME))
                    .as("실행 id=" + yielded.executionId() + " 는 돌긴 했지만 검증 파생 행을 한 행도 안 걷었다")
                    .isNaN();
        }
    }

    /**
     * <b>{@code EXIT_CODE} 는 nullable 이다.</b> {@code <> 'YIELDED'} 만 쓰면 NULL 비교가
     * UNKNOWN 이라 그 행이 떨어지는데, 그것은 성공을 못 본 것이라 <b>없는 사고로 알림이
     * 뜬다.</b> 같은 NULL 함정을 이 티켓의 {@code deleteFindings} 에서 한 번 밟았다.
     */
    @Test
    @DisplayName("종료 코드가 비어 있어도 COMPLETED 면 성공으로 센다")
    void nullExitCodeStillCountsAsSuccess() {
        try (RunningJobFixture completed = RunningJobFixture.plantCompleted(
                jobRepository, CleanupJobConfig.JOB_NAME, KEY, Duration.ofHours(1))) {
            setExitCode(completed.executionId(), null);

            refresher.refresh();

            assertThat(lastSuccess(CleanupJobConfig.JOB_NAME))
                    .as("배치 메타가 비어 있는 것을 사고로 바꿔 읽으면 안 된다")
                    .isEqualTo(completed.endTime().atZone(ZoneId.systemDefault()).toEpochSecond());
        }
    }

    /** 배치 메타를 직접 손댄다 — JobRepository 로는 NULL 종료 코드를 만들 수 없다. */
    private void setExitCode(long executionId, String exitCode) {
        jdbcClient.sql("UPDATE BATCH_JOB_EXECUTION SET EXIT_CODE = :code "
                        + "WHERE JOB_EXECUTION_ID = :id")
                .param("code", exitCode)
                .param("id", executionId)
                .update();
    }

    /**
     * 가드가 시체를 무시할 때 남기는 것은 WARN 로그뿐이고, 그 로그는 <b>누가 가드를 부를 때만</b>
     * 나온다 — 검증을 하루 한 번 돌리면 그 사이 며칠은 신호가 없다. 그동안 만료↔검증
     * 상호 배제가 그만큼 꺼져 있다.
     */
    @Test
    @DisplayName("진도가 멈춘 실행을 센다")
    void countsStuckExecutions() {
        try (RunningJobFixture alive = RunningJobFixture.plant(
                     jobRepository, jdbcClient, ExpireJobConfig.JOB_NAME, KEY.minusDays(1));
             RunningJobFixture stuck = RunningJobFixture.plant(
                     jobRepository, jdbcClient, ExpireJobConfig.JOB_NAME, KEY.minusDays(2),
                     Duration.ofHours(2), Duration.ofHours(2))) {

            refresher.refresh();

            assertThat(stuckCount(ExpireJobConfig.JOB_NAME))
                    .as("살아 있는 id=" + alive.executionId() + " 은 빼고 "
                            + "멈춘 id=" + stuck.executionId() + " 만 센다")
                    .isEqualTo(1.0);
        }
    }

    /**
     * <b>못 읽었으면 직전 값을 들고 있으면 안 된다.</b> 마지막 성공 시각에서 그 오해는
     * <i>"방금 돌았다"</i> 가 되어 SLA 알림을 통째로 재운다 — 감시가 꺼진 줄도 모르게 된다.
     */
    @Test
    @DisplayName("되읽기가 실패하면 게이지를 NaN 으로 떨어뜨린다")
    void failedRefreshFallsBackToUnknown() {
        try (RunningJobFixture done = RunningJobFixture.plantCompleted(
                jobRepository, ExpireJobConfig.JOB_NAME, KEY, Duration.ofMinutes(1))) {

            refresher.refresh();
            assertThat(lastSuccess(ExpireJobConfig.JOB_NAME))
                    .as("실행 id=" + done.executionId() + " 을 읽어 값이 있어야 한다")
                    .isNotNaN();
        }

        // 배치 메타를 통째로 못 읽는 상태를 만든다. 되읽기가 던지면 markUnknown 으로 떨어진다.
        jdbcClient.sql("RENAME TABLE BATCH_JOB_INSTANCE TO BATCH_JOB_INSTANCE_HIDDEN").update();
        try {
            refresher.refresh();
            assertThat(lastSuccess(ExpireJobConfig.JOB_NAME))
                    .as("직전 값을 들고 있으면 관제가 그것을 지금 상태로 읽는다")
                    .isNaN();
            assertThat(stuckCount(ExpireJobConfig.JOB_NAME))
                    .as("이쪽이 0 에 얼어붙으면 BatchStuckExecution 이 조용히 꺼진다 — "
                            + "NaN > 0 은 거짓이라 BatchRunMetricsUnknown 이 그 자리를 진다")
                    .isNaN();
        } finally {
            jdbcClient.sql("RENAME TABLE BATCH_JOB_INSTANCE_HIDDEN TO BATCH_JOB_INSTANCE").update();
        }
    }

    /**
     * <b>이 게이지가 없으면 검증 SLA 를 못 건다.</b> 잡 이름 그레인({@code lastSuccess})은
     * {@code verifyJob} 하나로 {@code CLEAN} 과 {@code CORRUPT} 를 뭉치는데, 게이트가 보는
     * 것은 앞의 것뿐이다 — 그래서 {@code (dataset, scope)} 축을 따로 낸다(CY-470).
     */
    @Test
    @DisplayName("게이트 조합의 검증 성공을 (dataset, scope) 그레인으로 낸다")
    void reportsVerifySuccessAtTheGateGrain() {
        try (RunningJobFixture done = RunningJobFixture.plantCompleted(
                jobRepository, VerifyRunContext.JOB_NAME, gateParameters(), Duration.ofMinutes(2))) {

            refresher.refresh();

            assertThat(verifyLastSuccess())
                    .as("실행 id=" + done.executionId())
                    .isEqualTo(done.endTime().atZone(ZoneId.systemDefault()).toEpochSecond());
        }
    }

    /**
     * <b>이 티켓의 요지다.</b> 발표 리허설로 오염셋을 한 번 돌리면 잡 이름 그레인 게이지는
     * 앞으로 밀린다 — 그 축에 SLA 를 걸면 <b>게이트 조합이 며칠째 안 돌았는데 조용하다.</b>
     * 새 게이지는 그 실행을 안 봐야 한다.
     */
    @Test
    @DisplayName("CORRUPT 실행은 게이트 그레인의 SLA 를 리셋하지 않는다")
    void corruptRunDoesNotResetTheGateGrain() {
        JobParameters corrupt = new JobParametersBuilder()
                .addLocalDateTime("asOf", KEY)
                .addString("dataset", "CORRUPT")
                .addString("scope", VerifyRunContext.SLA_SCOPE)
                .toJobParameters();

        try (RunningJobFixture done = RunningJobFixture.plantCompleted(
                jobRepository, VerifyRunContext.JOB_NAME, corrupt, Duration.ofMinutes(2))) {

            refresher.refresh();

            assertThat(lastSuccess(VerifyRunContext.JOB_NAME))
                    .as("잡 이름 그레인은 이 실행을 본다 — 그래서 그 축에 SLA 를 걸 수 없다. "
                            + "실행 id=" + done.executionId())
                    .isNotNaN();
            assertThat(verifyLastSuccess())
                    .as("게이트 조합으로 성공한 적이 없으므로 NaN 이어야 한다")
                    .isNaN();
        }
    }

    /**
     * 되읽기가 죽으면 <b>두 축이 함께</b> 떨어져야 한다. 한쪽만 낡은 값을 들면 관제가
     * 둘을 나란히 볼 때 <i>"하나는 모름 하나는 정상"</i> 이라는 존재하지 않는 상태를 만든다.
     */
    @Test
    @DisplayName("되읽기가 실패하면 검증 게이지도 함께 NaN 이 된다")
    void failedRefreshAlsoDropsTheVerifyGauge() {
        try (RunningJobFixture done = RunningJobFixture.plantCompleted(
                jobRepository, VerifyRunContext.JOB_NAME, gateParameters(), Duration.ofMinutes(1))) {

            refresher.refresh();
            assertThat(verifyLastSuccess())
                    .as("실행 id=" + done.executionId() + " 을 읽어 값이 있어야 한다")
                    .isNotNaN();
        }

        jdbcClient.sql("RENAME TABLE BATCH_JOB_INSTANCE TO BATCH_JOB_INSTANCE_HIDDEN").update();
        try {
            refresher.refresh();
            assertThat(verifyLastSuccess())
                    .as("직전 값을 들고 있으면 관제가 그것을 지금 상태로 읽는다")
                    .isNaN();
        } finally {
            jdbcClient.sql("RENAME TABLE BATCH_JOB_INSTANCE_HIDDEN TO BATCH_JOB_INSTANCE").update();
        }
    }

    private static JobParameters gateParameters() {
        return new JobParametersBuilder()
                .addLocalDateTime("asOf", KEY)
                .addString("dataset", VerifyRunContext.SLA_DATASET)
                .addString("scope", VerifyRunContext.SLA_SCOPE)
                .toJobParameters();
    }

    private double verifyLastSuccess() {
        return registry.get("cy_verify_last_success_seconds")
                .tag("dataset", VerifyRunContext.SLA_DATASET)
                .tag("scope", VerifyRunContext.SLA_SCOPE)
                .gauge().value();
    }

    private double lastSuccess(String jobName) {
        return registry.get("cy_batch_last_success_seconds").tag("spring_batch_job_name", jobName).gauge().value();
    }

    private double stuckCount(String jobName) {
        return registry.get("cy_batch_stuck_executions").tag("spring_batch_job_name", jobName).gauge().value();
    }
}
