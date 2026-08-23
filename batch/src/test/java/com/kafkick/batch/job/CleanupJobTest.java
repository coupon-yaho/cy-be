// 정리 잡이 무엇을 남기고 무엇을 걷는지 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.batch.config.RunningJobFixture;
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>지우는 것보다 남기는 것이 어렵다.</b> 이 잡이 잘못 도는 방향은 둘인데 무게가 다르다 —
 * 안 지우면 디스크가 차고, <b>잘못 지우면 판정 근거가 사라진다.</b> 그래서 보존 쪽을
 * 먼저 잰다.
 *
 * <p><b>지우는 축 셋을 다 잰다.</b> 한때 {@code asof_state} 행 수만 세서,
 * {@code deleteFindings} 와 {@code stats.clear} 두 줄을 지워도 전부 초록이었다 —
 * 그 두 줄이 {@code v_latest_stats_run} 을 깨는 줄이었는데 테스트가 그쪽을 안 봤다.
 *
 * <p>스케줄러는 끈다. 이 클래스가 재는 것은 발화가 아니라 잡의 동작이고, 켜 두면 만료가
 * 같은 데이터를 건드린다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "batch.verify.asof-state-keep-runs=2",
        "batch.cleanup.chunk-size=3",
        "batch.cleanup.abandoned-after-hours=24"
})
@Import(MySqlContainerConfig.class)
class CleanupJobTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 4, 1, 9, 0);

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    @Qualifier("cleanupJob")
    private Job cleanupJob;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JdbcClient jdbcClient;

    /** 통계 두 테이블이 {@code coupons} 를 FK 로 물어서 실물 회차가 하나 필요하다. */
    private long couponId;

    @BeforeEach
    void setUp() {
        new JobRepositoryTestUtils(jobRepository).removeJobExecutions();
        VerificationSeed seed = new VerificationSeed(jdbcClient);
        seed.clear();
        couponId = seed.newCoupon();
        // grade_stats.grade 도 FK 다. 마이그레이션이 등급 행을 안 심으므로 여기서 만든다.
        jdbcClient.sql("INSERT IGNORE INTO grades (code, bit_value) VALUES ('VIP', 8)").update();
    }

    @Test
    @DisplayName("보존 창 안의 실행은 파생 행을 그대로 둔다")
    void keepsDerivedRowsInsideTheWindow() throws Exception {
        long older = run(AS_OF, "PASS", AS_OF);
        long newer = run(AS_OF.plusDays(1), "PASS", AS_OF.plusDays(1));
        asOfStateRows(older, 4);
        asOfStateRows(newer, 4);
        derivedRows(older);

        assertThat(runCleanup().getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(asOfStateCount(newer))
                .as("보존 창(2)이 최신 둘을 덮으므로 아무것도 안 지운다")
                .isEqualTo(4);
        assertThat(asOfStateCount(older)).isEqualTo(4);
        assertThat(findingCount(older)).isEqualTo(2);
        assertThat(statsCount(older)).isEqualTo(3);
    }

    /**
     * <b>{@code verification_runs} 행 자체는 남긴다.</b> 그것이 판정 이력이고
     * {@code cy_batch_last_success_seconds} 와 관제 히스토리가 그 위에 선다 — 파생 행만 걷는다.
     *
     * <p>지우는 축 <b>셋을 다</b> 단언한다. {@code asof_state} 만 세면
     * {@code deleteFindings}·{@code stats.clear} 두 줄이 무보증으로 남는다.
     */
    @Test
    @DisplayName("보존 창 밖은 파생 행 셋을 다 걷고 실행 행은 남긴다")
    void purgesDerivedRowsButKeepsTheRunItself() throws Exception {
        long oldest = run(AS_OF, "PASS", AS_OF);
        long middle = run(AS_OF.plusDays(1), "PASS", AS_OF.plusDays(1));
        long newest = run(AS_OF.plusDays(2), "PASS", AS_OF.plusDays(2));
        asOfStateRows(oldest, 7);
        asOfStateRows(middle, 4);
        asOfStateRows(newest, 4);
        derivedRows(oldest);
        derivedRows(middle);

        assertThat(runCleanup().getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(asOfStateCount(oldest))
                .as("청크 3 으로 7행을 지운다 — 0 이 나올 때까지 반복해야 한다")
                .isZero();
        assertThat(findingCount(oldest))
                .as("PASS 의 검출 행은 설명할 판정이 없다")
                .isZero();
        assertThat(statsCount(oldest))
                .as("통계 셋(coupon·grade·hourly)이 함께 걷혀야 한다")
                .isZero();
        assertThat(asOfStateCount(middle)).isEqualTo(4);
        assertThat(findingCount(middle)).isEqualTo(2);
        assertThat(statsCount(middle)).isEqualTo(3);
        assertThat(runExists(oldest))
                .as("판정 이력을 지우면 관제와 마지막 성공 시각이 함께 사라진다")
                .isTrue();
    }

    /**
     * <b>스냅샷을 지우는 것과 그 사실을 표시하는 것은 같은 사실이다.</b> 세 테이블만 비우고
     * {@code stats_status} 를 {@code COMPLETE} 로 두면 {@code verification_runs} 는
     * <i>"완결된 스냅샷"</i> 이라고 계속 말하는데 그 행이 하나도 없다 — {@code V8} 이
     * <i>"완결되지 않은 스냅샷은 물리적으로 조회되지 않는다"</i> 로 세운 계약이 그 자리에서
     * 깨진다. 뷰 제외 절은 <b>맨 앞의 한 행</b>만 지키므로 이 불변식을 대신하지 못한다.
     */
    @Test
    @DisplayName("통계를 걷으면 stats_status 도 함께 내려간다")
    void dropsTheStatsPointerWhenTheSnapshotIsPurged() throws Exception {
        // 뷰의 답은 as_of 가 가장 앞선 pointed 이고, purged 는 그 뒤에 남는 COMPLETE 다.
        long purged = run(AS_OF, "PASS", AS_OF, "COMPLETE");
        long pointed = run(AS_OF.plusDays(9), "PASS", AS_OF.plusDays(9), "COMPLETE");
        derivedRows(purged);
        asOfStateRows(purged, 3);
        run(AS_OF.plusDays(1), "PASS", AS_OF.plusDays(1));
        run(AS_OF.plusDays(2), "PASS", AS_OF.plusDays(2));

        assertThat(latestStatsRun())
                .as("전제 — 뷰가 보는 것은 purged 가 아니라 pointed 다")
                .hasValue(pointed);

        assertThat(runCleanup().getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(statsCount(purged)).isZero();
        assertThat(statsStatus(purged))
                .as("행이 0개인데 COMPLETE 로 남으면, 조건이 하나 바뀌는 날 뷰가 그 실행을 "
                        + "'완결된 최신 스냅샷' 으로 집는다")
                .isNull();
        assertThat(latestStatsRun()).hasValue(pointed);
    }

    /**
     * <b>보존 창은 {@code id} 순인데 뷰는 {@code as_of} 순이다.</b> 두 축이 갈리면 뷰가
     * 가리키는 실행이 창 밖으로 밀린다 — {@code V9} 헤더가 그 경우를 이미 적어 뒀다.
     * 그때 통계를 걷으면 대시보드가 <b>"데이터 없음" 과 "0건" 을 구분할 수 없다.</b>
     */
    @Test
    @DisplayName("v_latest_stats_run 이 가리키는 실행은 보존 창 밖이어도 통계를 남긴다")
    void keepsTheRunTheStatsViewPointsAt() throws Exception {
        // as_of 는 가장 앞서지만 id 는 가장 작다 — 뒤에 심는 둘이 보존 창을 채운다.
        long pointed = run(AS_OF.plusDays(9), "PASS", AS_OF, "COMPLETE");
        derivedRows(pointed);
        asOfStateRows(pointed, 3);
        run(AS_OF, "PASS", AS_OF);
        run(AS_OF.plusDays(1), "PASS", AS_OF.plusDays(1));

        assertThat(latestStatsRun())
                .as("전제가 성립해야 이 테스트가 뜻이 있다")
                .hasValue(pointed);

        assertThat(runCleanup().getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(statsCount(pointed))
                .as("뷰가 가리키는 실행의 통계를 걷으면 대시보드가 조용히 빈다")
                .isEqualTo(3);
        assertThat(latestStatsRun()).hasValue(pointed);
        assertThat(asOfStateCount(pointed))
                .as("함께 남는다 — 이 실행은 대상 집합에서 통째로 빠진다")
                .isEqualTo(3);
    }

    /**
     * <b>오염셋의 합격이 이 과제의 산출물이다.</b> {@code judgeAgainstManifest} 는 검출
     * 집합이 정답 매니페스트와 <b>정확히 일치할 때</b> {@code PASS} 를 낸다 — 목표 결과가
     * {@code dataset=CORRUPT · verdict=PASS · finding_count=800} 이고, 그 800행의
     * {@code (finding_type, target_key)} 가 <i>"누락 0 · 오탐 0"</i> 을 보여 준다.
     *
     * <p>한때 <i>"{@code FAIL} 만 남긴다"</i> 로 썼는데 <b>정확히 그 산출물을 지우는
     * 조건</b>이었다. 리뷰가 잡았다.
     */
    @Test
    @DisplayName("오염셋 합격(CORRUPT · PASS)의 검출 행은 보존 창 밖이어도 남는다")
    void keepsFindingsOfCorruptPass() throws Exception {
        long corruptPass = run(AS_OF, "PASS", AS_OF, null, "BATCH", "CORRUPT");
        derivedRows(corruptPass);
        asOfStateRows(corruptPass, 4);
        run(AS_OF.plusDays(1), "PASS", AS_OF.plusDays(1));
        run(AS_OF.plusDays(2), "PASS", AS_OF.plusDays(2));

        assertThat(runCleanup().getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(findingCount(corruptPass))
                .as("이 800행이 '누락 0 · 오탐 0' 의 유일한 증거다 — 건수 컬럼은 근거가 아니다")
                .isEqualTo(2);
        assertThat(asOfStateCount(corruptPass))
                .as("무거운 쪽은 그대로 걷는다 — 남기는 근거는 크기가 아니라 설명력이다")
                .isZero();
    }

    /**
     * <b>{@code FAIL} 은 남는데 무엇이 틀렸는지가 사라지면 안 된다.</b>
     * {@code VerificationVerdictFailed} 의 runbook 이 그 행을 직접 가리키고,
     * 이 과제의 합격 조건은 건수가 아니라 <b>집합</b>이다.
     */
    @Test
    @DisplayName("FAIL 판정의 검출 행은 보존 창 밖이어도 남는다")
    void keepsFindingsOfFailedVerdicts() throws Exception {
        long failed = run(AS_OF, "FAIL", AS_OF);
        derivedRows(failed);
        asOfStateRows(failed, 4);
        run(AS_OF.plusDays(1), "PASS", AS_OF.plusDays(1));
        run(AS_OF.plusDays(2), "PASS", AS_OF.plusDays(2));

        assertThat(runCleanup().getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(findingCount(failed))
                .as("finding_count 와 checksum 만으로는 어느 target_key 가 틀렸는지 못 가른다")
                .isEqualTo(2);
        assertThat(asOfStateCount(failed))
                .as("무거운 쪽은 그대로 걷는다 — 남기는 근거는 크기가 아니라 설명력이다")
                .isZero();
        assertThat(statsCount(failed)).isZero();
    }

    /**
     * <b>시드가 심은 행은 이 배치가 만든 것이 아니다.</b> CORRUPT 는 정답 800행을 그
     * {@code run_id} 에 붙이고 게이트가 쓰는 {@code as_of} 도 그 행에서 나온다 —
     * {@code SELECT_LATEST_CLOSED} 가 같은 이유로 같은 조건을 이미 걸고 있다.
     */
    @Test
    @DisplayName("시드가 심은 실행(origin=SEED)은 손대지 않는다")
    void leavesSeedPlantedRuns() throws Exception {
        long seeded = run(AS_OF, "FAIL", AS_OF, null, "SEED");
        derivedRows(seeded);
        asOfStateRows(seeded, 3);
        run(AS_OF.plusDays(1), "PASS", AS_OF.plusDays(1));
        run(AS_OF.plusDays(2), "PASS", AS_OF.plusDays(2));
        run(AS_OF.plusDays(3), "PASS", AS_OF.plusDays(3));

        assertThat(runCleanup().getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(asOfStateCount(seeded))
                .as("시드 행은 게이트의 기준값이지 이 배치의 산출물이 아니다")
                .isEqualTo(3);
        assertThat(findingCount(seeded)).isEqualTo(2);
        assertThat(statsCount(seeded)).isEqualTo(3);
    }

    /**
     * <b>{@code SKIPPED} 는 지우면 안 되는 값이다.</b> CORRUPT 는 <i>"오염셋이라 집계를 안
     * 했다"</i>, CLEAN 인데 {@code verdict != PASS} 면 <i>"불합격이라 안 했다"</i> 를 뜻하고
     * 뒤쪽은 <b>경보</b>다. 무조건 NULL 로 덮으면 그 둘과 <i>"통계 Step 이 죽었다"</i> 가
     * 한 값으로 접혀, {@code finalizeRunStep} 이 컬럼을 쓴 이유가 사라진다.
     */
    @Test
    @DisplayName("SKIPPED 스냅샷 상태는 정리가 지나가도 그대로 남는다")
    void keepsSkippedStatsStatus() throws Exception {
        long skipped = run(AS_OF, "PASS", AS_OF, "SKIPPED", "BATCH", "CORRUPT");
        derivedRows(skipped);
        asOfStateRows(skipped, 3);
        run(AS_OF.plusDays(1), "PASS", AS_OF.plusDays(1));
        run(AS_OF.plusDays(2), "PASS", AS_OF.plusDays(2));

        assertThat(runCleanup().getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(statsCount(skipped))
                .as("스냅샷 행은 걷는다 — 남기는 것은 그 사실을 적은 값뿐이다")
                .isZero();
        assertThat(statsStatus(skipped))
                .as("NULL 로 덮으면 '오염셋이라 안 했다' 와 '통계 Step 이 죽었다' 가 같아진다")
                .isEqualTo("SKIPPED");
    }

    /**
     * <b>대상이 여럿일 때 커서가 전부를 가로지르는가.</b> 케이스마다 대상이 하나뿐이면
     * {@code min} 과 {@code max} 가 같아 <b>이 축이 한 번도 안 재진다</b> —
     * {@code max} 로 바뀌면 매일 밤 최신 하나만 걷히고 오래된 300만 행은 영원히 남는데,
     * 잡은 {@code COMPLETED} 라 어떤 알림도 안 운다.
     */
    @Test
    @DisplayName("한 실행에서 대상 여럿을 오래된 것부터 전부 걷는다")
    void purgesEveryTargetOldestFirst() throws Exception {
        long a = run(AS_OF, "PASS", AS_OF);
        long b = run(AS_OF.plusDays(1), "PASS", AS_OF.plusDays(1));
        long c = run(AS_OF.plusDays(2), "PASS", AS_OF.plusDays(2));
        run(AS_OF.plusDays(3), "PASS", AS_OF.plusDays(3));
        run(AS_OF.plusDays(4), "PASS", AS_OF.plusDays(4));   // 보존 창(2)이 이 둘을 덮는다
        for (long id : new long[] {a, b, c}) {
            asOfStateRows(id, 4);
            derivedRows(id);
        }

        assertThat(runCleanup().getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(asOfStateCount(a) + asOfStateCount(b) + asOfStateCount(c))
                .as("min 이 max 로 바뀌면 최신 하나만 걷히고 둘이 남는다")
                .isZero();
        assertThat(statsCount(a) + statsCount(b) + statsCount(c)).isZero();
    }

    /**
     * <b>CY-384 가 연 누수다.</b> {@code assertFrozenStep} 이 데이터 변동을 잡으면
     * {@code finalizeRunStep} 이 안 돌아 실행이 열린 채 남는데, 그 실행이 이미 쓴
     * {@code asof_state} 는 아무도 안 걷었다.
     */
    @Test
    @DisplayName("판정을 못 낸 채 버려진 실행은 보존 창 안이어도 걷는다")
    void purgesAbandonedRunsEvenInsideTheWindow() throws Exception {
        long abandoned = run(AS_OF, null, AS_OF.minusDays(3));
        long healthy = run(AS_OF.plusDays(1), "PASS", AS_OF.plusDays(1));
        asOfStateRows(abandoned, 5);
        asOfStateRows(healthy, 5);
        derivedRows(abandoned);

        assertThat(runCleanup().getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(asOfStateCount(abandoned))
                .as("보존 창(2)이 둘 다 덮지만 판정이 비어 있으면 파생 행은 쓸모가 없다")
                .isZero();
        assertThat(findingCount(abandoned))
                .as("설명할 판정이 없는 검출 행이다. 지우는 조건을 verdict 비교만으로 쓰면 "
                        + "NULL 비교가 UNKNOWN 이라 이 행이 영원히 남는다")
                .isZero();
        assertThat(statsCount(abandoned)).isZero();
        assertThat(asOfStateCount(healthy)).isEqualTo(5);
    }

    /**
     * <b>도는 중인 검증을 걷으면 안 된다.</b> 300만 전수는 소요를 아직 안 쟀으므로
     * ({@code docs/13} §6 의 D) 하루 창을 두고, 그 안에 시작한 열린 실행은 손대지 않는다.
     */
    @Test
    @DisplayName("방금 시작해 아직 판정이 없는 실행은 안 걷는다")
    void leavesRunsThatMayStillBeRunning() throws Exception {
        long running = run(AS_OF, null, nowUtc().minusMinutes(5));
        asOfStateRows(running, 3);
        // 보존 창 밖으로 밀어낸다 — 그래도 "도는 중" 이라 살아야 한다.
        run(AS_OF.plusDays(1), "PASS", AS_OF.plusDays(1));
        run(AS_OF.plusDays(2), "PASS", AS_OF.plusDays(2));

        assertThat(runCleanup().getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(asOfStateCount(running))
                .as("걷으면 그 검증이 자기 입력을 잃는다 — 판정이 데이터 변동으로 죽는다")
                .isEqualTo(3);
    }

    /**
     * <b>시각 창은 두 번째 방어선이다.</b> {@code abandoned-after-hours} 의 근거는
     * <i>"소요를 아직 안 쟀다"</i> 뿐이라, 실제 소요가 그 값을 넘으면 <b>도는 검증의 입력</b>
     * 이 걷힌다. 그때 규칙은 빈 상태를 읽고 예외 없이 <b>조용히 틀린 답</b>을 낸다.
     * 그래서 배치 메타에 같은 질문을 먼저 한다.
     */
    @Test
    @DisplayName("검증이 도는 중이면 시각 창 밖 실행이어도 안 걷는다")
    void stopsWhileVerifyIsRunning() throws Exception {
        long stale = run(AS_OF, null, AS_OF.minusDays(3));
        asOfStateRows(stale, 4);
        run(AS_OF.plusDays(1), "PASS", AS_OF.plusDays(1));
        run(AS_OF.plusDays(2), "PASS", AS_OF.plusDays(2));

        try (RunningJobFixture verifying = RunningJobFixture.plant(
                jobRepository, jdbcClient, VerifyJobConfig.JOB_NAME,
                LocalDateTime.of(2026, 4, 9, 5, 0), Duration.ofMinutes(30), Duration.ZERO)) {
            assertThat(verifying.executionId()).isPositive();

            assertThat(runCleanup().getStatus())
                    .as("멈추는 것이지 실패가 아니다 — 실패로 두면 게이지가 안 움직여 "
                            + "CleanupNotSucceeding 이 정리가 아니라 검증 때문에 운다")
                    .isEqualTo(BatchStatus.COMPLETED);

            assertThat(asOfStateCount(stale))
                    .as("하루가 지난 열린 실행이지만 실제로 도는 중이다 — 시각 창은 그것을 모른다")
                    .isEqualTo(4);
        }
    }

    /**
     * <b>물러난 것을 성공으로 기록하면 안 된다.</b> 그냥 {@code COMPLETED} 로 닫으면
     * {@code cy_batch_last_success_seconds} 가 갱신되어, 한 행도 안 지웠는데
     * {@code CleanupNotSucceeding} 이 영원히 조용하다 — {@code chunk-size=0} 을 기동 때
     * 거절한 근거와 관측상 똑같은 상태다. 로그는 감시 수단이 아니다.
     */
    @Test
    @DisplayName("검증 때문에 물러나면 종료 코드가 YIELDED 다 — 성공 집계에서 빠진다")
    void marksYieldedExitCode() throws Exception {
        // **걷을 것이 실제로 남아 있어야 한다.** 파생 행이 없으면 그 실행은 애초에 대상이
        // 아니라(CleanupJdbcAdapter 의 "남은 행" 술어) COMPLETED 로 닫힌다 — 그것이 맞다.
        long stale = run(AS_OF, null, AS_OF.minusDays(3));
        asOfStateRows(stale, 4);
        run(AS_OF.plusDays(1), "PASS", AS_OF.plusDays(1));
        run(AS_OF.plusDays(2), "PASS", AS_OF.plusDays(2));

        try (RunningJobFixture verifying = RunningJobFixture.plant(
                jobRepository, jdbcClient, VerifyJobConfig.JOB_NAME,
                LocalDateTime.of(2026, 4, 11, 5, 0), Duration.ofMinutes(30), Duration.ZERO)) {
            assertThat(verifying.executionId()).isPositive();

            JobExecution execution = runCleanup();

            assertThat(execution.getStatus())
                    .as("실패가 아니다 — 실패로 두면 BatchJobFailed 가 검증 때문에 운다")
                    .isEqualTo(BatchStatus.COMPLETED);
            assertThat(execution.getExitStatus().getExitCode())
                    .as("BatchRunMetricsRefresher 가 이 코드를 마지막 성공에서 뺀다")
                    .isEqualTo(CleanupJobConfig.YIELDED_EXIT_CODE);
            // 검증이 **시작 전부터** 돌고 있으므로 첫 청크에서 물러난다 — 여기서는 0 이 사실이다.
            //
            // ⚠️ 0 이 아닌 경로(중간에 검증이 떠서 커밋된 진도를 남기고 멈추는 것)는 이
            //    하네스로 결정적으로 못 만든다. 그 값이 계산된 것이라는 사실은 실행 id 가
            //    함께 실리는 것으로 대신 재고, 의미는 상수 javadoc 이 진다.
            assertThat(execution.getExitStatus().getExitDescription())
                    .as("코드 하나로는 '한 행도 못 걷었다' 와 '200만 걷고 멈췄다' 가 같은 값이다 — "
                            + "그 구분을 배치 메타가 져야 되짚을 때 남는다")
                    .isEqualTo("verifyExecutionIds=[" + verifying.executionId() + "] purgedRows=0");
        }
    }

    /**
     * <b>할 일이 없던 밤은 물러난 것이 아니다.</b> 검증 실행 검사가 대상 조회보다 앞에 있으면,
     * 걷을 것이 하나도 없는 밤에도 검증이 떠 있다는 이유만으로 {@code YIELDED} 가 되고
     * 그 실행이 마지막 성공에서 빠진다 — 이틀 연속이면 <b>정상 상태에서
     * {@code CleanupNotSucceeding} 이 운다.</b> 04:30 UTC 는 13:30 KST 라 손 트리거 검증과
     * 겹치기 쉬워, 백로그가 빠진 뒤의 <b>평상시</b>가 그 상태다.
     */
    @Test
    @DisplayName("걷을 것이 없으면 검증이 도는 중이어도 COMPLETED 다")
    void completesWhenThereIsNothingToPurge() throws Exception {
        // 보존 창(2) 안쪽으로만 심는다 — 대상이 0 이다.
        run(AS_OF, "PASS", AS_OF);
        run(AS_OF.plusDays(1), "PASS", AS_OF.plusDays(1));

        try (RunningJobFixture verifying = RunningJobFixture.plant(
                jobRepository, jdbcClient, VerifyJobConfig.JOB_NAME,
                LocalDateTime.of(2026, 4, 13, 5, 0), Duration.ofMinutes(30), Duration.ZERO)) {
            assertThat(verifying.executionId()).isPositive();

            assertThat(runCleanup().getExitStatus().getExitCode())
                    .as("할 일이 없었던 것을 '물러났다' 로 기록하면 마지막 성공이 안 갱신되어 "
                            + "아무 잘못 없이 SLA 알림이 뜬다")
                    .isEqualTo("COMPLETED");
        }
    }

    /**
     * <b>{@code origin='SEED'} 이면서 판정이 비어 있는 행</b>은 {@code abandonedRunIds} 의
     * {@code origin} 절만이 지킨다. 그 절을 지워도 초록이면 다음 사람이 "중복" 으로 뺀다 —
     * 그러면 시드가 심은 기준 행의 파생 행이 24시간 뒤 걷히고, 게이트가 쓰는 정답 800행이
     * 사라진다. 재시딩 말고 복구가 없다.
     */
    @Test
    @DisplayName("시드가 심은 열린 실행(origin=SEED · 판정 없음)도 손대지 않는다")
    void leavesSeedPlantedOpenRuns() throws Exception {
        long seededOpen = run(AS_OF, null, AS_OF.minusDays(3), null, "SEED");
        asOfStateRows(seededOpen, 3);
        derivedRows(seededOpen);
        run(AS_OF.plusDays(1), "PASS", AS_OF.plusDays(1));
        run(AS_OF.plusDays(2), "PASS", AS_OF.plusDays(2));

        assertThat(runCleanup().getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(asOfStateCount(seededOpen))
                .as("버려진 실행 경로는 보존 창을 우회하므로, origin 절이 유일한 방어다")
                .isEqualTo(3);
        assertThat(findingCount(seededOpen)).isEqualTo(2);
    }

    /**
     * <b>백로그가 빠진 뒤의 평상시</b>다 — 보존 창 밖에 실행이 여럿 있지만 파생 행은 이미
     * 다 걷혔다. 이때 검증이 도는 중이라고 {@code YIELDED} 로 닫으면 그 실행이 마지막
     * 성공에서 빠져, <b>아무 잘못 없이</b> 이틀 뒤 {@code CleanupNotSucceeding} 이 운다.
     *
     * <p>대상 조회를 앞으로 옮기는 것만으로는 이 축이 안 지켜졌다 — 대상 집합이
     * <i>"걷을 것이 남았나"</i> 를 안 보면 지난 이력 전체가 늘 대상이라 그 갈래에 도달하지
     * 못했다. 그 술어를 지우면 이 테스트가 빨개진다.
     */
    @Test
    @DisplayName("보존 창 밖이어도 이미 다 걷혔으면 검증 중에도 COMPLETED 다")
    void completesWhenEveryTargetIsAlreadyEmpty() throws Exception {
        run(AS_OF, "PASS", AS_OF);
        run(AS_OF.plusDays(1), "PASS", AS_OF.plusDays(1));
        run(AS_OF.plusDays(2), "PASS", AS_OF.plusDays(2));
        run(AS_OF.plusDays(3), "PASS", AS_OF.plusDays(3));

        try (RunningJobFixture verifying = RunningJobFixture.plant(
                jobRepository, jdbcClient, VerifyJobConfig.JOB_NAME,
                LocalDateTime.of(2026, 4, 15, 5, 0), Duration.ofMinutes(30), Duration.ZERO)) {
            assertThat(verifying.executionId()).isPositive();

            assertThat(runCleanup().getExitStatus().getExitCode())
                    .as("보존 창 밖 실행이 둘 있지만 걷을 파생 행이 하나도 없다 — "
                            + "물러난 것이 아니라 할 일이 없었던 것이다")
                    .isEqualTo("COMPLETED");
        }
    }

    @Test
    @DisplayName("평소 실행의 종료 코드는 COMPLETED 다 — YIELDED 필터가 정상까지 지우면 안 된다")
    void marksCompletedExitCodeNormally() throws Exception {
        run(AS_OF, "PASS", AS_OF);
        run(AS_OF.plusDays(1), "PASS", AS_OF.plusDays(1));
        run(AS_OF.plusDays(2), "PASS", AS_OF.plusDays(2));

        assertThat(runCleanup().getExitStatus().getExitCode()).isEqualTo("COMPLETED");
    }

    /**
     * <b>청크가 커밋을 나누는가.</b> {@code LIMIT} 을 붙여 지워도 태스클릿이 한 번만 돌면
     * 커밋이 마지막에 한 번뿐이라 언두 로그와 잠금은 전량을 통째로 들고 있고, 데드라인에
     * 걸리면 <b>여태 지운 것이 전부 롤백</b>된다. 그러면 진도가 0 이라 다음 날도 같은 양을
     * 처음부터 시도해 또 실패한다 — 영원히 성공 못 하는 잡이 된다.
     *
     * <p>커밋 수로 잰다. 시간에 안 기대므로 CI 가 느린 날에도 안 흔들린다.
     */
    @Test
    @DisplayName("청크마다 트랜잭션이 갈린다 — 커밋이 한 번이 아니다")
    void commitsEachChunkSeparately() throws Exception {
        long target = run(AS_OF, "PASS", AS_OF);
        asOfStateRows(target, 9);   // 청크 3 → 삭제 세 번 + 0 을 받는 한 번
        run(AS_OF.plusDays(1), "PASS", AS_OF.plusDays(1));
        run(AS_OF.plusDays(2), "PASS", AS_OF.plusDays(2));

        JobExecution execution = runCleanup();
        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        StepExecution step = execution.getStepExecutions().iterator().next();
        assertThat(step.getCommitCount())
                .as("9행 ÷ 청크 3 이면 삭제만 세 번이다. 커밋이 1 이면 한 트랜잭션이 전량을 "
                        + "들고 있다는 뜻이고, 그때 LIMIT 은 아무것도 안 나눈 것이다")
                .isGreaterThan(3);
        assertThat(asOfStateCount(target)).isZero();
    }

    private JobExecution runCleanup() throws Exception {
        return jobOperator.start(cleanupJob, new JobParametersBuilder()
                .addLocalDateTime("firedAt", nowUtc())
                .toJobParameters());
    }

    /**
     * <b>잡과 같은 시계를 쓴다.</b> 잡은 {@code TimeProvider}({@code Clock.systemUTC})로
     * 창을 자르는데, 이 테스트 JVM 은 {@code user.timezone=Asia/Seoul} 로 고정돼 있어
     * ({@code batch/build.gradle}) 인자 없는 {@code now()} 는 아홉 시간 어긋난다.
     * 지금은 어긋나는 방향이 <b>안전한 쪽</b>이라 통과하지만, 그것은 우연이다 —
     * 우연으로 통과하는 단언은 반대 방향 존에서 조용히 뒤집힌다.
     */
    private static LocalDateTime nowUtc() {
        return LocalDateTime.now(ZoneOffset.UTC);
    }

    private long run(LocalDateTime asOf, String verdict, LocalDateTime startedAt) {
        return run(asOf, verdict, startedAt, null, "BATCH");
    }

    private long run(LocalDateTime asOf, String verdict, LocalDateTime startedAt,
            String statsStatus) {
        return run(asOf, verdict, startedAt, statsStatus, "BATCH");
    }

    private long run(LocalDateTime asOf, String verdict, LocalDateTime startedAt,
            String statsStatus, String origin) {
        return run(asOf, verdict, startedAt, statsStatus, origin, "CLEAN");
    }

    private long run(LocalDateTime asOf, String verdict, LocalDateTime startedAt,
            String statsStatus, String origin, String dataset) {
        jdbcClient.sql("""
                        INSERT INTO verification_runs
                               (as_of, scope, dataset, attempt, verdict, finding_count,
                                started_at, stats_status, origin)
                        VALUES (:asOf, 'FULL', :dataset, :attempt, :verdict, 0,
                                :startedAt, :statsStatus, :origin)
                        """)
                .param("asOf", asOf)
                .param("dataset", dataset)
                .param("attempt", (int) (asOf.toLocalDate().toEpochDay() % 1000))
                .param("verdict", verdict)
                .param("startedAt", startedAt)
                .param("statsStatus", statsStatus)
                .param("origin", origin)
                .update();
        return jdbcClient.sql("SELECT LAST_INSERT_ID()").query(Long.class).single();
    }

    private void asOfStateRows(long runId, int count) {
        for (int i = 1; i <= count; i++) {
            jdbcClient.sql("""
                            INSERT INTO asof_state (run_id, coupon_id, state, active_usage_count)
                            VALUES (:runId, :couponId, 'ISSUED', 0)
                            """)
                    .param("runId", runId).param("couponId", (long) i).update();
        }
    }

    /** 검출 2행 + 통계 세 테이블에 한 행씩. 지우는 축 셋을 한 번에 심는다. */
    private void derivedRows(long runId) {
        for (int i = 1; i <= 2; i++) {
            jdbcClient.sql("""
                            INSERT INTO verification_findings
                                   (run_id, finding_type, target_key, expected, actual)
                            VALUES (:runId, 'V1_STOCK_MISMATCH', :key, 'x', 'y')
                            """)
                    .param("runId", runId).param("key", "COUPON:" + runId + ":" + i).update();
        }
        jdbcClient.sql("""
                        INSERT INTO coupon_stats (run_id, coupon_id) VALUES (:runId, :couponId)
                        """)
                .param("runId", runId).param("couponId", couponId).update();
        jdbcClient.sql("""
                        INSERT INTO grade_stats (run_id, coupon_id, grade)
                        VALUES (:runId, :couponId, 'VIP')
                        """)
                .param("runId", runId).param("couponId", couponId).update();
        jdbcClient.sql("""
                        INSERT INTO hourly_stats (run_id, day_of_week, hour)
                        VALUES (:runId, 'MON', 9)
                        """)
                .param("runId", runId).update();
    }

    private int asOfStateCount(long runId) {
        return count("SELECT COUNT(*) FROM asof_state WHERE run_id = :runId", runId);
    }

    private int findingCount(long runId) {
        return count("SELECT COUNT(*) FROM verification_findings WHERE run_id = :runId", runId);
    }

    /** 세 테이블의 합. 하나만 세면 나머지 둘이 무보증으로 남는다. */
    private int statsCount(long runId) {
        return count("SELECT COUNT(*) FROM coupon_stats WHERE run_id = :runId", runId)
                + count("SELECT COUNT(*) FROM grade_stats WHERE run_id = :runId", runId)
                + count("SELECT COUNT(*) FROM hourly_stats WHERE run_id = :runId", runId);
    }

    private String statsStatus(long runId) {
        return jdbcClient.sql("SELECT stats_status FROM verification_runs WHERE id = :runId")
                .param("runId", runId).query(String.class).optional().orElse(null);
    }

    private Optional<Long> latestStatsRun() {
        return jdbcClient.sql("SELECT id FROM v_latest_stats_run").query(Long.class).optional();
    }

    private int count(String sql, long runId) {
        return jdbcClient.sql(sql).param("runId", runId).query(Integer.class).single();
    }

    private boolean runExists(long runId) {
        return count("SELECT COUNT(*) FROM verification_runs WHERE id = :runId", runId) == 1;
    }
}
