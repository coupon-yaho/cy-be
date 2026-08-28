// 정리 잡이 무엇을 남기고 무엇을 걷는지 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
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
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.batch.config.ExpireStepContext;
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
        "batch.cleanup.abandoned-after-hours=24",
        // 되읽기 창(7일)보다 커야 한다. 픽스처는 이 값을 넘겨 심는다.
        "batch.cleanup.metadata-keep-days=10",
        // 1 이면 대상 하나마다 트랜잭션이 갈린다 — 드레인이 한 청크에서 끊기는 것을
        // 잡으려면 경계를 실제로 밟아야 한다. asof_state 축이 chunk-size=3 으로 같은 일을 한다.
        "batch.cleanup.metadata-chunk-size=1"
})
@Import({MySqlContainerConfig.class, CleanupJobTest.FixedClockConfig.class})
class CleanupJobTest {

    /**
     * <b>창 판정을 실제 벽시계에 맡기지 않는다.</b> {@code abandoned-after-hours} 는
     * {@code TimeProvider} 로 컷오프를 만드는데, 픽스처는 {@code AS_OF} 기준 상수로 심는다 —
     * 두 축이 갈리면 <b>CI 가 도는 시각에 따라 창 안팎이 뒤집힌다.</b> 지금은 {@code AS_OF} 가
     * 과거라 우연히 통과하는데, 우연으로 통과하는 단언은 언젠가 우연히 깨진다.
     *
     * <p>{@code TimeConfig} 의 {@code Clock} 빈을 {@code AS_OF} 에 고정해 둘을 한 축에 세운다.
     */
    @TestConfiguration(proxyBeanMethods = false)
    static class FixedClockConfig {

        @Bean
        @Primary
        Clock fixedClock() {
            return Clock.fixed(AS_OF.toInstant(ZoneOffset.UTC), ZoneOffset.UTC);
        }
    }

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

        // 배치 메타 쪽에도 걷을 것을 하나 심는다 — 앞 Step 이 물러나도 이건 걷혀야 한다.
        RunningJobFixture oldMeta = RunningJobFixture.plant(
                jobRepository, jdbcClient, ExpireStepContext.JOB_NAME, AS_OF.minusDays(40),
                Duration.ofHours(1), Duration.ofHours(1));
        long oldMetaExecution = oldMeta.executionId();
        finishedAt(oldMetaExecution, AS_OF.minusDays(40));

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
                    .isEqualTo("verifyExecutionIds=[" + verifying.executionId() + "] purgedRows=0"
                            + " metaExecutions=1 metaInstances=1");

            // **뒤 Step 은 같이 안 물러난다.** 앞 Step 의 양보는 도는 검증의 입력을 지키려는
            // 것이고 이 Step 은 그 데이터를 안 건드린다. 같이 멈추면 손 트리거 검증이
            // 13:30 KST 에 걸친 날마다 그날치 BATCH_* 가 통째로 안 걷힌다 — 그런데도 잡은
            // 매일 돌고, 배치 메타 백로그에는 전용 알림이 없어 아무도 모른다.
            assertThat(metaRows("BATCH_JOB_EXECUTION", oldMetaExecution))
                    .as("물러난 것은 앞 Step 의 축이지 배치 메타 축이 아니다")
                    .isZero();
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

            JobExecution execution = runCleanup();
            assertThat(execution.getExitStatus().getExitCode())
                    .as("할 일이 없었던 것을 '물러났다' 로 기록하면 마지막 성공이 안 갱신되어 "
                            + "아무 잘못 없이 SLA 알림이 뜬다")
                    .isEqualTo("COMPLETED");

            // 걷을 것이 0 인 밤에도 **수치는 계산돼서 남는다.** 여기서 설명이 비면
            // "한 행도 못 걷었다" 와 "애초에 대상이 없었다" 가 되짚을 때 같은 값이 된다.
            assertThat(execution.getStepExecutions().stream()
                    .filter(step -> "purgeBatchMetadataStep".equals(step.getStepName()))
                    .findFirst().orElseThrow()
                    .getExitStatus().getExitDescription())
                    .as("종료 설명은 대상이 0 일 때도 형식을 지켜야 한다")
                    .startsWith("metaExecutions=");
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

    /**
     * <b>보존 기간이 지난 배치 메타를 딸린 행까지 걷는다.</b>
     *
     * <p><b>Step 이 있는 실행으로 심는다.</b> {@code plantCompleted} 는
     * {@code BATCH_STEP_EXECUTION} 을 하나도 안 만들어서, 그것으로 재면 Step 축 삭제 두
     * 문장을 통째로 지워도 <b>대상에 자식이 없어 FK 위반이 안 나고 초록</b>이다 —
     * 실제로 그렇게 썼다가 리뷰가 잡았다. 운영에서 {@code verifyJob}(Step 열하나)이 처음
     * 보존 창을 넘기는 날 {@code JOB_EXEC_STEP_FK} 로 청크가 통째로 롤백된다.
     *
     * <p>다섯 테이블에 <b>사전 단언</b>을 건다. 그게 없으면 {@code isZero()} 가 픽스처가
     * 바뀌는 날 공허하게 통과한다.
     */
    @Test
    @DisplayName("보존 기간이 지난 배치 메타를 딸린 행까지 걷는다")
    void purgesExpiredBatchMetadata() throws Exception {
        // **try-with-resources 를 안 쓴다.** 이 행은 잡이 걷어 가는 것이 계약인데,
        // close() 가 이미 없는 행을 닫으려다 EmptyResultDataAccessException 으로 터진다.
        // 정리는 @BeforeEach 의 removeJobExecutions() 가 한다.
        RunningJobFixture old = RunningJobFixture.plant(
                jobRepository, jdbcClient, ExpireStepContext.JOB_NAME, AS_OF.minusDays(40),
                Duration.ofHours(1), Duration.ofHours(1));
        long target = old.executionId();
        long instance = instanceOf(target);
        // 픽스처는 **실제 벽시계**로 심고 잡의 컷오프는 **고정 시계(AS_OF)** 다.
        // 두 축이 다르므로 심은 뒤 잡의 좌표로 옮긴다.
        finishedAt(target, AS_OF.minusDays(40));

        assertThat(metaRows("BATCH_JOB_EXECUTION", target)).isEqualTo(1);
        assertThat(metaRows("BATCH_JOB_EXECUTION_PARAMS", target)).isEqualTo(1);
        assertThat(metaRows("BATCH_JOB_EXECUTION_CONTEXT", target)).isEqualTo(1);
        assertThat(metaRows("BATCH_STEP_EXECUTION", target))
                .as("Step 이 없으면 FK 역순의 Step 축을 아예 안 재게 된다")
                .isEqualTo(1);
        assertThat(stepContextRows(target)).isEqualTo(1);

        JobExecution execution = runCleanup();
        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(metaRows("BATCH_JOB_EXECUTION", target))
                .as("keep-days(10) 를 넘긴 실행은 걷혀야 한다")
                .isZero();
        assertThat(metaRows("BATCH_JOB_EXECUTION_PARAMS", target)).isZero();
        assertThat(metaRows("BATCH_JOB_EXECUTION_CONTEXT", target)).isZero();
        assertThat(metaRows("BATCH_STEP_EXECUTION", target))
                .as("Step 행이 남으면 FK 역순이 깨진 것이다")
                .isZero();
        assertThat(stepContextRows(target)).isZero();
        assertThat(instanceRows(instance))
                .as("실행을 다 지웠으면 인스턴스도 고아가 되어 걷혀야 한다 — "
                        + "안 걷으면 BATCH_JOB_INSTANCE 만 상한 없이 자란다")
                .isZero();

        // **종료 설명을 고정한다.** 이 Step 이 로그 대신 종료 설명을 택한 이유가
        // "컨테이너 로그는 롤오버돼도 배치 메타는 남는다" 인데, 안 재면 다음 리팩터가
        // setExitStatus 를 ExitStatus.COMPLETED 로 단순화해도 전 테스트가 초록이다.
        StepExecution meta = execution.getStepExecutions().stream()
                .filter(step -> "purgeBatchMetadataStep".equals(step.getStepName()))
                .findFirst().orElseThrow();
        assertThat(meta.getExitStatus().getExitDescription())
                .as("되짚을 때 남는 유일한 수치다 — 실행 1 · 고아 인스턴스 1")
                .isEqualTo("metaExecutions=1 metaInstances=1");
        assertThat(meta.getWriteCount())
                .as("WRITE_COUNT 단위는 **잡 실행 수**다. 고아 인스턴스를 더하면 runbook 이 "
                        + "chunk-size 로 나눈 진도를 두 배로 읽는다")
                .isEqualTo(1);
    }

    /**
     * <b>실행이 하나라도 남은 인스턴스는 안 지운다.</b> 지우면 남은 실행의 잡 이름을 잃어
     * 되읽기 조회가 통째로 못 찾는다. {@code CleanupRepository} 가 계약으로 적어 둔 문장이다.
     */
    @Test
    @DisplayName("실행이 남아 있는 인스턴스는 안 지운다")
    void keepsInstancesThatStillHaveExecutions() throws Exception {
        RunningJobFixture old = RunningJobFixture.plant(
                jobRepository, jdbcClient, ExpireStepContext.JOB_NAME, AS_OF.minusDays(40),
                Duration.ofHours(1), Duration.ofHours(1));
        long instance = instanceOf(old.executionId());
        finishedAt(old.executionId(), AS_OF.minusDays(40));

        // 같은 인스턴스에 보존 창 안의 실행을 하나 더 붙인다.
        // 시퀀스 테이블이 관리하는 자리라 프레임워크와 안 겹치게 높은 번호를 쓴다.
        long recent = 900_001L;
        jdbcClient.sql("INSERT INTO BATCH_JOB_EXECUTION(JOB_EXECUTION_ID, VERSION, "
                        + "JOB_INSTANCE_ID, CREATE_TIME, START_TIME, END_TIME, STATUS, "
                        + "EXIT_CODE, EXIT_MESSAGE, LAST_UPDATED) VALUES "
                        + "(:id, 0, :inst, :at, :at, :at, 'COMPLETED', 'COMPLETED', '', :at)")
                .param("id", recent).param("inst", instance)
                .param("at", AS_OF.minusDays(1)).update();

        assertThat(runCleanup().getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(metaRows("BATCH_JOB_EXECUTION", old.executionId()))
                .as("보존 창 밖 실행은 걷힌다").isZero();
        assertThat(metaRows("BATCH_JOB_EXECUTION", recent))
                .as("창 안 실행은 그대로다").isEqualTo(1);
        assertThat(instanceRows(instance))
                .as("실행이 남았으면 인스턴스를 지우면 안 된다 — 지우면 남은 실행의 "
                        + "잡 이름을 잃어 되읽기가 통째로 못 찾는다")
                .isEqualTo(1);
    }

    /**
     * <b>끝나지 않은 실행은 아무리 오래돼도 안 걷는다.</b>
     *
     * <p>{@code END_TIME} 술어 둘은 인덱스용이 아니라 <b>시체 보존</b>이다. 지우면
     * {@code BatchStuckExecution} 이 조용해지는데 그건 고친 게 아니라 <b>증거를 지운
     * 것</b>이고, 그 행은 CY-429 의 복구 API 가 사람의 판단으로 닫는다.
     *
     * <p><b>{@code CREATE_TIME} 만 창 밖으로 민다.</b> 다른 테스트는 세 시각을 같은 값으로
     * 맞춰서 두 술어가 구별되지 않는다 — {@code END_TIME} 조건을 통째로 지워도 초록이었다.
     */
    @Test
    @DisplayName("보존 창 밖이어도 END_TIME 이 비어 있으면 안 걷는다 — 증거를 지우는 일이다")
    void keepsUnfinishedExecutionsHoweverOld() throws Exception {
        RunningJobFixture stuck = RunningJobFixture.plant(
                jobRepository, jdbcClient, ExpireStepContext.JOB_NAME, AS_OF.minusDays(41),
                Duration.ofHours(1), Duration.ofHours(1));
        jdbcClient.sql("UPDATE BATCH_JOB_EXECUTION SET CREATE_TIME = :at, START_TIME = :at, "
                        + "END_TIME = NULL, STATUS = 'STARTED' WHERE JOB_EXECUTION_ID = :id")
                .param("at", AS_OF.minusDays(40)).param("id", stuck.executionId()).update();

        assertThat(runCleanup().getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(metaRows("BATCH_JOB_EXECUTION", stuck.executionId()))
                .as("END_TIME 이 비어 있는 행은 BatchStuckExecution 의 유일한 증거다")
                .isEqualTo(1);
    }

    /**
     * <b>{@code CREATE_TIME} 은 창 밖인데 {@code END_TIME} 은 창 안인 실행.</b> 두 술어를
     * 갈라놓는 유일한 조합이라, 이것이 없으면 {@code END_TIME < :olderThan} 한 줄을 지워도
     * 전 테스트가 초록이다 — 형제 테스트들은 세 시각이 같거나 {@code END_TIME} 이
     * {@code NULL} 이라 {@code IS NOT NULL} 쪽이 먼저 막아 준다.
     *
     * <p><b>지키는 것은 성능이 아니라 {@code BatchMetadataWindow.LOOKBACK_DAYS} 하한 전체다.</b> 이 행이
     * 바로 두 되읽기가 {@code END_TIME > NOW() - 7 DAY} 창에서 찾는 <b>마지막 성공</b>이다.
     * 걷어 버리면 기동 가드로 막아 둔 상태 — 게이지 {@code NaN} — 가 그대로 열린다.
     */
    @Test
    @DisplayName("창 밖에 만들어졌어도 창 안에서 끝났으면 안 걷는다 — 되읽기 창의 근거다")
    void keepsExecutionsThatFinishedInsideTheWindow() throws Exception {
        RunningJobFixture longRun = RunningJobFixture.plant(
                jobRepository, jdbcClient, ExpireStepContext.JOB_NAME, AS_OF.minusDays(41),
                Duration.ofHours(1), Duration.ofHours(1));
        jdbcClient.sql("UPDATE BATCH_JOB_EXECUTION SET CREATE_TIME = :c, START_TIME = :c, "
                        + "END_TIME = :e, STATUS = 'COMPLETED', EXIT_CODE = 'COMPLETED' "
                        + "WHERE JOB_EXECUTION_ID = :id")
                .param("c", AS_OF.minusDays(40))
                .param("e", AS_OF.minusDays(1))
                .param("id", longRun.executionId()).update();

        assertThat(runCleanup().getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(metaRows("BATCH_JOB_EXECUTION", longRun.executionId()))
                .as("되읽기가 END_TIME > NOW()-7DAY 창에서 찾는 행이다 — 지우면 게이지가 NaN")
                .isEqualTo(1);
    }

    /**
     * <b>드레인이 한 청크에서 끊기면 백로그가 안 빠진다.</b> 그런데도 잡은 매일
     * {@code COMPLETED} 이고 마지막 성공 시각도 갱신되니 <b>알림이 하나도 안 운다</b> —
     * 완전히 조용한 고장이라 테스트 말고는 잡을 것이 없다.
     */
    @Test
    @DisplayName("메타도 청크마다 트랜잭션이 갈린다 — 한 청크에서 끊기면 안 된다")
    void drainsBatchMetadataAcrossChunks() throws Exception {
        for (int i = 0; i < 3; i++) {
            RunningJobFixture old = RunningJobFixture.plant(
                    jobRepository, jdbcClient, ExpireStepContext.JOB_NAME,
                    AS_OF.minusDays(40 + i), Duration.ofHours(1), Duration.ofHours(1));
            finishedAt(old.executionId(), AS_OF.minusDays(40 + i));
        }

        JobExecution execution = runCleanup();
        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        StepExecution meta = execution.getStepExecutions().stream()
                .filter(step -> "purgeBatchMetadataStep".equals(step.getStepName()))
                .findFirst().orElseThrow();
        assertThat(meta.getCommitCount())
                .as("chunk-size=1 · 대상 3 이면 삭제 세 번 + 종료 한 번 = 4 다. 여유가 0 인 "
                        + "판정이고 그게 의도다 — 하나라도 덜 돌면(드레인이 안 돌면) 죽는다")
                .isGreaterThan(3);
        assertThat(meta.getWriteCount())
                .as("WRITE_COUNT 단위는 잡 실행 수다 — chunk-size 로 나누면 청크 수가 나온다. "
                        + "0 이면 CleanupRunningTooLong 의 runbook 이 '새 Step 이 아무것도 "
                        + "안 한다' 는 정반대 결론으로 보낸다")
                .isEqualTo(3);
        assertThat(oldMetaCount()).as("백로그가 다 빠져야 한다").isZero();
    }

    /**
     * <b>{@code SimpleJob} 은 마지막 Step 의 종료 상태를 잡의 것으로 덮는다.</b> 순서가
     * 바뀌거나 {@code .split()} 이 되면 {@code YIELDED} 가 {@code COMPLETED} 로 덮여
     * 아무것도 안 한 주기가 마지막 성공 시각을 민다. 그 계약이 산문에만 있었다.
     */
    @Test
    @DisplayName("Step 은 둘이고 순서가 고정이다 — 마지막 Step 의 종료 상태가 잡의 것이 된다")
    void pinsStepOrder() {
        assertThat(((org.springframework.batch.core.job.SimpleJob) cleanupJob).getStepNames())
                .containsExactly("purgeVerificationRunsStep", "purgeBatchMetadataStep");
    }

    /** 보존 창 밖에 남은 배치 메타 실행 수. 드레인이 끝났는지 보는 값이다. */
    private int oldMetaCount() {
        return jdbcClient.sql("SELECT COUNT(*) FROM BATCH_JOB_EXECUTION "
                        + "WHERE END_TIME IS NOT NULL AND END_TIME < :at")
                .param("at", AS_OF.minusDays(10)).query(Integer.class).single();
    }

    /**
     * <b>종료 시각을 잡의 좌표로, 세 시각을 함께 옮긴다.</b> {@code RunningJobFixture} 는
     * 실제 벽시계로 심는데 이 잡의 컷오프는 {@code FixedClockConfig} 가 묶어 둔
     * {@code AS_OF} 기준이다 — 두 축을 안 맞추면 보존 판정이 테스트가 도는 날짜에 따라
     * 뒤집힌다. 삭제 술어가 {@code END_TIME} 인데 조회는
     * {@code CREATE_TIME} 도 함께 본다(인덱스를 타려고) — 운영에서는 실행이 만들어진 뒤에
     * 끝나므로 늘 참이지만, 테스트가 {@code END_TIME} 만 옮기면 그 불변식이 깨져
     * <b>대상이 아닌 것으로 보인다.</b> 실제로 그렇게 썼다가 이 테스트가 잡았다.
     */
    private void finishedAt(long executionId, LocalDateTime endTime) {
        jdbcClient.sql("UPDATE BATCH_JOB_EXECUTION SET CREATE_TIME = :at, START_TIME = :at, "
                        + "END_TIME = :at, STATUS = 'COMPLETED' "
                        + "WHERE JOB_EXECUTION_ID = :id")
                .param("at", endTime).param("id", executionId).update();
        jdbcClient.sql("UPDATE BATCH_STEP_EXECUTION SET END_TIME = :at, STATUS = 'COMPLETED' "
                        + "WHERE JOB_EXECUTION_ID = :id")
                .param("at", endTime).param("id", executionId).update();
    }

    private long instanceOf(long executionId) {
        return jdbcClient.sql("SELECT JOB_INSTANCE_ID FROM BATCH_JOB_EXECUTION "
                        + "WHERE JOB_EXECUTION_ID = :id")
                .param("id", executionId).query(Long.class).single();
    }

    private int instanceRows(long instanceId) {
        return jdbcClient.sql("SELECT COUNT(*) FROM BATCH_JOB_INSTANCE "
                        + "WHERE JOB_INSTANCE_ID = :id")
                .param("id", instanceId).query(Integer.class).single();
    }

    private int stepContextRows(long executionId) {
        return jdbcClient.sql("SELECT COUNT(*) FROM BATCH_STEP_EXECUTION_CONTEXT sec "
                        + "JOIN BATCH_STEP_EXECUTION se "
                        + "  ON se.STEP_EXECUTION_ID = sec.STEP_EXECUTION_ID "
                        + "WHERE se.JOB_EXECUTION_ID = :id")
                .param("id", executionId).query(Integer.class).single();
    }

    private int metaRows(String table, long executionId) {
        return jdbcClient.sql("SELECT COUNT(*) FROM " + table
                        + " WHERE JOB_EXECUTION_ID = :id")
                .param("id", executionId)
                .query(Integer.class)
                .single();
    }

    /**
     * <b>두 Step 의 컷오프가 실행 문맥에 얼어 있다.</b> 청크마다 다시 잡으면 드레인이
     * 길어질수록 기준이 앞으로 밀려, 한 실행 안에서 <i>"보존 기간"</i> 의 뜻이 달라진다.
     * Step 1 쪽이 더 위험하다 — 미는 것이 "지울 배치 메타" 가 아니라 <b>도는 검증의
     * 입력({@code asof_state})</b> 이라, 시작 때 대상이 아니던 검증의 입력이 걷힌다.
     *
     * <p><b>왜 값을 문맥에서 재는가.</b> 이 클래스는 {@code Clock} 을 {@code AS_OF} 에
     * 고정하므로 <b>청크가 몇 번을 돌든 {@code now()} 가 같다</b> — 동작으로는 언 것과
     * 안 언 것이 구분되지 않아, 한때 Step 2 의 그 분기를 지워도 전부 초록이었다(코드 주석이
     * 그 사실을 자백해 뒀다). 똑딱이는 시계를 넣는 길도 있지만 되읽기 빈들이 같은 시계를
     * 읽어 값이 실행 순서에 흔들린다. 그래서 <b>"컷오프가 문맥에서 온다"</b> 는 불변식
     * 자체를 잰다 — 키가 없으면 매번 새로 잡는다는 뜻이고, 이 단언이 그것을 잡는다.
     */
    @Test
    @DisplayName("두 Step 의 컷오프가 첫 청크 값으로 얼어 있다")
    void freezesBothCutoffsOnTheFirstChunk() throws Exception {
        long stale = run(AS_OF.minusDays(9), "PASS", AS_OF.minusDays(9));
        asOfStateRows(stale, 4);

        JobExecution execution = runCleanup();
        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(cutoffOf(execution, "purgeVerificationRunsStep", "cleanup.abandonedCutoff"))
                .as("Step 1 의 버려진-실행 컷오프가 안 얼면, 드레인 중에 대상이 아니던 "
                        + "검증의 asof_state 가 걷힌다")
                .isEqualTo(AS_OF.minusHours(24));

        assertThat(cutoffOf(execution, "purgeBatchMetadataStep", "cleanup.metaCutoff"))
                .as("Step 2 는 이미 얼려 뒀는데 그 분기를 지워도 초록이었다 — 여기서 못 박는다")
                .isEqualTo(AS_OF.minusDays(10));
    }

    /** 지정한 Step 의 실행 문맥에서 컷오프를 읽는다. 없으면 실패시킨다. */
    private static LocalDateTime cutoffOf(JobExecution execution, String stepName, String key) {
        StepExecution step = execution.getStepExecutions().stream()
                .filter(candidate -> stepName.equals(candidate.getStepName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError(stepName + " 이 안 돌았습니다"));
        assertThat(step.getExecutionContext().containsKey(key))
                .as("%s 의 문맥에 %s 가 없습니다 — 컷오프를 청크마다 다시 잡고 있다는 뜻입니다",
                        stepName, key)
                .isTrue();
        return LocalDateTime.parse(step.getExecutionContext().getString(key));
    }

    private JobExecution runCleanup() throws Exception {
        return jobOperator.start(cleanupJob, new JobParametersBuilder()
                .addLocalDateTime("firedAt", nowUtc())
                .toJobParameters());
    }

    /**
     * <b>잡이 보는 "지금" 이다.</b> 위 {@code FixedClockConfig} 가 {@code TimeProvider} 를
     * {@code AS_OF} 에 묶어 뒀으므로, 픽스처도 여기서 파생해야 두 축이 같은 좌표에 선다.
     */
    private static LocalDateTime nowUtc() {
        return AS_OF;
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
