package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.InvalidJobParametersException;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.coupon.IssuanceEventType;
import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.core.verification.VerificationFinding;
import com.kafkick.core.verification.VerificationRuleRepository;
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;
import com.kafkick.storage.verification.VerificationRuleJdbcAdapter;

/**
 * Step 0 의 종단 확인이다. 잡을 실제로 돌려 {@code asof_state} 가 생기는지 본다.
 *
 * <p>이 테스트는 트랜잭션 밖에서 돌아 롤백이 없다. 검증 테이블은 시드가 지우고,
 * 배치 메타는 {@link JobRepositoryTestUtils} 가 지운다. 메타를 안 지우면 두 번째 테스트가
 * 같은 파라미터로 돌 때 Spring Batch 가 완료된 인스턴스라며 거부한다.
 *
 * <p>창 크기를 2 로 낮춰 창이 여러 번 넘어가는 경로를 실제로 태운다.
 * 기본값 50000 이면 어떤 테스트도 창을 한 번밖에 안 읽는다.
 *
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "batch.verify.chunk-size=2",
        "batch.verify.replay-window-size=2"
})
@Import({MySqlContainerConfig.class, VerifyJobConfigTest.MutateAfterStockStepConfig.class})
class VerifyJobConfigTest {

    /** 실행 시작 시각(= 실제 지금)보다 앞서야 한다. asOf 는 미래를 가리킬 수 없다. */
    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

    /**
     * <b>V1 이 읽은 직후에 재고를 건드린다.</b> 시작 가드는 통과시키고 끝 가드만 발동시키는
     * 유일한 방법이다 — 그러지 않으면 {@code assertFrozenStep} 의 재고 가지를 통째로 지워도
     * 테스트가 전부 초록이다.
     *
     * <p><b>리포지터리를 대역으로 갈아끼우지 않는다.</b> 인터페이스에 메서드가 늘 때
     * {@code default} 로 추가하면 대역이 조용히 위임을 안 해서, 이 클래스만 다른 세계를 보게 된다.
     * 정작 잡 전체 배선을 확인하는 유일한 테스트가 여기다. Step 리스너면 프로덕션 빈이 그대로 남는다.
     */
    static Long mutateStockAfterRead;

    /**
     * 같은 이유로 정책 축도 규칙이 읽은 뒤에 건드린다.
     *
     * <p><b>회차 식별자를 담는다.</b> {@code WHERE} 없이 테이블을 통째로 바꾸면, 나중에 회차를
     * 둘 이상 세우는 테스트가 이 클래스에 추가될 때 무관한 회차까지 건드려
     * 원인을 알 수 없는 실패를 만든다. {@code null} 이면 건드리지 않는다.
     */
    static Long mutatePolicyAfterRead;

    /**
     * 같은 이유로 이력 축도 규칙이 읽은 뒤에 건드린다. 발급건 식별자를 담고,
     * 얼린 상한보다 <b>큰 id</b> 로 {@code asOf} 이하 이력을 하나 넣는다 —
     * 리플레이는 못 읽었는데 지문은 읽는 상태를 만든다.
     */
    static Long insertHistoryAfterFreeze;

    @TestConfiguration
    static class MutateAfterStockStepConfig {

        /**
         * <b>인터페이스가 아니라 어댑터를 상속한다.</b> 인터페이스를 손으로 구현하면
         * 메서드가 늘 때 {@code default} 로 추가된 것을 조용히 안 위임해서,
         * 이 클래스만 다른 세계를 보게 된다 — 정작 잡 전체 배선을 확인하는 유일한 테스트가 여기다.
         * 상속이면 새 메서드가 자동으로 진짜 구현을 탄다.
         */
        @Bean
        @Primary
        VerificationRuleJdbcAdapter mutatingRules(JdbcClient jdbc) {
            return new VerificationRuleJdbcAdapter(jdbc) {

                @Override
                public List<VerificationFinding> findStockMismatches(
                        long runId, LocalDateTime asOf, int limit) {
                    List<VerificationFinding> found = super.findStockMismatches(runId, asOf, limit);

                    if (mutateStockAfterRead != null) {
                        jdbc.sql("UPDATE coupon_stocks SET updated_at = :at WHERE coupon_id = :couponId")
                                .param("at", asOf.plusSeconds(1))
                                .param("couponId", mutateStockAfterRead)
                                .update();
                    }
                    if (insertHistoryAfterFreeze != null) {
                        jdbc.sql("""
                                        INSERT INTO issuance_histories
                                            (issuance_id, event_type, from_status, to_status, created_at)
                                        VALUES (:id, 'USE', 'ISSUED', 'USED', :at)
                                        """)
                                .param("id", insertHistoryAfterFreeze)
                                .param("at", AS_OF.minusHours(1))
                                .update();
                    }
                    if (mutatePolicyAfterRead != null) {
                        jdbc.sql("UPDATE coupons SET eligible_grades_mask = 7 WHERE id = :couponId")
                                .param("couponId", mutatePolicyAfterRead)
                                .update();
                    }
                    return found;
                }
            };
        }
    }

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private Job verifyJob;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JdbcClient jdbcClient;

    private VerificationSeed seed;

    @BeforeEach
    void setUp() {
        mutateStockAfterRead = null;
        mutatePolicyAfterRead = null;
        insertHistoryAfterFreeze = null;
        new JobRepositoryTestUtils(jobRepository).removeJobExecutions();
        seed = new VerificationSeed(jdbcClient);
        seed.clear();
    }

    @Test
    @DisplayName("잡을 돌리면 이력이 접혀 asof_state 가 생긴다")
    void produceAsOfStateFromHistories() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.USED);
        issued(issuanceId, AS_OF.minusHours(3));
        used(issuanceId, AS_OF.minusHours(2));

        JobExecution execution = launch(1);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(statesOf(issuanceId)).containsExactly("USED");
    }

    @Test
    @DisplayName("마지막 이력보다 앞선 asOf 는 거부한다 — asOf 는 실행 순간을 고정하는 값이지 과거 조회가 아니다")
    void rejectAsOfBeforeLatestHistory() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.USED);
        issued(issuanceId, AS_OF.minusHours(1));
        used(issuanceId, AS_OF.plusHours(1));

        JobExecution execution = launch(1);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(failureMessagesOf(execution)).anyMatch(
                message -> message.contains("마지막 이력 시각 이상이어야 합니다"));
        assertThat(asOfStateCount()).isZero();
    }

    @Test
    @DisplayName("asOf 이후에 갱신된 발급건이 있으면 거부한다 — 조용히 빼면 0건이 두 뜻을 갖는다")
    void rejectIssuancesUpdatedAfterAsOf() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED);
        issued(issuanceId, AS_OF.minusHours(1));
        jdbcClient.sql("UPDATE issuances SET updated_at = :at WHERE id = :id")
                .param("at", AS_OF.plusSeconds(1))
                .param("id", issuanceId)
                .update();

        JobExecution execution = launch(1);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(failureMessagesOf(execution)).anyMatch(
                m -> m.contains("asOf 이후에 갱신된 발급건이 있습니다"));
        assertThat(asOfStateCount()).isZero();
    }

    @Test
    @DisplayName("이력이 전부 asOf 뒤면 거부한다 — 접을 것이 없다고 초록불이 뜨면 안 된다")
    void rejectWhenEveryHistoryIsAfterAsOf() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED);
        issued(issuanceId, AS_OF.plusHours(1));

        JobExecution execution = launch(1);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(failureMessagesOf(execution)).anyMatch(
                m -> m.contains("마지막 이력 시각 이상이어야 합니다"));
    }

    @Test
    @DisplayName("증분 검증은 거부한다 — 받아 놓고 전수로 돌면 기록만 INCREMENTAL 로 남는다")
    void rejectIncrementalScope() throws Exception {
        JobParameters parameters = new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .addLocalDateTime("fromTs", AS_OF.minusMinutes(10))
                .addString("scope", "INCREMENTAL")
                .addString("dataset", "CLEAN")
                .addLong("attempt", 1L)
                .toJobParameters();

        JobExecution execution = jobOperator.start(verifyJob, parameters);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(failureMessagesOf(execution)).anyMatch(
                message -> message.contains("증분 검증은 아직 지원하지 않습니다"));
        assertThat(runCount()).isZero();
    }

    @Test
    @DisplayName("필수 파라미터가 빠지면 잡이 시작조차 하지 않는다 — 무엇이 빠졌는지 보여야 한다")
    void rejectMissingRequiredParameter() {
        JobParameters parameters = new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .addString("dataset", "CLEAN")
                .addLong("attempt", 1L)
                .toJobParameters();

        assertThatThrownBy(() -> jobOperator.start(verifyJob, parameters))
                .isInstanceOf(InvalidJobParametersException.class);
    }

    @Test
    @DisplayName("창을 여러 번 넘겨도 모든 발급건이 남는다")
    void coverEveryIssuanceAcrossWindows() throws Exception {
        List<Long> issuanceIds = List.of(
                seed.issuance(IssuanceStatus.ISSUED), seed.issuance(IssuanceStatus.ISSUED),
                seed.issuance(IssuanceStatus.ISSUED), seed.issuance(IssuanceStatus.ISSUED),
                seed.issuance(IssuanceStatus.ISSUED));
        issuanceIds.forEach(id -> issued(id, AS_OF.minusHours(1)));

        launch(1);

        assertThat(asOfStateCount()).isEqualTo(issuanceIds.size());
    }

    @Test
    @DisplayName("활성 사용 건수가 채워진다 — 리플레이가 끝난 뒤 한 문장으로 센다")
    void fillActiveUsageCount() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.USED);
        issued(issuanceId, AS_OF.minusHours(3));
        used(issuanceId, AS_OF.minusHours(2));
        seed.usage(issuanceId, AS_OF.minusHours(2), null);

        launch(1);

        assertThat(usageCountsOf(issuanceId)).containsExactly(1);
    }

    @Test
    @DisplayName("asOf 이전에 취소된 사용은 세지 않는다")
    void ignoreUsageCanceledBeforeAsOf() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED);
        issued(issuanceId, AS_OF.minusHours(3));
        seed.usage(issuanceId, AS_OF.minusHours(2), AS_OF.minusHours(1));

        launch(1);

        assertThat(usageCountsOf(issuanceId)).containsExactly(0);
    }

    @Test
    @DisplayName("불법 전이가 있어도 잡은 끝까지 간다 — 기록하고 계속한다")
    void completeDespiteIllegalTransition() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.EXPIRED);
        issued(issuanceId, AS_OF.minusHours(3));
        seed.history(issuanceId, IssuanceEventType.EXPIRE,
                IssuanceStatus.USED, IssuanceStatus.EXPIRED, AS_OF.minusHours(2));

        JobExecution execution = launch(1);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(statesOf(issuanceId)).containsExactly("EXPIRED");
    }

    @Test
    @DisplayName("불법 전이가 V4 검출로 남는다 — 접기가 계산한 것을 같은 청크에서 쓴다")
    void recordIllegalTransitionAsFinding() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.EXPIRED);
        issued(issuanceId, AS_OF.minusHours(3));
        long illegal = seed.history(issuanceId, IssuanceEventType.EXPIRE,
                IssuanceStatus.USED, IssuanceStatus.EXPIRED, AS_OF.minusHours(2));

        launch(1);

        assertThat(findingTargetKeys()).containsExactly("HISTORY:" + illegal);
        assertThat(findingTypesOf()).containsExactly("ILLEGAL_TRANSITION");
    }

    @Test
    @DisplayName("합법 전이만 있으면 검출이 없다 — 정상셋 0건이 성립해야 한다")
    void recordNoFindingForLegalHistory() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.USED);
        issued(issuanceId, AS_OF.minusHours(3));
        used(issuanceId, AS_OF.minusHours(2));
        seed.usage(issuanceId, AS_OF.minusHours(2), null);

        launch(1);

        assertThat(findingTargetKeys()).isEmpty();
    }

    @Test
    @DisplayName("종단 상태에서 되살리는 이력 하나가 검출 하나를 낸다 — 오염 유형 4 의 모양이다")
    void recordOneFindingPerIllegalHistory() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.USED);
        issued(issuanceId, AS_OF.minusHours(4));
        seed.history(issuanceId, IssuanceEventType.EXPIRE,
                IssuanceStatus.ISSUED, IssuanceStatus.EXPIRED, AS_OF.minusHours(3));
        long revived = seed.history(issuanceId, IssuanceEventType.USE,
                IssuanceStatus.EXPIRED, IssuanceStatus.USED, AS_OF.minusHours(2));
        seed.usage(issuanceId, AS_OF.minusHours(2), null);   // 사용 축은 맞춰 V5 를 침묵시킨다

        launch(1);

        assertThat(findingTargetKeys()).containsExactly("HISTORY:" + revived);
        assertThat(statesOf(issuanceId)).containsExactly("USED");
    }

    @Test
    @DisplayName("두 번 돌려도 검출 집합이 같다 — 판정이 이 집합으로 이뤄진다")
    void produceSameFindingsOnRerun() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.EXPIRED);
        issued(issuanceId, AS_OF.minusHours(3));
        seed.history(issuanceId, IssuanceEventType.EXPIRE,
                IssuanceStatus.USED, IssuanceStatus.EXPIRED, AS_OF.minusHours(2));

        long runOne = launch(1).getExecutionContext().getLong("runId");
        long runTwo = launch(2).getExecutionContext().getLong("runId");

        assertThat(findingKeysOf(runOne)).isEqualTo(findingKeysOf(runTwo)).isNotEmpty();
    }

    @Test
    @DisplayName("이력이 하나도 없어도 잡은 정상 종료한다")
    void completeOnEmptyDataset() throws Exception {
        JobExecution execution = launch(1);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(asOfStateCount()).isZero();
    }

    @Test
    @DisplayName("attempt 를 올리면 같은 asOf 로 다시 돈다 — 결정론 증명의 전제다")
    void rerunSameAsOfWithNextAttempt() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED);
        issued(issuanceId, AS_OF.minusHours(1));

        JobExecution first = launch(1);
        JobExecution second = launch(2);

        assertThat(first.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(second.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(runCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("두 번 돌리면 asof_state 전체가 바이트 단위로 같다 — 결정론의 실체다")
    void produceIdenticalAsOfStateOnRerun() throws Exception {
        long first = seed.issuance(IssuanceStatus.USED);
        issued(first, AS_OF.minusHours(3));
        used(first, AS_OF.minusHours(2));
        seed.usage(first, AS_OF.minusHours(2), null);

        long second = seed.issuance(IssuanceStatus.ISSUED);
        issued(second, AS_OF.minusHours(1));

        long runOne = launch(1).getExecutionContext().getLong("runId");
        long runTwo = launch(2).getExecutionContext().getLong("runId");

        assertThat(digestOf(runOne)).isEqualTo(digestOf(runTwo)).isNotNull();
    }

    @Test
    @DisplayName("같은 시각 이력이 섞여도 두 실행이 같다 — 타이브레이커가 빠지면 여기서 갈린다")
    void stayIdenticalWhenHistoriesShareTimestamp() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.USED);
        issued(issuanceId, AS_OF.minusHours(1));
        used(issuanceId, AS_OF.minusHours(1));

        long runOne = launch(1).getExecutionContext().getLong("runId");
        long runTwo = launch(2).getExecutionContext().getLong("runId");

        assertThat(digestOf(runOne)).isEqualTo(digestOf(runTwo));
    }

    @Test
    @DisplayName("run 마다 따로 쌓인다 — asof_state 는 run 단위로 재생성한다")
    void keepStatePerRun() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED);
        issued(issuanceId, AS_OF.minusHours(1));

        launch(1);
        launch(2);

        assertThat(asOfStateCount()).isEqualTo(2);
    }

    // ─────────────────────────── V1 재고 정합 ───────────────────────────

    @Test
    @DisplayName("접은 활성 건수와 재고가 어긋나면 V1 검출이 남는다 — 오염 유형 1 의 모양이다")
    void recordStockMismatchAsFinding() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED);
        issued(issuanceId, AS_OF.minusHours(1));
        seed.overwriteStock(5);                       // 접기는 1인데 재고는 5

        launch(1);

        assertThat(findingTargetKeys()).containsExactly("COUPON:" + seed.currentCouponId());
        assertThat(findingTypesOf()).containsExactly("STOCK_MISMATCH");
    }

    @Test
    @DisplayName("재고가 맞으면 V1 검출이 없다 — 정상셋 0건이 성립해야 한다")
    void recordNoStockMismatchWhenCountsAgree() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED);
        issued(issuanceId, AS_OF.minusHours(1));

        launch(1);

        assertThat(findingTargetKeys()).isEmpty();
    }

    @Test
    @DisplayName("asOf 이후에 갱신된 재고가 있으면 거부한다 — 발급건 축과 대칭이다")
    void rejectStocksUpdatedAfterAsOf() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED);
        issued(issuanceId, AS_OF.minusHours(1));
        seed.overwriteStock(1, AS_OF.plusSeconds(1));   // 시각만 미래로 민다

        JobExecution execution = launch(1);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(failureMessagesOf(execution)).anyMatch(
                m -> m.contains("asOf 이후에 갱신된 재고가 있습니다"));
        assertThat(asOfStateCount())
                .as("시작 가드라 접기 전에 멈춰야 한다")
                .isZero();
    }

    @Test
    @DisplayName("규칙이 읽은 뒤 재고가 갱신되면 끝 가드가 잡는다 — 시작 가드만으로는 57초가 무방비다")
    void failWhenStockIsUpdatedAfterRulesRead() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED);
        issued(issuanceId, AS_OF.minusHours(1));
        mutateStockAfterRead = seed.currentCouponId();   // V1 이 읽은 직후 런타임이 건드리는 상황

        JobExecution execution = launch(1);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(failureMessagesOf(execution)).anyMatch(
                m -> m.contains("실행 중에 재고가 갱신됐습니다"));
    }

    @Test
    @DisplayName("실행 중에 정책이 바뀌면 끝 가드가 잡는다 — 지문은 같은데 검출만 달라지는 자리다")
    void failWhenPolicyChangesDuringRun() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED);
        issued(issuanceId, AS_OF.minusHours(1));
        mutatePolicyAfterRead = seed.currentCouponId();

        JobExecution execution = launch(1);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(failureMessagesOf(execution)).anyMatch(
                m -> m.contains("실행 중에 회차 정책 또는 등급 체계가 바뀌었습니다"));
    }

    /**
     * <b>이력 축도 얼려야 한다.</b> 리플레이는 시작에 얼린 {@code maxHistoryId} 까지만 읽는데
     * {@code dataset_fingerprint} 는 {@code MAX(id) WHERE created_at <= asOf} 를 다시 잰다.
     * 그 사이 백데이트 이력이 들어오면 <b>리플레이는 못 읽고 지문은 읽어</b>,
     * 같은 지문에 다른 검출이 나온다 — 판정표가 "검증기 버그" 로 잘못 읽는 칸이다.
     */
    @Test
    @DisplayName("실행 중에 asOf 이하 이력이 끼어들면 끝 가드가 잡는다")
    void failWhenBackdatedHistoryAppearsDuringRun() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED);
        issued(issuanceId, AS_OF.minusHours(2));
        insertHistoryAfterFreeze = issuanceId;

        JobExecution execution = launch(1);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(failureMessagesOf(execution))
                .anyMatch(m -> m.contains("asOf 이하 이력이 추가됐습니다"));
    }

    /**
     * <b>라벨과 실제 스키마가 어긋나면 시작도 못 한다.</b> {@code dataset} 은
     * {@code verification_runs} 에 적히는 이름일 뿐이라, 이 가드가 없으면
     * <b>CLEAN DB 를 보면서 "CORRUPT 에서 검출 0건" 으로 기록</b>된다.
     * 오염을 심을 수 없는 스키마라 0건이 나오는 것이 당연한데, 기록만 보면 합격으로 읽힌다.
     */
    @Test
    @DisplayName("CLEAN 스키마인데 dataset=CORRUPT 면 거부한다 — 0건이 합격으로 읽히는 자리다")
    void rejectCorruptLabelOnCleanSchema() throws Exception {
        JobExecution execution = jobOperator.start(verifyJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .addString("scope", "FULL")
                .addString("dataset", "CORRUPT")
                .addLong("attempt", 1L)
                .toJobParameters());

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(failureMessagesOf(execution))
                .anyMatch(m -> m.contains("uk_coupon_member 가 살아 있습니다"));
    }

    /**
     * <b>주입 700 이 정답 800 이 되는 자리다.</b> 유형 3(CANCEL_USE 이중 기록)만 두 규칙을
     * 동시에 울린다 — {@code FindingType} 이 그 산술을 명시하는데, 그 조합을 실제로 세워
     * <b>정확히 그 둘만</b> 나오는지 보는 테스트가 없었다.
     *
     * <p><b>침묵 쪽이 이 테스트의 본체다.</b> 접힌 최종 상태가 {@code ISSUED} 라 저장값과 맞아
     * V3 가 조용해야 하고, 활성 사용이 없어 V5 도 조용해야 한다. 그래서
     * {@code containsExactlyInAnyOrder} 여야 한다 — {@code contains} 면 오탐이 붙어도 초록이다.
     *
     * <p>다만 <b>접기가 {@code to_status} 를 따라가는지</b>는 이 시나리오로 확인되지 않는다.
     * 여기서는 직전 상태가 이미 {@code ISSUED} 라 따라가든 안 가든 결과가 같다(돌연변이로 확인).
     * 그 동작은 {@code completeDespiteIllegalTransition} 등 유형 4 테스트가 지킨다.
     */
    @Test
    @DisplayName("CANCEL_USE 가 두 번이면 V4 와 V1 만 운다 — 주입 100 이 정답 200 이 되는 자리다")
    void recordStockAndTransitionForDoubleCancelUse() throws Exception {
        long couponId = seed.currentCouponIdOrCreate();
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED);
        issued(issuanceId, AS_OF.minusHours(4));
        seed.history(issuanceId, IssuanceEventType.USE,
                IssuanceStatus.ISSUED, IssuanceStatus.USED, AS_OF.minusHours(3));
        seed.history(issuanceId, IssuanceEventType.CANCEL_USE,
                IssuanceStatus.USED, IssuanceStatus.ISSUED, AS_OF.minusHours(2));
        long forged = seed.history(issuanceId, IssuanceEventType.CANCEL_USE,
                IssuanceStatus.USED, IssuanceStatus.ISSUED, AS_OF.minusHours(1));
        seed.overwriteStock(0);   // 재고 이중 복원 — 접기는 활성 1, 저장은 0

        assertThat(launch(1).getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(findingsOf()).containsExactlyInAnyOrder(
                "ILLEGAL_TRANSITION:HISTORY:" + forged,
                "STOCK_MISMATCH:COUPON:" + couponId);
    }

    // ─────────────────────────── V6 등급 자격 ───────────────────────────

    @Test
    @DisplayName("허용 집합에 없는 등급이면 V6 검출이 남는다")
    void recordGradeViolationAsFinding() throws Exception {
        seed.restrictCouponTo(12);           // {GOLD, VIP}
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED, "SILVER");
        issued(issuanceId, AS_OF.minusHours(1));

        launch(1);

        assertThat(findingTargetKeys()).containsExactly("ISSUANCE:" + issuanceId);
        assertThat(findingTypesOf()).containsExactly("GRADE_VIOLATION");
    }

    @Test
    @DisplayName("허용 등급이면 V6 검출이 없다 — 오염 유형이 없어 정상셋 0건이 유일한 검증이다")
    void recordNoGradeViolationForEligibleGrade() throws Exception {
        seed.restrictCouponTo(12);
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED, "GOLD");
        issued(issuanceId, AS_OF.minusHours(1));

        launch(1);

        assertThat(findingTargetKeys()).isEmpty();
    }

    @Test
    @DisplayName("한 발급건이 V6 와 V3 를 함께 울릴 수 있다 — 주입 700 이 정답 800 이 되는 이유다")
    void recordTwoFindingsForOneIssuance() throws Exception {
        seed.restrictCouponTo(12);
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED, "SILVER");
        issued(issuanceId, AS_OF.minusHours(2));
        used(issuanceId, AS_OF.minusHours(1));   // 접기는 USED, 저장은 ISSUED -> V3
        seed.usage(issuanceId, AS_OF.minusHours(1), null);

        launch(1);

        assertThat(findingTypesOf())
                .containsExactlyInAnyOrder("GRADE_VIOLATION", "REPLAY_MISMATCH");
    }

    private void issued(long issuanceId, LocalDateTime at) {
        seed.history(issuanceId, IssuanceEventType.ISSUE, null, IssuanceStatus.ISSUED, at);
    }

    private void used(long issuanceId, LocalDateTime at) {
        seed.history(issuanceId, IssuanceEventType.USE,
                IssuanceStatus.ISSUED, IssuanceStatus.USED, at);
    }

    private JobExecution launch(int attempt) throws Exception {
        JobParameters parameters = new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .addString("scope", "FULL")
                .addString("dataset", "CLEAN")
                .addLong("attempt", (long) attempt)
                .toJobParameters();

        return jobOperator.start(verifyJob, parameters);
    }

    private List<String> statesOf(long issuanceId) {
        return jdbcClient.sql("SELECT state FROM asof_state WHERE coupon_id = :id ORDER BY run_id")
                .param("id", issuanceId)
                .query(String.class)
                .list();
    }

    private List<Integer> usageCountsOf(long issuanceId) {
        return jdbcClient.sql(
                        "SELECT active_usage_count FROM asof_state WHERE coupon_id = :id ORDER BY run_id")
                .param("id", issuanceId)
                .query(Integer.class)
                .list();
    }

    /**
     * 상태 한 컬럼만 보면 last_history_id 가 뒤바뀌어도, 발급건 하나가 통째로 빠져도 통과한다.
     * 행 전체를 정렬해 접어야 타이브레이커 회귀가 드러난다.
     */
    private String digestOf(long runId) {
        jdbcClient.sql("SET SESSION group_concat_max_len = 1048576").update();

        return jdbcClient.sql("""
                        SELECT SHA2(GROUP_CONCAT(
                                 CONCAT_WS(0x1f, coupon_id, state,
                                           COALESCE(last_history_id, ''), last_event_at,
                                           active_usage_count)
                                 ORDER BY coupon_id SEPARATOR 0x1e), 256)
                          FROM asof_state WHERE run_id = :runId
                        """)
                .param("runId", runId)
                .query(String.class)
                .single();
    }

    @Test
    @DisplayName("접은 상태와 저장값이 어긋나면 V3 로 남는다 — 오염 유형 2 의 모양이다")
    void recordReplayMismatch() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED);
        issued(issuanceId, AS_OF.minusHours(3));
        used(issuanceId, AS_OF.minusHours(2));
        seed.usage(issuanceId, AS_OF.minusHours(2), null);   // 사용 축은 맞춰 V5 를 침묵시킨다

        launch(1);

        assertThat(findingsOf()).containsExactly(
                "REPLAY_MISMATCH:ISSUANCE:" + issuanceId);
    }

    @Test
    @DisplayName("ISSUED 인데 활성 사용이 남아 있으면 V5 로 남는다 — 오염 유형 7 의 모양이다")
    void recordUsageMismatch() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED);
        issued(issuanceId, AS_OF.minusHours(3));
        seed.usage(issuanceId, AS_OF.minusHours(2), null);

        launch(1);

        assertThat(findingsOf()).containsExactly(
                "USAGE_MISMATCH:ISSUANCE:" + issuanceId);
    }

    @Test
    @DisplayName("정상 발급건만 있으면 검출이 하나도 없다 — 정상셋 0건이 이 잡의 합격 조건이다")
    void recordNothingForCleanData() throws Exception {
        long used = seed.issuance(IssuanceStatus.USED);
        issued(used, AS_OF.minusHours(3));
        used(used, AS_OF.minusHours(2));
        seed.usage(used, AS_OF.minusHours(2), null);

        long issuedOnly = seed.issuance(IssuanceStatus.ISSUED);
        issued(issuedOnly, AS_OF.minusHours(1));

        launch(1);

        assertThat(findingsOf()).isEmpty();
    }

    @Test
    @DisplayName("한 발급건이 두 규칙을 함께 울릴 수 있다 — 주입 수와 정답 행수가 다른 이유다")
    void recordMultipleRulesForOneIssuance() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.EXPIRED);
        issued(issuanceId, AS_OF.minusHours(3));
        used(issuanceId, AS_OF.minusHours(2));
        // 접기는 USED(활성)인데 저장은 EXPIRED 라 시드가 재고를 안 만들었다.
        // 재고 축을 맞춰야 V1 이 침묵하고 의도한 두 규칙만 남는다.
        seed.overwriteStock(1);

        launch(1);

        assertThat(findingsOf()).containsExactlyInAnyOrder(
                "REPLAY_MISMATCH:ISSUANCE:" + issuanceId,
                "USAGE_MISMATCH:ISSUANCE:" + issuanceId);
    }

    @Test
    @DisplayName("두 번 돌려도 검출 집합이 같다 — 판정이 이 집합으로 이뤄진다")
    void produceSameFindingSetAcrossRules() throws Exception {
        // V1·V6 을 함께 태운다. 둘 다 deterministic=false 로 분류한 규칙이라 위험이 가장 큰데,
        // 정작 "두 번 돌려 같은가" 를 이 둘에 대고 확인한 적이 없었다.
        // V1 은 run 마다 asof_state 를 새로 만들고 현재 재고를 읽고,
        // V6 은 살아 있는 coupons.eligible_grades_mask 를 읽는다.
        seed.restrictCouponTo(12);                            // {GOLD, VIP}
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED, "SILVER");
        issued(issuanceId, AS_OF.minusHours(3));
        used(issuanceId, AS_OF.minusHours(2));
        seed.usage(issuanceId, AS_OF.minusHours(2), null);
        seed.overwriteStock(5);                                        // 접기 1 vs 재고 5

        long runOne = launch(1).getExecutionContext().getLong("runId");
        long runTwo = launch(2).getExecutionContext().getLong("runId");

        assertThat(findingKeysOf(runOne))
                .as("계약의 checksum 입력이 (finding_type, target_key) 라 이 비교가 그 대리물이다")
                .containsExactlyInAnyOrder(
                        "STOCK_MISMATCH:COUPON:" + seed.currentCouponId(),
                        "REPLAY_MISMATCH:ISSUANCE:" + issuanceId,
                        "GRADE_VIOLATION:ISSUANCE:" + issuanceId)
                .isEqualTo(findingKeysOf(runTwo));
    }

    private List<String> findingsOf() {
        return jdbcClient.sql("""
                        SELECT CONCAT(finding_type, ':', target_key)
                          FROM verification_findings
                         ORDER BY finding_type, target_key
                        """)
                .query(String.class)
                .list();
    }

    private List<String> findingTargetKeys() {
        return jdbcClient.sql("SELECT target_key FROM verification_findings ORDER BY id")
                .query(String.class)
                .list();
    }

    private List<String> findingTypesOf() {
        return jdbcClient.sql("SELECT finding_type FROM verification_findings ORDER BY id")
                .query(String.class)
                .list();
    }

    private List<String> findingKeysOf(long targetRunId) {
        return jdbcClient.sql("""
                        SELECT CONCAT(finding_type, ':', target_key)
                          FROM verification_findings
                         WHERE run_id = :runId
                         ORDER BY finding_type, target_key
                        """)
                .param("runId", targetRunId)
                .query(String.class)
                .list();
    }

    private static List<String> failureMessagesOf(JobExecution execution) {
        return execution.getAllFailureExceptions().stream()
                .map(Throwable::getMessage)
                .toList();
    }

    private int asOfStateCount() {
        return jdbcClient.sql("SELECT COUNT(*) FROM asof_state").query(Integer.class).single();
    }

    private int runCount() {
        return jdbcClient.sql("SELECT COUNT(*) FROM verification_runs")
                .query(Integer.class)
                .single();
    }
}
