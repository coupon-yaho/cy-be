package com.kafkick.api.admin.dashboard.mock;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import org.springframework.stereotype.Component;

import com.kafkick.core.admin.overview.CampaignOverviewSource;
import com.kafkick.core.admin.overview.OverviewCalculationPolicy;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.admin.overview.calculator.CampaignQueueCalculator.QueueInput;
import com.kafkick.core.admin.overview.calculator.CustomerOutcomeCalculator.OutcomeCount;
import com.kafkick.core.admin.overview.calculator.CustomerOutcomeCalculator.OutcomeInput;
import com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator.IssuanceBucket;
import com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator.IssuanceFlowInput;
import com.kafkick.core.coupon.CouponStatus;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.Severity;

import static com.kafkick.core.observation.SourceStatus.*;

/**
 * 캠페인 저장소가 준비되기 전 관리자 운영현황 화면 Fixture를 생성합니다.
 *
 * <p>실행 날짜에 따라 오픈 임박 판정이 달라지지 않도록 절대 날짜를 저장하지 않고, 호출자가 전달한
 * 스냅샷 시각을 기준으로 모든 캠페인 시각을 상대적으로 생성합니다. 이 정책·수치는 운영 기본값이
 * 아니라 화면 조립·표시 시나리오 전용 Fixture입니다.</p>
 */
@Component
public class AdminOverviewMockDataFactory {

    /**
     * O1~O4 조립을 검증할 운영·오픈 임박·준비 미완료·종료 화면 시나리오를 생성합니다.
     *
     * @param snapshotAt 캠페인 시각과 조치 감지 시각을 만드는 기준 시각
     * @return 동일한 기준 시각으로 생성한 캠페인 원천과 조치 후보
     * @throws NullPointerException snapshotAt이 {@code null}인 경우
     */
    public AdminOverviewMockDataset create(Instant snapshotAt) {
        Objects.requireNonNull(snapshotAt, "snapshotAt");

        // TODO: 캠페인별 실제 발급 관측 조회가 제공되면 Mock 입력 생성을 해당 조회 결과로 교체합니다.
        OverviewCalculationPolicy policy = new OverviewCalculationPolicy(
                0.50,
                Duration.ofMinutes(2),
                Duration.ofMinutes(10),
                Duration.ofMinutes(2),
                Duration.ofMinutes(10));
        CampaignOverviewSource admissionStoppedCampaign = new CampaignOverviewSource(
                101L,
                "입장 중단 쿠폰",
                "카프킥",
                CouponStatus.OPEN,
                snapshotAt.minus(Duration.ofHours(1)),
                snapshotAt.plus(Duration.ofHours(2)),
                EngineVersion.V1,
                15_000L,
                10_350L,
                snapshotAt,
                VALID,
                true
        );
        CampaignOverviewSource depletionCampaign = new CampaignOverviewSource(
                102L,
                "소진 임박 쿠폰",
                "카프킥",
                CouponStatus.OPEN,
                snapshotAt.minus(Duration.ofMinutes(30)),
                snapshotAt.plus(Duration.ofHours(4)),
                EngineVersion.V1,
                7_000L,
                6_650L,
                snapshotAt,
                VALID,
                true
        );
        CampaignOverviewSource decreasingQueueCampaign = new CampaignOverviewSource(
                103L,
                "정상 발급 감소 대기 쿠폰",
                "카프킥",
                CouponStatus.OPEN,
                snapshotAt.minus(Duration.ofMinutes(10)),
                snapshotAt.plus(Duration.ofHours(3)),
                EngineVersion.V1,
                null,
                null,
                null,
                UNAVAILABLE,
                true
        );
        CampaignOverviewSource readyScheduledCampaign = new CampaignOverviewSource(
                104L,
                "준비 완료 예약 쿠폰",
                "카프킥",
                CouponStatus.SCHEDULED,
                snapshotAt.plus(Duration.ofMinutes(20)),
                snapshotAt.plus(Duration.ofHours(3)),
                EngineVersion.V1,
                null,
                null,
                null,
                N_A,
                true
        );
        CampaignOverviewSource incompleteCampaign = new CampaignOverviewSource(
                105L, "준비 미완료 예약 쿠폰", "카프킥", CouponStatus.SCHEDULED,
                snapshotAt.plus(Duration.ofMinutes(10)), snapshotAt.plus(Duration.ofHours(3)),
                EngineVersion.V1, null, null, null, N_A, false);
        CampaignOverviewSource closedCampaign = new CampaignOverviewSource(
                106L, "종료된 시즌 쿠폰", "카프킥", CouponStatus.CLOSED,
                snapshotAt.minus(Duration.ofHours(5)), snapshotAt.minus(Duration.ofHours(1)),
                EngineVersion.V1, null, null, null, N_A, true);

        // 준비 미완료 판정은 Mock 원천에서 확정하고 집계 계산기는 판정 결과만 소비합니다.
        AdminOverviewSnapshot.OperationActionItem incompleteAction =
                new AdminOverviewSnapshot.OperationActionItem(
                        incompleteCampaign.couponId(),
                        incompleteCampaign.campaignName(),
                        incompleteCampaign.opensAt(),
                        Severity.WARN,
                        AdminOverviewSnapshot.CustomerImpact.NONE,
                        "오픈 전 필수 준비 항목을 확인해야 합니다.",
                        snapshotAt,
                        null,
                        new AdminOverviewSnapshot.RecommendedAction(
                                AdminOverviewSnapshot.ActionCode.CAMPAIGN_NOT_READY,
                                "캠페인 준비 상태 확인",
                                AdminOverviewSnapshot.TargetScreen.CAMPAIGN_DETAIL)
                );

        Instant windowStart = snapshotAt.minus(Duration.ofMinutes(1));
        List<IssuanceFlowInput> issuanceFlowInputs = List.of(
                issuanceInput(101L, CouponStatus.OPEN, true, 20L, 0L, 40L,
                        snapshotAt.minus(Duration.ofMinutes(12)), null, windowStart, snapshotAt),
                issuanceInput(102L, CouponStatus.OPEN, true, 60L, 44L, 100L,
                        snapshotAt.minus(Duration.ofMinutes(3)), snapshotAt, windowStart, snapshotAt),
                issuanceInput(103L, CouponStatus.OPEN, true, 700L, 612L, 1_000L,
                        snapshotAt.minus(Duration.ofMinutes(1)), snapshotAt, windowStart, snapshotAt),
                notApplicableIssuance(104L, CouponStatus.SCHEDULED),
                notApplicableIssuance(105L, CouponStatus.SCHEDULED),
                notApplicableIssuance(106L, CouponStatus.CLOSED));
        List<QueueInput> queueInputs = List.of(
                queueInput(101L, 3_204L, 3_000L, 0L,
                        snapshotAt.minus(Duration.ofMinutes(12)), windowStart, snapshotAt),
                queueInput(102L, 0L, 0L, 0L,
                        snapshotAt.minus(Duration.ofMinutes(1)), windowStart, snapshotAt),
                queueInput(103L, 184L, 424L, 276L,
                        snapshotAt.minus(Duration.ofMinutes(1)), windowStart, snapshotAt),
                notApplicableQueue(104L), notApplicableQueue(105L), notApplicableQueue(106L));
        OutcomeInput outcomeInput = new OutcomeInput(
                snapshotAt.minus(Duration.ofMinutes(5)), snapshotAt,
                List.of(
                        new OutcomeCount(AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 1_847L),
                        new OutcomeCount(AdminOverviewSnapshot.CustomerOutcomeType.QUEUED, 412L),
                        new OutcomeCount(AdminOverviewSnapshot.CustomerOutcomeType.ALREADY_ISSUED, 238L),
                        new OutcomeCount(AdminOverviewSnapshot.CustomerOutcomeType.STOCK_EXHAUSTED, 81L),
                        new OutcomeCount(AdminOverviewSnapshot.CustomerOutcomeType.INELIGIBLE, 57L),
                        new OutcomeCount(AdminOverviewSnapshot.CustomerOutcomeType.ENTRY_EXPIRED, 34L),
                        new OutcomeCount(AdminOverviewSnapshot.CustomerOutcomeType.SYSTEM_FAILURE, 9L)),
                VALID, snapshotAt);

        return new AdminOverviewMockDataset(policy, issuanceFlowInputs, queueInputs, outcomeInput,
                List.of(admissionStoppedCampaign, depletionCampaign, decreasingQueueCampaign,
                        readyScheduledCampaign, incompleteCampaign, closedCampaign),
                List.of(incompleteAction));
    }

    /** 실제 1분 관측 구간에서 O1 상태와 그래프 점을 만드는 화면 시나리오 보조 메서드입니다. */
    private static IssuanceFlowInput issuanceInput(long couponId, CouponStatus status, boolean stockAvailable,
            long attemptedCount, long completedCount, long comparisonCompletedCount, Instant conditionStartedAt,
            Instant lastCompletedAt, Instant windowStart, Instant snapshotAt) {
        Instant comparisonWindowStart = windowStart.minus(Duration.ofMinutes(1));
        return new IssuanceFlowInput(couponId, status, stockAvailable, windowStart, snapshotAt,
                attemptedCount, completedCount, comparisonCompletedCount, comparisonWindowStart, windowStart,
                List.of(new IssuanceBucket(windowStart, snapshotAt, completedCount)), lastCompletedAt,
                conditionStartedAt, VALID, snapshotAt);
    }

    /** SCHEDULED·CLOSED 캠페인의 비적용 O1 원천을 N_A로 명시합니다. */
    private static IssuanceFlowInput notApplicableIssuance(long couponId, CouponStatus status) {
        return new IssuanceFlowInput(couponId, status, null, null, null, null, null, null,
                null, null, null, null, null, N_A, null);
    }

    /** 실제 1분 관측 구간의 O2 대기·입장 상태를 만드는 화면 시나리오 보조 메서드입니다. */
    private static QueueInput queueInput(long couponId, long currentWaitingCount, long previousWaitingCount,
            long admittedCount, Instant stoppedStartedAt, Instant windowStart, Instant snapshotAt) {
        Instant lastAdmissionAt = admittedCount > 0L ? snapshotAt : null;
        Instant admissionStoppedStartedAt = currentWaitingCount > 0L && admittedCount == 0L
                ? stoppedStartedAt : null;
        return new QueueInput(couponId, currentWaitingCount, previousWaitingCount, admittedCount,
                windowStart, snapshotAt, lastAdmissionAt, admissionStoppedStartedAt,
                VALID, snapshotAt);
    }

    /** SCHEDULED·CLOSED 캠페인의 비적용 O2 원천을 N_A로 명시합니다. */
    private static QueueInput notApplicableQueue(long couponId) {
        return new QueueInput(couponId, null, null, null, null, null, null, null,
                N_A, null);
    }
}
