package com.kafkick.api.admin.dashboard.mock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.overview.CampaignOverviewSource;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.coupon.CouponStatus;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.admin.overview.calculator.CampaignQueueCalculator.QueueInput;
import com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator.IssuanceFlowInput;

/** 관리자 운영현황 선행 구현에 사용할 캠페인 상황별 Mock 원천을 검증합니다. */
class AdminOverviewMockDataFactoryTest {

    private static final Instant SNAPSHOT_AT = Instant.parse("2026-08-21T03:00:00Z");

    /**
     * 하나의 화면 스냅샷이 O1~O3 입력·정책·캠페인 원천·준비 미완료 후보를 같은 불변 Dataset으로
     * 전달하는 계약을 고정합니다.
     */
    @Test
    @DisplayName("Mock Dataset은 화면 조립에 필요한 O1 O2 O3 입력과 정책을 함께 보유한다")
    void exposesAllOverviewCalculationInputsInOneDataset() {
        AdminOverviewMockDataset dataset = new AdminOverviewMockDataFactory().create(SNAPSHOT_AT);

        assertThat(AdminOverviewMockDataset.class.getRecordComponents()).extracting(RecordComponent::getName)
                .containsExactly("policy", "issuanceFlowInputs", "queueInputs", "outcomeInput", "campaigns",
                        "preparationActionCandidates");
        assertThat(dataset.policy().issuanceDecreaseRatio()).isEqualTo(0.50);
        assertThat(dataset.issuanceFlowInputs()).hasSize(6);
        assertThat(dataset.queueInputs()).hasSize(6);
        assertThat(dataset.issuanceFlowInputs())
                .filteredOn(input -> input.couponId().equals(102L))
                .singleElement()
                .satisfies(input -> assertThat(input.completedCount()).isEqualTo(44L));
        assertThat(dataset.issuanceFlowInputs())
                .filteredOn(input -> input.couponId().equals(104L))
                .singleElement()
                .satisfies(input -> assertThat(input.sourceStatus()).isEqualTo(SourceStatus.N_A));
        assertThat(dataset.outcomeInput().counts()).hasSize(7);
        assertThatThrownBy(() -> dataset.queueInputs().add(null))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /** 실행 날짜와 무관하게 동일한 상대 시각과 운영 상태를 제공하는지 검증합니다. */
    @Test
    @DisplayName("Mock 캠페인은 운영 중·오픈 임박·준비 미완료·종료 상황을 제공한다")
    void createsOperationalCampaignScenariosRelativeToSnapshotTime() {
        AdminOverviewMockDataset dataset = new AdminOverviewMockDataFactory().create(SNAPSHOT_AT);

        assertThat(dataset.campaigns())
                .extracting(CampaignOverviewSource::status)
                .containsExactly(
                        CouponStatus.OPEN,
                        CouponStatus.OPEN,
                        CouponStatus.OPEN,
                        CouponStatus.SCHEDULED,
                        CouponStatus.SCHEDULED,
                        CouponStatus.CLOSED);
        assertThat(dataset.campaigns().get(3).opensAt())
                .isEqualTo(SNAPSHOT_AT.plus(Duration.ofMinutes(20)));
        assertThat(dataset.campaigns().get(4).opensAt())
                .isEqualTo(SNAPSHOT_AT.plus(Duration.ofMinutes(10)));
        assertThat(dataset.campaigns().get(4).preparationCompleted()).isFalse();
    }

    /** 준비 미완료 상황이 화면 조치 KPI와 목록에 사용할 판정 후보로 함께 제공되는지 검증합니다. */
    @Test
    @DisplayName("Mock Dataset은 준비 미완료 캠페인의 조치 후보를 제공한다")
    void createsActionCandidateForIncompletePreparation() {
        AdminOverviewMockDataset dataset = new AdminOverviewMockDataFactory().create(SNAPSHOT_AT);

        assertThat(dataset.preparationActionCandidates())
                .singleElement()
                .satisfies(action -> {
                    assertThat(action.couponId()).isEqualTo(105L);
                    assertThat(action.recommendedAction().code())
                            .isEqualTo(AdminOverviewSnapshot.ActionCode.CAMPAIGN_NOT_READY);
                    assertThat(action.detectedAt()).isEqualTo(SNAPSHOT_AT);
                });
    }

    /** Dataset 경계에서 O1·O2·캠페인·준비 후보가 정확히 같은 couponId 모집단을 써야 합니다. */
    @Test
    @DisplayName("Dataset은 중복·누락·여분 couponId와 캠페인 밖 준비 후보를 거부한다")
    void rejectsMismatchedCouponIdPopulations() {
        AdminOverviewMockDataset dataset = new AdminOverviewMockDataFactory().create(SNAPSHOT_AT);

        assertThatThrownBy(() -> datasetWith(dataset,
                List.of(dataset.campaigns().getFirst(), dataset.campaigns().getFirst()),
                dataset.issuanceFlowInputs(), dataset.queueInputs(), dataset.preparationActionCandidates()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> datasetWith(dataset, dataset.campaigns(),
                dataset.issuanceFlowInputs().subList(1, dataset.issuanceFlowInputs().size()),
                dataset.queueInputs(), dataset.preparationActionCandidates()))
                .isInstanceOf(IllegalArgumentException.class);
        List<QueueInput> duplicateQueueInputs = new ArrayList<>(dataset.queueInputs());
        duplicateQueueInputs.add(dataset.queueInputs().getFirst());
        assertThatThrownBy(() -> datasetWith(dataset, dataset.campaigns(), dataset.issuanceFlowInputs(),
                duplicateQueueInputs, dataset.preparationActionCandidates()))
                .isInstanceOf(IllegalArgumentException.class);
        List<QueueInput> extraQueueInputs = new ArrayList<>(dataset.queueInputs());
        extraQueueInputs.add(new QueueInput(999L, null, null, null, null, null, null, null,
                SourceStatus.UNAVAILABLE, null));
        assertThatThrownBy(() -> datasetWith(dataset, dataset.campaigns(), dataset.issuanceFlowInputs(),
                extraQueueInputs, dataset.preparationActionCandidates()))
                .isInstanceOf(IllegalArgumentException.class);
        AdminOverviewSnapshot.OperationActionItem foreignCandidate =
                new AdminOverviewSnapshot.OperationActionItem(999L, "외부", null, null, null, null, null,
                        null, null);
        assertThatThrownBy(() -> datasetWith(dataset, dataset.campaigns(), dataset.issuanceFlowInputs(),
                dataset.queueInputs(), List.of(foreignCandidate)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 원천을 목록에서 빼지 않고 UNAVAILABLE로 명시하면 같은 모집단 계약을 유지합니다. */
    @Test
    @DisplayName("Dataset은 같은 couponId의 명시적 UNAVAILABLE O1 O2 입력을 허용한다")
    void acceptsExplicitUnavailableInputsForExistingCampaign() {
        AdminOverviewMockDataset dataset = new AdminOverviewMockDataFactory().create(SNAPSHOT_AT);
        List<IssuanceFlowInput> issuanceInputs = new ArrayList<>(dataset.issuanceFlowInputs());
        List<QueueInput> queueInputs = new ArrayList<>(dataset.queueInputs());
        issuanceInputs.set(0, new IssuanceFlowInput(101L, CouponStatus.OPEN, null, null, null, null, null,
                null, null, null, null, null, null, SourceStatus.UNAVAILABLE, null));
        queueInputs.set(0, new QueueInput(101L, null, null, null, null, null, null, null,
                SourceStatus.UNAVAILABLE, null));

        AdminOverviewMockDataset result = datasetWith(dataset, dataset.campaigns(), issuanceInputs, queueInputs,
                dataset.preparationActionCandidates());

        assertThat(result.issuanceFlowInputs().getFirst().sourceStatus()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(result.queueInputs().getFirst().sourceStatus()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /** 비교 count와 중단 지속 시간은 실제로 분리된 관측 구간·마지막 성공 시각으로 표현해야 합니다. */
    @Test
    @DisplayName("Mock O1은 직전 비교 구간과 중단 시작 이전의 마지막 성공 시각을 사용한다")
    void createsConsistentIssuanceTimeSources() {
        AdminOverviewMockDataset dataset = new AdminOverviewMockDataFactory().create(SNAPSHOT_AT);
        IssuanceFlowInput decreasing = dataset.issuanceFlowInputs().stream()
                .filter(input -> input.couponId().equals(102L))
                .findFirst()
                .orElseThrow();
        IssuanceFlowInput stopped = dataset.issuanceFlowInputs().getFirst();

        assertThat(decreasing.comparisonWindowEnd()).isEqualTo(decreasing.windowStart());
        assertThat(decreasing.comparisonWindowStart()).isEqualTo(SNAPSHOT_AT.minus(Duration.ofMinutes(2)));
        assertThat(stopped.lastCompletedAt()).isNull();
    }

    /** 한 독립 재고 원천만 미수집이어도 같은 OPEN 캠페인의 O1·O2 입력은 보존해야 합니다. */
    @Test
    @DisplayName("Mock은 OPEN 캠페인의 재고만 UNAVAILABLE이고 O1 O2는 유지되는 시나리오를 제공한다")
    void createsUnavailableStockOnlyScenario() {
        AdminOverviewMockDataset dataset = new AdminOverviewMockDataFactory().create(SNAPSHOT_AT);

        assertThat(dataset.campaigns()).filteredOn(source -> source.couponId().equals(103L))
                .singleElement()
                .satisfies(source -> {
                    assertThat(source.stockStatus()).isEqualTo(SourceStatus.UNAVAILABLE);
                    assertThat(source.totalQuantity()).isNull();
                    assertThat(source.activeCount()).isNull();
                    assertThat(source.stockObservedAt()).isNull();
                });
        assertThat(dataset.issuanceFlowInputs()).filteredOn(input -> input.couponId().equals(103L))
                .singleElement()
                .satisfies(input -> assertThat(input.sourceStatus()).isEqualTo(SourceStatus.VALID));
        assertThat(dataset.queueInputs()).filteredOn(input -> input.couponId().equals(103L))
                .singleElement()
                .satisfies(input -> assertThat(input.sourceStatus()).isEqualTo(SourceStatus.VALID));
    }

    /** 재사용 가능한 기본 Dataset에서 일부 목록만 바꾼 생성자 계약 검증 보조 메서드입니다. */
    private static AdminOverviewMockDataset datasetWith(
            AdminOverviewMockDataset base,
            List<CampaignOverviewSource> campaigns,
            List<IssuanceFlowInput> issuanceInputs,
            List<QueueInput> queueInputs,
            List<AdminOverviewSnapshot.OperationActionItem> preparationCandidates
    ) {
        return new AdminOverviewMockDataset(base.policy(), issuanceInputs, queueInputs, base.outcomeInput(),
                campaigns, preparationCandidates);
    }
}
