package com.kafkick.core.admin.overview;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;

/** 운영현황 기술 중립 Snapshot 계약이 Core 경계에서 안전하게 사용되는지 검증합니다. */
class AdminOverviewCoreContractTest {

    private static final Instant FROM = Instant.parse("2026-08-17T00:00:00Z");
    private static final Instant TO = Instant.parse("2026-08-17T01:00:00Z");

    /** 조치 항목이 고객 영향 코드와 설명을 함께 보존하는지 검증합니다. */
    @Test
    void actionItemPreservesCustomerImpact() {
        AdminOverviewSnapshot.OperationActionItem item = new AdminOverviewSnapshot.OperationActionItem(
                1L,
                "캠페인",
                FROM,
                Severity.WARN,
                AdminOverviewSnapshot.CustomerImpact.LIMITED,
                "일부 고객 대기",
                FROM,
                Duration.ofMinutes(1),
                new AdminOverviewSnapshot.RecommendedAction(
                        AdminOverviewSnapshot.ActionCode.QUEUE_STALLED,
                        "대기열 확인",
                        AdminOverviewSnapshot.TargetScreen.METRICS));

        assertThat(item.customerImpact()).isEqualTo(AdminOverviewSnapshot.CustomerImpact.LIMITED);
        assertThat(item.customerImpactText()).isEqualTo("일부 고객 대기");
    }

    /** Core 요약은 전체 건수와 우선 노출할 최대 20개를 별도로 보존합니다. */
    @Test
    void actionItemSnapshotPreservesTotalAndRanksTopItems() {
        AdminOverviewSnapshot.OperationActionItem warn = actionItem(4L, Severity.WARN, FROM);
        AdminOverviewSnapshot.OperationActionItem criticalNew = actionItem(3L, Severity.CRITICAL, TO);
        AdminOverviewSnapshot.OperationActionItem criticalOld = actionItem(2L, Severity.CRITICAL, FROM);
        AdminOverviewSnapshot.OperationActionItem criticalUnknown = actionItem(1L, Severity.CRITICAL, null);

        AdminOverviewSnapshot.ActionItemSnapshot snapshot = new AdminOverviewSnapshot.ActionItemSnapshot(
                100, List.of(warn, criticalNew, criticalUnknown, criticalOld));

        assertThat(snapshot.totalCount()).isEqualTo(100);
        assertThat(snapshot.topItems())
                .extracting(AdminOverviewSnapshot.OperationActionItem::couponId)
                .containsExactly(2L, 3L, 1L, 4L);
    }

    /** 심각도와 감지 시각이 같아도 couponId가 입력 순서와 무관한 최종 순서를 결정합니다. */
    @Test
    void actionItemSnapshotUsesCouponIdAsDeterministicTieBreaker() {
        AdminOverviewSnapshot.OperationActionItem lowerId = actionItem(1L, Severity.CRITICAL, FROM);
        AdminOverviewSnapshot.OperationActionItem higherId = actionItem(2L, Severity.CRITICAL, FROM);

        AdminOverviewSnapshot.ActionItemSnapshot forward =
                new AdminOverviewSnapshot.ActionItemSnapshot(2, List.of(lowerId, higherId));
        AdminOverviewSnapshot.ActionItemSnapshot reversed =
                new AdminOverviewSnapshot.ActionItemSnapshot(2, List.of(higherId, lowerId));

        assertThat(forward.topItems())
                .extracting(AdminOverviewSnapshot.OperationActionItem::couponId)
                .containsExactly(1L, 2L);
        assertThat(reversed.topItems()).isEqualTo(forward.topItems());
    }

    /** 쿠폰에 연결되지 않은 조치 항목은 생성할 수 없습니다. */
    @Test
    void actionItemRejectsNullCouponId() {
        assertThatThrownBy(() -> actionItem(null, Severity.CRITICAL, FROM))
                .isInstanceOf(NullPointerException.class);
    }

    /** 쿠폰별 조치 항목은 최대 하나이므로 동일 couponId가 중복되면 거부합니다. */
    @Test
    void actionItemSnapshotRejectsDuplicateCouponIds() {
        AdminOverviewSnapshot.OperationActionItem first = actionItem(1L, Severity.WARN, FROM);
        AdminOverviewSnapshot.OperationActionItem duplicate = actionItem(1L, Severity.CRITICAL, TO);

        assertThatThrownBy(() -> new AdminOverviewSnapshot.ActionItemSnapshot(
                2, List.of(first, duplicate)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 호출자가 원본 목록을 바꿔도 Snapshot의 상위 목록은 변하지 않습니다. */
    @Test
    void actionItemSnapshotDefensivelyCopiesTopItems() {
        List<AdminOverviewSnapshot.OperationActionItem> items = new ArrayList<>();
        items.add(actionItem(1L, Severity.WARN, FROM));

        AdminOverviewSnapshot.ActionItemSnapshot snapshot =
                new AdminOverviewSnapshot.ActionItemSnapshot(1, items);
        items.clear();

        assertThat(snapshot.topItems()).hasSize(1);
        assertThatThrownBy(() -> snapshot.topItems().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /** 전체 건수와 상위 목록 크기가 서로 모순되거나 상위 목록이 20개를 넘으면 거부합니다. */
    @Test
    void actionItemSnapshotRejectsInvalidCounts() {
        List<AdminOverviewSnapshot.OperationActionItem> twentyOneItems =
                java.util.stream.LongStream.rangeClosed(1, 21)
                        .mapToObj(id -> actionItem(id, Severity.WARN, FROM))
                        .toList();

        assertThatThrownBy(() -> new AdminOverviewSnapshot.ActionItemSnapshot(-1, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminOverviewSnapshot.ActionItemSnapshot(
                0, List.of(actionItem(1L, Severity.WARN, FROM))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminOverviewSnapshot.ActionItemSnapshot(21, twentyOneItems))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 발급 흐름이 생성 후 원본 points 변경의 영향을 받지 않고 수정 불가능한 목록을 노출합니다. */
    @Test
    void issuanceFlowDefensivelyCopiesPoints() {
        List<AdminOverviewSnapshot.IssuanceRatePoint> points = new ArrayList<>();
        points.add(new AdminOverviewSnapshot.IssuanceRatePoint(FROM, 10.0));

        AdminOverviewSnapshot.IssuanceFlow flow = new AdminOverviewSnapshot.IssuanceFlow(
                10.0, FROM, TO, points,
                AdminOverviewSnapshot.IssuanceFlowState.NORMAL, Duration.ZERO);
        points.clear();

        assertThat(flow.points()).hasSize(1);
        assertThatThrownBy(() -> flow.points().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /** 고객 결과가 생성 후 원본 outcomes 변경의 영향을 받지 않고 수정 불가능한 목록을 노출합니다. */
    @Test
    void customerOutcomeSummaryDefensivelyCopiesOutcomes() {
        List<AdminOverviewSnapshot.CustomerOutcome> outcomes = new ArrayList<>();
        outcomes.add(new AdminOverviewSnapshot.CustomerOutcome(
                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 1, 1.0, "정상 발급"));

        AdminOverviewSnapshot.CustomerOutcomeSummary summary =
                new AdminOverviewSnapshot.CustomerOutcomeSummary(FROM, TO, 1, outcomes);
        outcomes.clear();

        assertThat(summary.outcomes()).hasSize(1);
        assertThatThrownBy(() -> summary.outcomes().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /** O3 public value는 window·type·count·ratio가 하나의 일관된 요약을 이루어야 합니다. */
    @Test
    void customerOutcomeSummaryRejectsCrossFieldContradictions() {
        AdminOverviewSnapshot.CustomerOutcome issued = new AdminOverviewSnapshot.CustomerOutcome(
                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 0.1d, 1d / 3d, "발급");
        AdminOverviewSnapshot.CustomerOutcome queued = new AdminOverviewSnapshot.CustomerOutcome(
                AdminOverviewSnapshot.CustomerOutcomeType.QUEUED, 0.2d, 2d / 3d, "대기");

        assertThat(new AdminOverviewSnapshot.CustomerOutcomeSummary(
                FROM, TO, 0.3d, List.of(issued, queued)).totalCount()).isEqualTo(0.3d);
        assertThatThrownBy(() -> new AdminOverviewSnapshot.CustomerOutcomeSummary(
                null, TO, 0.3d, List.of(issued, queued))).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new AdminOverviewSnapshot.CustomerOutcomeSummary(
                TO, FROM, 0.3d, List.of(issued, queued))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminOverviewSnapshot.CustomerOutcomeSummary(
                FROM, TO, 0.2d, List.of(issued, issued))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminOverviewSnapshot.CustomerOutcomeSummary(
                FROM, TO, 0.4d, List.of(issued, queued))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminOverviewSnapshot.CustomerOutcomeSummary(
                FROM, TO, 0d, List.of(new AdminOverviewSnapshot.CustomerOutcome(
                        AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 0d, 0d, "발급 없음"))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminOverviewSnapshot.CustomerOutcomeSummary(
                FROM, TO, 0.1d, List.of(new AdminOverviewSnapshot.CustomerOutcome(
                        AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 0.1d, 0.5d, "잘못된 비율"))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 가장 작은 양수 total도 빈 outcomes와 함께일 수 없습니다. */
    @Test
    void customerOutcomeSummaryRejectsEmptyPositiveTotal() {
        assertThatThrownBy(() -> new AdminOverviewSnapshot.CustomerOutcomeSummary(
                FROM, TO, Double.MIN_VALUE, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 큰 total에서도 반올림 잡음보다 큰 실제 count 합계 불일치를 허용하지 않습니다. */
    @Test
    void customerOutcomeSummaryRejectsLargeTotalMismatch() {
        double total = 1_000_000_000_000_000d;
        double count = total - 500d;
        assertThatThrownBy(() -> new AdminOverviewSnapshot.CustomerOutcomeSummary(
                FROM, TO, total, List.of(new AdminOverviewSnapshot.CustomerOutcome(
                        AdminOverviewSnapshot.CustomerOutcomeType.ISSUED,
                        count,
                        count / total,
                        "발급"))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 전체 Snapshot이 생성 후 원본 campaigns 변경의 영향을 받지 않고 수정 불가능한 목록을 노출합니다. */
    @Test
    void snapshotDefensivelyCopiesCampaigns() {
        List<AdminOverviewSnapshot.CampaignOverview> campaigns = new ArrayList<>();
        campaigns.add(campaign(1L));
        AdminOverviewSnapshot.Observation<List<AdminOverviewSnapshot.CampaignOverview>> observation =
                new AdminOverviewSnapshot.Observation<>(campaigns, SourceStatus.VALID, FROM);

        AdminOverviewSnapshot snapshot = new AdminOverviewSnapshot(
                FROM, null, null, null, null, null, null, null, null, null, observation, null);
        campaigns.clear();

        assertThat(snapshot.campaigns().value()).hasSize(1);
        assertThatThrownBy(() -> snapshot.campaigns().value().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private AdminOverviewSnapshot.OperationActionItem actionItem(
            Long couponId, Severity severity, Instant detectedAt) {
        return new AdminOverviewSnapshot.OperationActionItem(
                couponId,
                "캠페인 " + couponId,
                FROM,
                severity,
                AdminOverviewSnapshot.CustomerImpact.LIMITED,
                "일부 고객 대기",
                detectedAt,
                Duration.ofMinutes(1),
                new AdminOverviewSnapshot.RecommendedAction(
                        AdminOverviewSnapshot.ActionCode.QUEUE_STALLED,
                        "대기열 확인",
                        AdminOverviewSnapshot.TargetScreen.METRICS));
    }

    private AdminOverviewSnapshot.CampaignOverview campaign(Long couponId) {
        return new AdminOverviewSnapshot.CampaignOverview(
                1, couponId, "캠페인", "브랜드", CouponRoundStatus.OPEN, FROM, TO, Severity.NONE,
                null, null, null, AdminOverviewSnapshot.CustomerImpact.NONE, "영향 없음", null);
    }
}
