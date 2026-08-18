package com.kafkick.core.admin.overview;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.kafkick.core.coupon.CouponStatus;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;

/** 운영 현황 Provider 계약이 Core 경계에서 안전하게 사용되는지 검증합니다. */
class AdminOverviewCoreContractTest {

    private static final Instant FROM = Instant.parse("2026-08-17T00:00:00Z");
    private static final Instant TO = Instant.parse("2026-08-17T01:00:00Z");

    /** Provider가 API 타입에 의존하지 않고 Core 조회·결과 타입만 사용하는지 검증합니다. */
    @Test
    void providerUsesCoreQueryAndSnapshotTypes() throws Exception {
        Method method = AdminOverviewProvider.class.getDeclaredMethod("getOverview", AdminOverviewQuery.class);

        assertThat(method.getReturnType()).isEqualTo(AdminOverviewSnapshot.class);
        assertThat(AdminOverviewProvider.class.getDeclaredMethods()).containsExactly(method);
    }

    /** 생성 후 원본 Set 변경이 조회 조건을 바꾸는 회귀를 방지합니다. */
    @Test
    void queryDefensivelyCopiesCouponIds() {
        Set<Long> couponIds = new HashSet<>(Set.of(11L));

        AdminOverviewQuery query = new AdminOverviewQuery(FROM, TO, couponIds);
        couponIds.add(22L);

        assertThat(query.couponIds()).containsExactly(11L);
    }

    /** Query가 반환한 쿠폰 집합을 호출자가 변경하지 못하게 합니다. */
    @Test
    void queryExposesUnmodifiableCouponIds() {
        AdminOverviewQuery query = new AdminOverviewQuery(FROM, TO, new HashSet<>(Set.of(11L)));

        assertThatThrownBy(() -> query.couponIds().add(22L))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /** 미지정 필터를 뜻하는 null 계약은 방어적 복사 후에도 유지합니다. */
    @Test
    void queryPreservesNullCouponIds() {
        assertThat(new AdminOverviewQuery(FROM, TO, null).couponIds()).isNull();
    }

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
                1, couponId, "캠페인", "브랜드", CouponStatus.OPEN, FROM, TO, Severity.NONE,
                null, null, null, AdminOverviewSnapshot.CustomerImpact.NONE, "영향 없음", null);
    }
}
