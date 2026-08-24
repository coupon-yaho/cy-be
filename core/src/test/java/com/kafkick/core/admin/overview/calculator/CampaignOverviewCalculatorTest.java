package com.kafkick.core.admin.overview.calculator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.overview.CampaignOverviewSource;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.SourceStatus;

/** 캠페인 원천값에서 관리자 운영현황 캠페인 영역을 계산하는 규칙을 검증합니다. */
class CampaignOverviewCalculatorTest {

    private static final Instant SNAPSHOT_AT = Instant.parse("2026-08-21T03:00:00Z");

    private final CampaignOverviewCalculator calculator = new CampaignOverviewCalculator();

    /** 상태 분류가 목록 순서나 오픈 시각으로 다시 추론되는 회귀를 방지합니다. */
    @Test
    @DisplayName("캠페인의 확정 상태로 OPEN·SCHEDULED·CLOSED 건수를 계산한다")
    void countsCampaignStatuses() {
        CampaignOverviewCalculator.CampaignCalculation result = calculate(
                List.of(
                        source(1L, CouponRoundStatus.OPEN, SNAPSHOT_AT, EngineVersion.V1,
                                100L, 20L, SNAPSHOT_AT, true),
                        source(2L, CouponRoundStatus.SCHEDULED, SNAPSHOT_AT.plusSeconds(10),
                                EngineVersion.V1, 100L, 0L, SNAPSHOT_AT, true),
                        source(3L, CouponRoundStatus.SCHEDULED, SNAPSHOT_AT.plusSeconds(20),
                                EngineVersion.V1, null, null, null, false),
                        source(4L, CouponRoundStatus.CLOSED, SNAPSHOT_AT.minusSeconds(10),
                                EngineVersion.V1, 100L, 100L, SNAPSHOT_AT, true)
                )
        );

        assertThat(result.campaignStatusSummary())
                .isEqualTo(new AdminOverviewSnapshot.CampaignStatusSummary(1, 2, 1));
    }

    /** 현재 이후부터 정확히 30분까지라는 오픈 임박 시간 경계를 고정합니다. */
    @Test
    @DisplayName("오픈 임박은 현재보다 늦고 30분 이하인 예약 캠페인만 포함한다")
    void calculatesOpeningSoonAtThirtyMinuteBoundary() {
        CampaignOverviewCalculator.CampaignCalculation result = calculate(
                List.of(
                        source(1L, CouponRoundStatus.SCHEDULED, SNAPSHOT_AT,
                                EngineVersion.V1, null, null, null, false),
                        source(2L, CouponRoundStatus.SCHEDULED, SNAPSHOT_AT.plusSeconds(1),
                                EngineVersion.V1, null, null, null, false),
                        source(3L, CouponRoundStatus.SCHEDULED, SNAPSHOT_AT.plus(Duration.ofMinutes(30)),
                                EngineVersion.V1, null, null, null, true),
                        source(4L, CouponRoundStatus.SCHEDULED,
                                SNAPSHOT_AT.plus(Duration.ofMinutes(30)).plusSeconds(1),
                                EngineVersion.V1, null, null, null, false),
                        source(5L, CouponRoundStatus.OPEN, SNAPSHOT_AT.plus(Duration.ofMinutes(10)),
                                EngineVersion.V1, 100L, 10L, SNAPSHOT_AT, false)
                )
        );

        assertThat(result.openingSoon())
                .isEqualTo(new AdminOverviewSnapshot.OpeningSoonSummary(2, 1));
    }

    /** O4는 Campaign Calculator 내부 V1 수량 계산이 아니라 전달받은 O4 결과만 조립하는지 검증합니다. */
    @Test
    @DisplayName("O4 Map이 없으면 V1 수량이 있어도 재고를 UNAVAILABLE로 유지한다")
    void keepsStockUnavailableWithoutCalculatedO4Map() {
        CampaignOverviewCalculator.CampaignCalculation result = calculate(
                List.of(source(1L, CouponRoundStatus.OPEN, SNAPSHOT_AT, EngineVersion.V1,
                        1_000L, 700L, SNAPSHOT_AT.minusSeconds(2), true))
        );

        AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.StockForecast> observation =
                result.campaigns().getFirst().stockForecast();

        assertThat(observation.status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(observation.value()).isNull();
    }

    /** 잘못된 V1 재고는 O4 계산 전 원천 경계에서 거부해 0 재고로 보정하지 않습니다. */
    @Test
    @DisplayName("불완전하거나 유효하지 않은 V1 재고는 Source 경계에서 거부한다")
    void rejectsInvalidV1StockAtSourceBoundary() {
        assertThatThrownBy(() -> source(1L, CouponRoundStatus.OPEN, SNAPSHOT_AT, EngineVersion.V1,
                100L, null, SNAPSHOT_AT, true)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> source(2L, CouponRoundStatus.OPEN, SNAPSHOT_AT, EngineVersion.V1,
                100L, -1L, SNAPSHOT_AT, true)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> source(3L, CouponRoundStatus.OPEN, SNAPSHOT_AT, EngineVersion.V1,
                100L, 101L, SNAPSHOT_AT, true)).isInstanceOf(IllegalArgumentException.class);
    }

    /** Redis 재고가 필요한 엔진을 V1 DB 수량으로 대신 계산하는 회귀를 방지합니다. */
    @Test
    @DisplayName("Redis 원천이 없는 V2·V3 캠페인 재고는 UNAVAILABLE로 유지한다")
    void keepsRedisEngineStockUnavailable() {
        CampaignOverviewCalculator.CampaignCalculation result = calculate(
                List.of(
                        source(2L, CouponRoundStatus.OPEN, SNAPSHOT_AT, EngineVersion.V2,
                                100L, 20L, SNAPSHOT_AT, true),
                        source(3L, CouponRoundStatus.OPEN, SNAPSHOT_AT, EngineVersion.V3,
                                100L, 20L, SNAPSHOT_AT, true)
                )
        );

        assertThat(result.campaigns())
                .extracting(campaign -> campaign.stockForecast().status())
                .containsOnly(SourceStatus.UNAVAILABLE);
    }

    /** 입력 위치가 아니라 운영 조치 필요 여부가 화면 확인 순서를 결정하는지 검증합니다. */
    @Test
    @DisplayName("준비 미완료 WARN 캠페인은 정상 캠페인보다 먼저 노출한다")
    void prioritizesWarningCampaignBeforeNormalCampaign() {
        AdminOverviewSnapshot.OperationActionItem warning = new AdminOverviewSnapshot.OperationActionItem(
                2L, "캠페인 2", SNAPSHOT_AT.plusSeconds(10),
                com.kafkick.core.observation.Severity.WARN, AdminOverviewSnapshot.CustomerImpact.NONE,
                "준비 미완료", SNAPSHOT_AT, null, new AdminOverviewSnapshot.RecommendedAction(
                        AdminOverviewSnapshot.ActionCode.CAMPAIGN_NOT_READY, "준비 확인",
                        AdminOverviewSnapshot.TargetScreen.CAMPAIGN_DETAIL));
        CampaignOverviewCalculator.CampaignCalculation result = calculator.calculate(
                SNAPSHOT_AT,
                List.of(
                        source(1L, CouponRoundStatus.OPEN, SNAPSHOT_AT.minusSeconds(10),
                                EngineVersion.V1, 100L, 20L, SNAPSHOT_AT, true),
                        source(2L, CouponRoundStatus.SCHEDULED, SNAPSHOT_AT.plusSeconds(10),
                                EngineVersion.V1, 100L, 0L, SNAPSHOT_AT, false)),
                Map.of(), Map.of(), Map.of(), Map.of(2L, warning)
        );

        assertThat(result.campaigns())
                .extracting(AdminOverviewSnapshot.CampaignOverview::couponId)
                .containsExactly(2L, 1L);
        assertThat(result.campaigns())
                .extracting(AdminOverviewSnapshot.CampaignOverview::priority)
                .containsExactly(1, 2);
    }

    /** Repository 조회 순서가 달라도 같은 캠페인 우선순위를 반환하도록 결정성을 검증합니다. */
    @Test
    @DisplayName("입력 순서가 달라도 위험도·운영상태·오픈 시각·ID 기준 우선순위는 동일하다")
    void assignsPriorityIndependentlyOfInputOrder() {
        CampaignOverviewSource normalLater = source(
                3L, CouponRoundStatus.SCHEDULED, SNAPSHOT_AT.plus(Duration.ofHours(2)),
                EngineVersion.V1, 100L, 0L, SNAPSHOT_AT, true);
        CampaignOverviewSource warning = source(
                2L, CouponRoundStatus.SCHEDULED, SNAPSHOT_AT.plusSeconds(10),
                EngineVersion.V1, 100L, 0L, SNAPSHOT_AT, false);
        CampaignOverviewSource normalEarlier = source(
                1L, CouponRoundStatus.OPEN, SNAPSHOT_AT.minusSeconds(10),
                EngineVersion.V1, 100L, 20L, SNAPSHOT_AT, true);
        AdminOverviewSnapshot.OperationActionItem representative = new AdminOverviewSnapshot.OperationActionItem(
                2L, "캠페인 2", warning.opensAt(), com.kafkick.core.observation.Severity.WARN,
                AdminOverviewSnapshot.CustomerImpact.NONE, "준비 미완료", SNAPSHOT_AT, null,
                new AdminOverviewSnapshot.RecommendedAction(
                        AdminOverviewSnapshot.ActionCode.CAMPAIGN_NOT_READY, "준비 확인",
                        AdminOverviewSnapshot.TargetScreen.CAMPAIGN_DETAIL));

        CampaignOverviewCalculator.CampaignCalculation first = calculator.calculate(
                SNAPSHOT_AT, List.of(normalLater, warning, normalEarlier),
                Map.of(), Map.of(), Map.of(), Map.of(2L, representative));
        CampaignOverviewCalculator.CampaignCalculation second = calculator.calculate(
                SNAPSHOT_AT, List.of(normalEarlier, warning, normalLater),
                Map.of(), Map.of(), Map.of(), Map.of(2L, representative));

        assertThat(first.campaigns())
                .extracting(AdminOverviewSnapshot.CampaignOverview::couponId)
                .containsExactly(2L, 1L, 3L);
        assertThat(second.campaigns())
                .extracting(AdminOverviewSnapshot.CampaignOverview::couponId)
                .containsExactly(2L, 1L, 3L);
        assertThat(first.campaigns())
                .extracting(AdminOverviewSnapshot.CampaignOverview::priority)
                .containsExactly(1, 2, 3);
        assertThat(second.campaigns())
                .extracting(AdminOverviewSnapshot.CampaignOverview::priority)
                .containsExactly(1, 2, 3);
    }

    /** O1·O2·O4 Map과 상위 20개가 아닌 전체 대표 Map이 한 행과 우선순위를 함께 결정하는지 검증합니다. */
    @Test
    @DisplayName("계산 Map을 조립하고 전체 대표 조치 Map으로 행 표시와 정렬을 결정한다")
    void assemblesCalculationMapsAndRepresentativeActions() {
        AdminOverviewSnapshot.OperationActionItem representative = new AdminOverviewSnapshot.OperationActionItem(
                2L, "캠페인 2", SNAPSHOT_AT, com.kafkick.core.observation.Severity.CRITICAL,
                AdminOverviewSnapshot.CustomerImpact.WIDESPREAD, "입장 중단", SNAPSHOT_AT,
                Duration.ofMinutes(2), new AdminOverviewSnapshot.RecommendedAction(
                        AdminOverviewSnapshot.ActionCode.QUEUE_STALLED, "대기열 확인",
                        AdminOverviewSnapshot.TargetScreen.CAMPAIGN_DETAIL));
        AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow> issuance =
                new AdminOverviewSnapshot.Observation<>(new AdminOverviewSnapshot.IssuanceFlow(
                        44.0, SNAPSHOT_AT.minusSeconds(60), SNAPSHOT_AT, List.of(),
                        AdminOverviewSnapshot.IssuanceFlowState.DECREASING, Duration.ofMinutes(2)),
                        SourceStatus.VALID, SNAPSHOT_AT);
        AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.StockForecast> stock =
                new AdminOverviewSnapshot.Observation<>(new AdminOverviewSnapshot.StockForecast(
                        350L, 7_000L, 0.05, Duration.ofSeconds(478)), SourceStatus.VALID, SNAPSHOT_AT);

        CampaignOverviewCalculator.CampaignCalculation result = calculator.calculate(SNAPSHOT_AT,
                List.of(source(1L, CouponRoundStatus.OPEN, SNAPSHOT_AT, EngineVersion.V1,
                                100L, 20L, SNAPSHOT_AT, true),
                        source(2L, CouponRoundStatus.OPEN, SNAPSHOT_AT, EngineVersion.V1,
                                7_000L, 6_650L, SNAPSHOT_AT, true)),
                Map.of(2L, issuance),
                Map.of(2L, new AdminOverviewSnapshot.Observation<>(null, SourceStatus.N_A, null)),
                Map.of(2L, stock), Map.of(2L, representative));

        assertThat(result.campaigns()).extracting(AdminOverviewSnapshot.CampaignOverview::couponId)
                .containsExactly(2L, 1L);
        assertThat(result.campaigns().getFirst()).satisfies(campaign -> {
            assertThat(campaign.issuanceFlow()).isSameAs(issuance);
            assertThat(campaign.campaignQueueStatus().status()).isEqualTo(SourceStatus.N_A);
            assertThat(campaign.stockForecast()).isSameAs(stock);
            assertThat(campaign.severity()).isEqualTo(com.kafkick.core.observation.Severity.CRITICAL);
            assertThat(campaign.customerImpact()).isEqualTo(AdminOverviewSnapshot.CustomerImpact.WIDESPREAD);
            assertThat(campaign.recommendedAction()).isEqualTo(representative.recommendedAction());
        });
        assertThat(result.campaigns().get(1).issuanceFlow().status()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    private static CampaignOverviewSource source(
            long couponId,
            CouponRoundStatus status,
            Instant opensAt,
            EngineVersion engineVersion,
            Long totalQuantity,
            Long activeCount,
            Instant stockObservedAt,
            boolean preparationCompleted
    ) {
        return new CampaignOverviewSource(
                couponId,
                "캠페인 " + couponId,
                "브랜드",
                status,
                opensAt,
                opensAt.plus(Duration.ofHours(1)),
                engineVersion,
                totalQuantity,
                activeCount,
                stockObservedAt,
                stockObservedAt == null ? SourceStatus.UNAVAILABLE : SourceStatus.VALID,
                preparationCompleted
        );
    }

    /** 조립 Map이 필요 없는 계산 계약도 공개 6인자 경계를 통해 검증합니다. */
    private CampaignOverviewCalculator.CampaignCalculation calculate(List<CampaignOverviewSource> campaigns) {
        return calculator.calculate(SNAPSHOT_AT, campaigns, Map.of(), Map.of(), Map.of(), Map.of());
    }
}
