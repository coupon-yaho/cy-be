package com.kafkick.core.admin.overview.calculator;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.overview.CampaignOverviewSource;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.coupon.CouponStatus;
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
        CampaignOverviewCalculator.CampaignCalculation result = calculator.calculate(
                SNAPSHOT_AT,
                List.of(
                        source(1L, CouponStatus.OPEN, SNAPSHOT_AT, EngineVersion.V1,
                                100L, 20L, SNAPSHOT_AT, true),
                        source(2L, CouponStatus.SCHEDULED, SNAPSHOT_AT.plusSeconds(10),
                                EngineVersion.V1, 100L, 0L, SNAPSHOT_AT, true),
                        source(3L, CouponStatus.SCHEDULED, SNAPSHOT_AT.plusSeconds(20),
                                EngineVersion.V1, null, null, null, false),
                        source(4L, CouponStatus.CLOSED, SNAPSHOT_AT.minusSeconds(10),
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
        CampaignOverviewCalculator.CampaignCalculation result = calculator.calculate(
                SNAPSHOT_AT,
                List.of(
                        source(1L, CouponStatus.SCHEDULED, SNAPSHOT_AT,
                                EngineVersion.V1, null, null, null, false),
                        source(2L, CouponStatus.SCHEDULED, SNAPSHOT_AT.plusSeconds(1),
                                EngineVersion.V1, null, null, null, false),
                        source(3L, CouponStatus.SCHEDULED, SNAPSHOT_AT.plus(Duration.ofMinutes(30)),
                                EngineVersion.V1, null, null, null, true),
                        source(4L, CouponStatus.SCHEDULED,
                                SNAPSHOT_AT.plus(Duration.ofMinutes(30)).plusSeconds(1),
                                EngineVersion.V1, null, null, null, false),
                        source(5L, CouponStatus.OPEN, SNAPSHOT_AT.plus(Duration.ofMinutes(10)),
                                EngineVersion.V1, 100L, 10L, SNAPSHOT_AT, false)
                )
        );

        assertThat(result.openingSoon())
                .isEqualTo(new AdminOverviewSnapshot.OpeningSoonSummary(2, 1));
    }

    /** V1 DB 재고에서 잔여 수량과 0~1 비율을 올바르게 계산하는지 검증합니다. */
    @Test
    @DisplayName("V1 캠페인은 전체 수량과 활성 수량으로 잔여 재고를 계산한다")
    void calculatesV1Stock() {
        CampaignOverviewCalculator.CampaignCalculation result = calculator.calculate(
                SNAPSHOT_AT,
                List.of(source(1L, CouponStatus.OPEN, SNAPSHOT_AT, EngineVersion.V1,
                        1_000L, 700L, SNAPSHOT_AT.minusSeconds(2), true))
        );

        AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.StockForecast> observation =
                result.campaigns().getFirst().stockForecast();

        assertThat(observation.status()).isEqualTo(SourceStatus.VALID);
        assertThat(observation.observedAt()).isEqualTo(SNAPSHOT_AT.minusSeconds(2));
        assertThat(observation.value()).isEqualTo(
                new AdminOverviewSnapshot.StockForecast(300L, 1_000L, 0.3, null));
    }

    /** 누락·역전된 재고를 실제 0 재고로 보정해 노출하는 회귀를 방지합니다. */
    @Test
    @DisplayName("불완전하거나 유효하지 않은 V1 재고는 UNAVAILABLE로 유지한다")
    void keepsInvalidV1StockUnavailable() {
        List<CampaignOverviewSource> sources = List.of(
                source(1L, CouponStatus.OPEN, SNAPSHOT_AT, EngineVersion.V1,
                        100L, null, SNAPSHOT_AT, true),
                source(2L, CouponStatus.OPEN, SNAPSHOT_AT, EngineVersion.V1,
                        0L, 0L, SNAPSHOT_AT, true),
                source(3L, CouponStatus.OPEN, SNAPSHOT_AT, EngineVersion.V1,
                        100L, -1L, SNAPSHOT_AT, true),
                source(4L, CouponStatus.OPEN, SNAPSHOT_AT, EngineVersion.V1,
                        100L, 101L, SNAPSHOT_AT, true)
        );

        CampaignOverviewCalculator.CampaignCalculation result =
                calculator.calculate(SNAPSHOT_AT, sources);

        assertThat(result.campaigns())
                .extracting(campaign -> campaign.stockForecast().status())
                .containsOnly(SourceStatus.UNAVAILABLE);
    }

    /** Redis 재고가 필요한 엔진을 V1 DB 수량으로 대신 계산하는 회귀를 방지합니다. */
    @Test
    @DisplayName("Redis 원천이 없는 V2·V3 캠페인 재고는 UNAVAILABLE로 유지한다")
    void keepsRedisEngineStockUnavailable() {
        CampaignOverviewCalculator.CampaignCalculation result = calculator.calculate(
                SNAPSHOT_AT,
                List.of(
                        source(2L, CouponStatus.OPEN, SNAPSHOT_AT, EngineVersion.V2,
                                100L, 20L, SNAPSHOT_AT, true),
                        source(3L, CouponStatus.OPEN, SNAPSHOT_AT, EngineVersion.V3,
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
        CampaignOverviewCalculator.CampaignCalculation result = calculator.calculate(
                SNAPSHOT_AT,
                List.of(
                        source(1L, CouponStatus.OPEN, SNAPSHOT_AT.minusSeconds(10),
                                EngineVersion.V1, 100L, 20L, SNAPSHOT_AT, true),
                        source(2L, CouponStatus.SCHEDULED, SNAPSHOT_AT.plusSeconds(10),
                                EngineVersion.V1, 100L, 0L, SNAPSHOT_AT, false)
                )
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
                3L, CouponStatus.SCHEDULED, SNAPSHOT_AT.plus(Duration.ofHours(2)),
                EngineVersion.V1, 100L, 0L, SNAPSHOT_AT, true);
        CampaignOverviewSource warning = source(
                2L, CouponStatus.SCHEDULED, SNAPSHOT_AT.plusSeconds(10),
                EngineVersion.V1, 100L, 0L, SNAPSHOT_AT, false);
        CampaignOverviewSource normalEarlier = source(
                1L, CouponStatus.OPEN, SNAPSHOT_AT.minusSeconds(10),
                EngineVersion.V1, 100L, 20L, SNAPSHOT_AT, true);

        CampaignOverviewCalculator.CampaignCalculation first = calculator.calculate(
                SNAPSHOT_AT, List.of(normalLater, warning, normalEarlier));
        CampaignOverviewCalculator.CampaignCalculation second = calculator.calculate(
                SNAPSHOT_AT, List.of(normalEarlier, warning, normalLater));

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

    private static CampaignOverviewSource source(
            long couponId,
            CouponStatus status,
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
                preparationCompleted
        );
    }
}
