package com.kafkick.core.admin.campaignsource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.RecordComponent;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.couponmetrics.CouponMetricsSource;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.SourceStatus;

/** 관리자 캠페인 DB 조회 경계의 상태·값·시각 계약을 검증합니다. */
class AdminCampaignDataContractTest {

    private static final Instant SNAPSHOT_AT = Instant.parse("2026-08-24T03:00:00Z");

    /** 빈 VALID 카탈로그가 실제로 조회된 빈 모집단과 오류 상태를 구별하는지 검증합니다. */
    @Test
    @DisplayName("VALID 카탈로그는 빈 캠페인 목록을 허용한다")
    void allowsEmptyValidCatalog() {
        AdminCampaignCatalog catalog = new AdminCampaignCatalog(SourceStatus.VALID, SNAPSHOT_AT, List.of());

        assertThat(catalog.campaigns()).isEmpty();
        assertThat(catalog.observedAt()).isEqualTo(SNAPSHOT_AT);
    }

    /** 카탈로그가 값 있는 목록을 불변 보존하고 캠페인별 재고 상태를 독립적으로 유지하는지 검증합니다. */
    @Test
    @DisplayName("VALID 카탈로그는 재고 UNAVAILABLE 캠페인을 목록에 보존한다")
    void preservesCampaignWhenItsStockIsUnavailable() {
        ArrayList<AdminCampaignCatalog.CampaignData> mutableCampaigns = new ArrayList<>();
        mutableCampaigns.add(campaign(11L, unavailableStock()));

        AdminCampaignCatalog catalog = new AdminCampaignCatalog(
                SourceStatus.VALID, SNAPSHOT_AT, mutableCampaigns);
        mutableCampaigns.clear();

        assertThat(catalog.campaigns()).singleElement().satisfies(data -> {
            assertThat(data.couponId()).isEqualTo(11L);
            assertThat(data.stock().status()).isEqualTo(SourceStatus.UNAVAILABLE);
            assertThat(data.stock().value()).isNull();
        });
        assertThatThrownBy(() -> catalog.campaigns().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /** PENDING과 UNAVAILABLE은 목록을 받은 것처럼 보이지 않도록 빈 목록·null 시각만 허용하는지 검증합니다. */
    @Test
    @DisplayName("값 없는 카탈로그 상태는 관측 시각과 캠페인 목록을 가질 수 없다")
    void rejectsValuesForUnavailableOrPendingCatalog() {
        AdminCampaignCatalog unavailable = new AdminCampaignCatalog(SourceStatus.UNAVAILABLE, null, List.of());
        AdminCampaignCatalog pending = new AdminCampaignCatalog(SourceStatus.PENDING, null, List.of());

        assertThat(unavailable.campaigns()).isEmpty();
        assertThat(pending.campaigns()).isEmpty();
        assertThatThrownBy(() -> new AdminCampaignCatalog(
                SourceStatus.UNAVAILABLE, SNAPSHOT_AT, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminCampaignCatalog(
                SourceStatus.PENDING, null, List.of(campaign(12L, unavailableStock()))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 상세 조회가 찾음·없음·조회 불가의 서로 다른 결과를 구분하는지 검증합니다. */
    @Test
    @DisplayName("상세 조회는 AVAILABLE NOT_FOUND UNAVAILABLE을 구분한다")
    void distinguishesDetailAvailability() {
        AdminCampaignDetailData available = new AdminCampaignDetailData(
                DetailAvailability.AVAILABLE, detailValue(21L));
        AdminCampaignDetailData notFound = new AdminCampaignDetailData(DetailAvailability.NOT_FOUND, null);
        AdminCampaignDetailData unavailable = new AdminCampaignDetailData(DetailAvailability.UNAVAILABLE, null);

        assertThat(available.value().couponId()).isEqualTo(21L);
        assertThat(notFound.value()).isNull();
        assertThat(unavailable.value()).isNull();
        assertThatThrownBy(() -> new AdminCampaignDetailData(DetailAvailability.AVAILABLE, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminCampaignDetailData(DetailAvailability.NOT_FOUND, detailValue(22L)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** DB 조회 장애가 존재하지 않는 캠페인과 같은 404로 축약되지 않는지 검증합니다. */
    @Test
    @DisplayName("DB 관측 불가는 ADMIN-CAMPAIGN-001 503 오류 계약을 사용한다")
    void exposesUnavailableObservationAsServiceUnavailable() {
        AdminCampaignDataErrorCode error = AdminCampaignDataErrorCode.OBSERVATION_UNAVAILABLE;

        assertThat(error.getStatus()).isEqualTo(503);
        assertThat(error.getCode()).isEqualTo("ADMIN-CAMPAIGN-001");
        assertThat(error.getMessage()).isEqualTo("캠페인 관측 데이터를 조회할 수 없습니다.");
    }

    /** A-F6 전 DB 조회 경계가 FINAL 상태나 조치 후보를 새로 노출하지 않는지 검증합니다. */
    @Test
    @DisplayName("DB 캠페인 조회 계약은 FINAL 상태와 조치 후보를 표현하지 않는다")
    void doesNotRepresentFinalOrSynthesizeActionCandidates() {
        assertThat(AdminCampaignDataReader.class.getDeclaredMethods())
                .extracting(method -> method.getName())
                .containsExactlyInAnyOrder("loadCatalog", "findDetail");
        assertThat(AdminCampaignCatalog.CampaignData.class.getRecordComponents())
                .extracting(RecordComponent::getName)
                .doesNotContain("final", "finalStatus", "actionCandidate");
        assertThat(AdminCampaignDetailData.DetailValue.class.getRecordComponents())
                .extracting(RecordComponent::getName)
                .doesNotContain("final", "finalStatus", "actionCandidate");
    }

    private static AdminCampaignCatalog.CampaignData campaign(
            long couponId,
            CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> stock
    ) {
        return new AdminCampaignCatalog.CampaignData(
                couponId, "캠페인 " + couponId, "브랜드", CouponRoundStatus.SCHEDULED,
                SNAPSHOT_AT.plusSeconds(60), SNAPSHOT_AT.plusSeconds(120), stock,
                new PreparationObservation(null, SourceStatus.PENDING, null));
    }

    private static AdminCampaignDetailData.DetailValue detailValue(long couponId) {
        return new AdminCampaignDetailData.DetailValue(
                couponId,
                "캠페인 " + couponId,
                "브랜드",
                new CouponMetricsSource.CampaignRuntime(CouponRoundStatus.OPEN, SNAPSHOT_AT.minusSeconds(60)),
                observed(new CouponMetricsSource.StockCounts(100L, 40L)),
                observed(new CouponMetricsSource.IssuanceStatusCounts(40L, 10L, 3L, 2L)),
                observed(List.of(new CouponMetricsSource.TransitionBucket(
                        SNAPSHOT_AT.minusSeconds(60), SNAPSHOT_AT, 1L, 2L, 3L, 4L))));
    }

    private static CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> unavailableStock() {
        return new CouponMetricsSource.Observation<>(null, SourceStatus.UNAVAILABLE, null);
    }

    private static <T> CouponMetricsSource.Observation<T> observed(T value) {
        return new CouponMetricsSource.Observation<>(value, SourceStatus.VALID, SNAPSHOT_AT);
    }
}
