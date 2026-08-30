package com.kafkick.core.admin.couponroundsource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.RecordComponent;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.CouponPolicyType;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSource;
import com.kafkick.core.admin.stock.AdminStockSnapshot;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.SourceStatus;

/** 관리자 쿠폰 회차 DB 조회 경계의 상태·값·시각 계약을 검증합니다. */
class AdminCouponRoundDataContractTest {

    private static final Instant SNAPSHOT_AT = Instant.parse("2026-08-24T03:00:00Z");

    /** 빈 VALID 카탈로그가 실제로 조회된 빈 모집단과 오류 상태를 구별하는지 검증합니다. */
    @Test
    @DisplayName("VALID 카탈로그는 빈 쿠폰 회차 목록을 허용한다")
    void allowsEmptyValidCatalog() {
        AdminCouponRoundCatalog catalog = new AdminCouponRoundCatalog(SourceStatus.VALID, SNAPSHOT_AT, List.of());

        assertThat(catalog.couponRounds()).isEmpty();
        assertThat(catalog.observedAt()).isEqualTo(SNAPSHOT_AT);
    }

    /** 카탈로그가 값 있는 목록을 불변 보존하고 쿠폰 회차별 재고 상태를 독립적으로 유지하는지 검증합니다. */
    @Test
    @DisplayName("VALID 카탈로그는 재고 UNAVAILABLE 쿠폰 회차를 목록에 보존한다")
    void preservesCouponRoundWhenItsStockIsUnavailable() {
        ArrayList<AdminCouponRoundCatalog.CouponRoundData> mutableCouponRounds = new ArrayList<>();
        mutableCouponRounds.add(couponRound(11L, unavailableStock()));

        AdminCouponRoundCatalog catalog = new AdminCouponRoundCatalog(
                SourceStatus.VALID, SNAPSHOT_AT, mutableCouponRounds);
        mutableCouponRounds.clear();

        assertThat(catalog.couponRounds()).singleElement().satisfies(data -> {
            assertThat(data.couponId()).isEqualTo(11L);
            assertThat(data.engineVersion()).isEqualTo(EngineVersion.V2);
            assertThat(data.stock().status()).isEqualTo(SourceStatus.UNAVAILABLE);
            assertThat(data.stock().value()).isNull();
        });
        assertThatThrownBy(() -> catalog.couponRounds().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /** PENDING과 UNAVAILABLE은 목록을 받은 것처럼 보이지 않도록 빈 목록·null 시각만 허용하는지 검증합니다. */
    @Test
    @DisplayName("값 없는 카탈로그 상태는 관측 시각과 쿠폰 회차 목록을 가질 수 없다")
    void rejectsValuesForUnavailableOrPendingCatalog() {
        AdminCouponRoundCatalog unavailable = new AdminCouponRoundCatalog(SourceStatus.UNAVAILABLE, null, List.of());
        AdminCouponRoundCatalog pending = new AdminCouponRoundCatalog(SourceStatus.PENDING, null, List.of());

        assertThat(unavailable.couponRounds()).isEmpty();
        assertThat(pending.couponRounds()).isEmpty();
        assertThatThrownBy(() -> new AdminCouponRoundCatalog(
                SourceStatus.UNAVAILABLE, SNAPSHOT_AT, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminCouponRoundCatalog(
                SourceStatus.PENDING, null, List.of(couponRound(12L, unavailableStock()))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 상세 조회가 찾음·없음·조회 불가의 서로 다른 결과를 구분하는지 검증합니다. */
    @Test
    @DisplayName("상세 조회는 AVAILABLE NOT_FOUND UNAVAILABLE을 구분한다")
    void distinguishesDetailAvailability() {
        AdminCouponRoundDetailData available = new AdminCouponRoundDetailData(
                DetailAvailability.AVAILABLE, detailValue(21L));
        AdminCouponRoundDetailData notFound = new AdminCouponRoundDetailData(DetailAvailability.NOT_FOUND, null);
        AdminCouponRoundDetailData unavailable = new AdminCouponRoundDetailData(DetailAvailability.UNAVAILABLE, null);

        assertThat(available.value().couponId()).isEqualTo(21L);
        assertThat(available.value().engineVersion()).isEqualTo(EngineVersion.V2);
        assertThat(notFound.value()).isNull();
        assertThat(unavailable.value()).isNull();
        assertThatThrownBy(() -> new AdminCouponRoundDetailData(DetailAvailability.AVAILABLE, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminCouponRoundDetailData(DetailAvailability.NOT_FOUND, detailValue(22L)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** DB 조회 장애가 존재하지 않는 쿠폰 회차와 같은 404로 축약되지 않는지 검증합니다. */
    @Test
    @DisplayName("DB 관측 불가는 ADMIN-COUPON-ROUND-001 503 오류 계약을 사용한다")
    void exposesUnavailableObservationAsServiceUnavailable() {
        AdminCouponRoundDataErrorCode error = AdminCouponRoundDataErrorCode.OBSERVATION_UNAVAILABLE;

        assertThat(error.getStatus()).isEqualTo(503);
        assertThat(error.getCode()).isEqualTo("ADMIN-COUPON-ROUND-001");
        assertThat(error.getMessage()).isEqualTo("쿠폰 회차 관측 데이터를 조회할 수 없습니다.");
    }

    /** A-F6 전 DB 조회 경계가 FINAL 상태나 조치 후보를 새로 노출하지 않는지 검증합니다. */
    @Test
    @DisplayName("DB 쿠폰 회차 조회 계약은 FINAL 상태와 조치 후보를 표현하지 않는다")
    void doesNotRepresentFinalOrSynthesizeActionCandidates() {
        assertThat(AdminCouponRoundDataReader.class.getDeclaredMethods())
                .extracting(method -> method.getName())
                .containsExactlyInAnyOrder("loadCatalog", "findDetail");
        assertThat(AdminCouponRoundCatalog.CouponRoundData.class.getRecordComponents())
                .extracting(RecordComponent::getName)
                .doesNotContain("final", "finalStatus", "actionCandidate");
        assertThat(AdminCouponRoundDetailData.DetailValue.class.getRecordComponents())
                .extracting(RecordComponent::getName)
                .doesNotContain("final", "finalStatus", "actionCandidate");
    }

    /** 권위 재고가 계산기에 전달할 수 없는 범위의 수량을 생성 단계에서 거부하는지 검증합니다. */
    @Test
    @DisplayName("권위 재고는 양수 전체 수량과 0 이상 전체 이하 잔여 수량만 허용한다")
    void authoritativeStockRejectsInvalidQuantityRange() {
        assertThat(new AdminStockSnapshot(100L, 25L).issuedQuantity()).isEqualTo(75L);
        assertThatThrownBy(() -> new AdminStockSnapshot(0L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminStockSnapshot(100L, -1L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminStockSnapshot(100L, 101L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** Redis meta 비교에 사용할 DB 등급 마스크가 준비 원천에서 손실되지 않는지 검증합니다. */
    @Test
    @DisplayName("VALID DB 준비 원천은 유효한 회차 등급 마스크를 보존한다")
    void preparationSourceCarriesEligibleGradesMask() {
        PreparationSource source = new PreparationSource(
                true, true, CouponPolicyType.FIXED_AMOUNT, 3, SourceStatus.VALID, SNAPSHOT_AT);

        assertThat(source.eligibleGradesMask()).isEqualTo(3);
    }

    /** 설정 완료 원천이 비어 있거나 지원하지 않는 등급 마스크를 정상값으로 싣는 것을 막습니다. */
    @Test
    @DisplayName("설정 완료 DB 준비 원천은 발급 도메인이 인정하는 등급 마스크가 필요하다")
    void configuredPreparationSourceRejectsInvalidGradeMask() {
        assertThatThrownBy(() -> new PreparationSource(
                true, true, CouponPolicyType.FIXED_AMOUNT, null, SourceStatus.VALID, SNAPSHOT_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PreparationSource(
                true, true, CouponPolicyType.FIXED_AMOUNT, 0, SourceStatus.VALID, SNAPSHOT_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PreparationSource(
                true, true, CouponPolicyType.FIXED_AMOUNT, 16, SourceStatus.VALID, SNAPSHOT_AT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 값 없는 DB 원천이 이전 등급 값을 남겨 확정값처럼 보이는 조합을 거부하는지 검증합니다. */
    @Test
    @DisplayName("값 없는 DB 준비 원천은 등급 마스크를 가질 수 없다")
    void valueLessPreparationSourceRejectsGradeMask() {
        assertThatThrownBy(() -> new PreparationSource(
                null, null, null, 3, SourceStatus.PENDING, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static AdminCouponRoundCatalog.CouponRoundData couponRound(
            long couponId,
            CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> stock
    ) {
        return new AdminCouponRoundCatalog.CouponRoundData(
                couponId, "쿠폰 회차 " + couponId, "브랜드", EngineVersion.V2,
                CouponRoundStatus.SCHEDULED,
                SNAPSHOT_AT.plusSeconds(60), SNAPSHOT_AT.plusSeconds(120), stock,
                new PreparationSource(null, null, null, null, SourceStatus.PENDING, null));
    }

    private static AdminCouponRoundDetailData.DetailValue detailValue(long couponId) {
        return new AdminCouponRoundDetailData.DetailValue(
                couponId,
                "쿠폰 회차 " + couponId,
                "브랜드",
                EngineVersion.V2,
                new CouponMetricsSource.CouponRoundRuntime(CouponRoundStatus.OPEN, SNAPSHOT_AT.minusSeconds(60)),
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
