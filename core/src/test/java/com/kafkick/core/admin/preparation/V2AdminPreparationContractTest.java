package com.kafkick.core.admin.preparation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.SourceStatus;

/** 관리자 V2 준비 관측 원천과 Redis 조회 요청의 생성 계약을 검증합니다. */
class V2AdminPreparationContractTest {

    private static final Instant OPENS_AT = Instant.parse("2026-08-29T03:00:00Z");
    private static final Instant CLOSES_AT = OPENS_AT.plusSeconds(3_600L);
    private static final Instant OBSERVED_AT = Instant.parse("2026-08-29T02:55:00Z");

    /** Redis에 접속해 확정한 두 준비 판정과 관측 시각이 손실되지 않는지 검증합니다. */
    @Test
    @DisplayName("VALID V2 준비 원천은 워밍업·게이트 판정과 관측 시각을 보존한다")
    void validSourceCarriesBothReadinessValues() {
        V2PreparationSource source = new V2PreparationSource(
                true, false, SourceStatus.VALID, OBSERVED_AT);

        assertThat(source.warmupReady()).isTrue();
        assertThat(source.gateReady()).isFalse();
        assertThat(source.status()).isEqualTo(SourceStatus.VALID);
        assertThat(source.observedAt()).isEqualTo(OBSERVED_AT);
    }

    /** 미실행·통신 실패·해당 없음이 확정 판정처럼 보이는 값 조합을 거부하는지 검증합니다. */
    @Test
    @DisplayName("값 없는 V2 준비 상태는 판정과 관측 시각을 가질 수 없다")
    void valueLessSourceRejectsReadinessValues() {
        assertThat(new V2PreparationSource(null, null, SourceStatus.PENDING, null).status())
                .isEqualTo(SourceStatus.PENDING);
        assertThat(new V2PreparationSource(null, null, SourceStatus.UNAVAILABLE, null).status())
                .isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(new V2PreparationSource(null, null, SourceStatus.N_A, null).status())
                .isEqualTo(SourceStatus.N_A);
        assertThatThrownBy(() -> new V2PreparationSource(
                true, null, SourceStatus.PENDING, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new V2PreparationSource(
                null, null, SourceStatus.UNAVAILABLE, OBSERVED_AT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 값 보유 상태가 null 판정이나 관측 시각 없이 생성되는 회귀를 방지합니다. */
    @Test
    @DisplayName("VALID V2 준비 상태는 두 판정과 관측 시각이 모두 필요하다")
    void validSourceRequiresBothValuesAndObservedAt() {
        assertThatThrownBy(() -> new V2PreparationSource(
                null, true, SourceStatus.VALID, OBSERVED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new V2PreparationSource(
                true, true, SourceStatus.VALID, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** Redis 준비 Reader가 DB에서 확정된 예약 회차 비교값을 그대로 받는지 검증합니다. */
    @Test
    @DisplayName("V2 준비 요청은 예약 회차의 DB 비교값을 보존한다")
    void requestCarriesScheduledRoundExpectedValues() {
        V2AdminPreparationReader.Request request = new V2AdminPreparationReader.Request(
                10L, CouponRoundStatus.SCHEDULED, OPENS_AT, CLOSES_AT, 3, 100L, 75L);

        assertThat(request.couponId()).isEqualTo(10L);
        assertThat(request.campaignStatus()).isEqualTo(CouponRoundStatus.SCHEDULED);
        assertThat(request.opensAt()).isEqualTo(OPENS_AT);
        assertThat(request.closesAt()).isEqualTo(CLOSES_AT);
        assertThat(request.expectedGradeMask()).isEqualTo(3);
        assertThat(request.expectedTotalQuantity()).isEqualTo(100L);
        assertThat(request.expectedRemainingQuantity()).isEqualTo(75L);
    }

    /** 비예약 회차와 발급 도메인이 복원할 수 없는 DB 비교값을 조회 전에 거부하는지 검증합니다. */
    @Test
    @DisplayName("V2 준비 요청은 비예약 회차와 잘못된 기간·등급·수량을 거부한다")
    void requestRejectsNonScheduledOrInvalidExpectedValues() {
        assertThatThrownBy(() -> new V2AdminPreparationReader.Request(
                0L, CouponRoundStatus.SCHEDULED, OPENS_AT, CLOSES_AT, 3, 100L, 100L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> request(CouponRoundStatus.OPEN, OPENS_AT, CLOSES_AT, 3, 100L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> request(
                CouponRoundStatus.SCHEDULED, OPENS_AT, OPENS_AT, 3, 100L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> request(
                CouponRoundStatus.SCHEDULED, OPENS_AT, OPENS_AT.plusSeconds(86_401L), 3, 100L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> request(
                CouponRoundStatus.SCHEDULED, OPENS_AT, CLOSES_AT, 0, 100L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> request(
                CouponRoundStatus.SCHEDULED, OPENS_AT, CLOSES_AT, 16, 100L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> request(
                CouponRoundStatus.SCHEDULED, OPENS_AT, CLOSES_AT, 3, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new V2AdminPreparationReader.Request(
                10L, CouponRoundStatus.SCHEDULED, OPENS_AT, CLOSES_AT, 3, 100L, -1L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new V2AdminPreparationReader.Request(
                10L, CouponRoundStatus.SCHEDULED, OPENS_AT, CLOSES_AT, 3, 100L, 101L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 개별 거절 조건에서 반복되는 정상 필드를 한곳에 고정합니다. */
    private static V2AdminPreparationReader.Request request(
            CouponRoundStatus status,
            Instant opensAt,
            Instant closesAt,
            int gradeMask,
            long totalQuantity
    ) {
        return new V2AdminPreparationReader.Request(
                10L, status, opensAt, closesAt, gradeMask, totalQuantity, totalQuantity);
    }
}
