package com.kafkick.core.admin.overview;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.couponroundsource.PreparationObservation;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.SourceStatus;

/** 쿠폰 회차 재고 원천의 상태·수량·관측 시각 불변식을 검증합니다. */
class CouponRoundOverviewSourceTest {

    private static final Instant NOW = Instant.parse("2026-08-22T03:00:00Z");

    /** 값 없는 재고 상태는 수량과 관측 시각을 함께 비워야 합니다. */
    @Test
    @DisplayName("값 없는 재고 상태에 수량이나 관측 시각이 있으면 거부한다")
    void rejectsRawStockValuesForNonCarryingStatus() {
        assertThatThrownBy(() -> source(100L, 10L, 5L, NOW, SourceStatus.UNAVAILABLE))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 값 있는 재고 상태는 두 수량과 원천 관측 시각을 모두 요구합니다. */
    @Test
    @DisplayName("값 있는 재고 상태의 수량과 관측 시각이 불완전하면 거부한다")
    void rejectsIncompleteOrInconsistentCarryingStockValues() {
        assertThatThrownBy(() -> source(101L, null, 5L, NOW, SourceStatus.VALID))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> source(102L, 10L, 11L, NOW, SourceStatus.STALE))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> source(103L, 0L, 0L, NOW, SourceStatus.VALID))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static CouponRoundOverviewSource source(
            long couponId, Long totalQuantity, Long activeCount, Instant stockObservedAt, SourceStatus stockStatus
    ) {
        return new CouponRoundOverviewSource(couponId, "쿠폰 회차", "브랜드", CouponRoundStatus.OPEN,
                NOW, NOW.plusSeconds(60), EngineVersion.V1, totalQuantity, activeCount, stockObservedAt,
                stockStatus, new PreparationObservation(true, SourceStatus.VALID, NOW));
    }
}
