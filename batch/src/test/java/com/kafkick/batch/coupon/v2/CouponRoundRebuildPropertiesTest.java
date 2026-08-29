package com.kafkick.batch.coupon.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * drain 값의 <b>양쪽 끝</b>을 막는다. 이 값이 잘못 들어오는 두 방향의 결말이 서로 다르다 —
 * 음수·부재는 창이 열린 채 도는 것이고, 과대는 회차 하나가 그 시간만큼 통째로 503 인 것이다.
 */
class CouponRoundRebuildPropertiesTest {

    @Test
    @DisplayName("음수와 부재를 거절한다 — 기다리지 않으면 이 단계가 있으나 마나다")
    void rejectsMissingAndNegativeDrain() {
        assertThatThrownBy(() -> new CouponRoundRebuildProperties(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CouponRoundRebuildProperties(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("상한을 넘는 값을 거절한다 — 2s 를 2m 로 잘못 적으면 그 회차가 2분간 503 이다")
    void rejectsDrainAboveTheCap() {
        assertThatThrownBy(() -> new CouponRoundRebuildProperties(Duration.ofMinutes(2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(CouponRoundRebuildProperties.MAX_DRAIN.toString());

        assertThat(new CouponRoundRebuildProperties(CouponRoundRebuildProperties.MAX_DRAIN).drain())
                .isEqualTo(CouponRoundRebuildProperties.MAX_DRAIN);
    }

    @Test
    @DisplayName("0 은 받는다 — 테스트가 쓰는 값이다")
    void acceptsZero() {
        assertThat(new CouponRoundRebuildProperties(Duration.ZERO).drain()).isZero();
    }
}
