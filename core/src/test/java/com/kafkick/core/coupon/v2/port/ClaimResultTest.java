package com.kafkick.core.coupon.v2.port;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ClaimResultTest {

    @Test
    @DisplayName("마지막 한 장을 가져간 0 은 정상이다")
    void zeroRemainingIsValid() {
        assertThat(ClaimResult.claimed(0).remainingStock()).isZero();
    }

    @Test
    @DisplayName("음수 잔여 재고는 성공이 될 수 없다 — 초과 발급이 성공 응답으로 나간다")
    void rejectsNegativeRemaining() {
        assertThatThrownBy(() -> ClaimResult.claimed(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("거절에는 잔여 재고가 없다 — 0 으로 채우면 매진과 구분되지 않는다")
    void rejectedHasNoStock() {
        ClaimResult rejected = ClaimResult.rejected(ClaimOutcome.CLOSED);

        assertThat(rejected.outcome()).isEqualTo(ClaimOutcome.CLOSED);
        assertThatThrownBy(rejected::remainingStock).isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("성공을 거절로 만들 수 없다")
    void claimedCannotBeRejected() {
        assertThatThrownBy(() -> ClaimResult.rejected(ClaimOutcome.CLAIMED))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
