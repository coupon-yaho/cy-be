package com.kafkick.core.coupon.v2;

import org.junit.jupiter.api.Test;

import com.kafkick.core.coupon.v2.port.ClaimOutcome;
import com.kafkick.core.coupon.v2.port.ClaimResult;
import com.kafkick.core.coupon.v2.port.CompensateOutcome;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class V2CouponIssueResultTest {

    @Test
    void databaseStockDivergenceIsOnlyValidForSoldOutRejection() {
        assertThatThrownBy(() -> new V2CouponIssueResult(
                ClaimResult.rejected(ClaimOutcome.DUP_PER_MEMBER), null, null, false, true, false, null))
                .isInstanceOf(IllegalArgumentException.class);

        V2CouponIssueResult result = V2CouponIssueResult.rejectedAfterDatabaseSoldOut(CompensateOutcome.REVERTED);

        assertThat(result.claimResult().outcome()).isEqualTo(ClaimOutcome.SOLD_OUT);
        assertThat(result.databaseSoldOutAfterRedisClaim()).isTrue();
        assertThat(result.databaseDuplicateAfterRedisClaim()).isFalse();
    }

    /** 회원 괴리 표시도 제 거절에만 붙는다 — 매진 거절에 붙으면 관제가 원인을 뒤바꿔 읽는다. */
    @Test
    void databaseMemberDivergenceIsOnlyValidForDuplicateRejection() {
        assertThatThrownBy(() -> new V2CouponIssueResult(
                ClaimResult.rejected(ClaimOutcome.SOLD_OUT), null, null, false, false, true, null))
                .isInstanceOf(IllegalArgumentException.class);

        V2CouponIssueResult result = V2CouponIssueResult.rejectedAfterDatabaseDuplicate(CompensateOutcome.REVERTED);

        assertThat(result.claimResult().outcome()).isEqualTo(ClaimOutcome.DUP_PER_MEMBER);
        assertThat(result.databaseDuplicateAfterRedisClaim()).isTrue();
        assertThat(result.databaseSoldOutAfterRedisClaim()).isFalse();
    }
}
