package com.kafkick.api.support;

import org.junit.jupiter.api.Test;

import com.kafkick.core.coupon.exception.CouponIssueV2ErrorCode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetryAfterExceptionTest {

    /**
     * {@code Retry-After: 0} 은 "즉시 다시 보내라" 라서 재시도 폭주와 같다. 지금 호출부는 둘 다
     * 설정을 거치고 설정이 0 이하를 막지만, 그 검증이 호출부에만 있으면 직접 만드는 자리가
     * 하나 생기는 순간 사라진다.
     */
    @Test
    void rejectsZeroAndNegativeSeconds() {
        assertThatThrownBy(() -> new RetryAfterException(CouponIssueV2ErrorCode.REPLAY_PENDING, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetryAfterException(CouponIssueV2ErrorCode.REPLAY_PENDING, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void keepsTheErrorCodeContractOfABusinessException() {
        RetryAfterException exception =
                new RetryAfterException(CouponIssueV2ErrorCode.GATE_NOT_READY, 2);

        assertThat(exception.getErrorCode()).isSameAs(CouponIssueV2ErrorCode.GATE_NOT_READY);
        assertThat(exception.retryAfterSeconds()).isEqualTo(2);
    }
}
