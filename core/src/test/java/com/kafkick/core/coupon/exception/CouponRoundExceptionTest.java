// 쿠폰 회차 중복과 저장 실패 예외가 응답 규약에 맞는 오류 코드를 가지는지 검증합니다.
package com.kafkick.core.coupon.exception;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CouponRoundExceptionTest {

    @Test
    @DisplayName("회차 중복 예외는 COUPON_ROUND-201과 409 상태를 가진다")
    void mapDuplicateRoundToConflictErrorCode() {
        CouponRoundAlreadyExistsException exception =
                new CouponRoundAlreadyExistsException("중복 회차", null);

        assertThat(exception.getErrorCode())
                .isEqualTo(CouponRoundErrorCode.COUPON_ROUND_ALREADY_EXISTS);
        assertThat(exception.getErrorCode().getCode()).isEqualTo("COUPON_ROUND-201");
        assertThat(exception.getErrorCode().getStatus()).isEqualTo(409);
    }

    @Test
    @DisplayName("회차 저장 실패 예외는 COUPON_ROUND-202와 500 상태를 가진다")
    void mapRoundSaveFailureToInternalErrorCode() {
        CouponRoundPersistenceException exception =
                new CouponRoundPersistenceException("저장 실패", null);

        assertThat(exception.getErrorCode())
                .isEqualTo(CouponRoundErrorCode.COUPON_ROUND_SAVE_FAILED);
        assertThat(exception.getErrorCode().getCode()).isEqualTo("COUPON_ROUND-202");
        assertThat(exception.getErrorCode().getStatus()).isEqualTo(500);
    }
}
