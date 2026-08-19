// 쿠폰 만료 저장 실패가 만료 전용 오류 코드로 변환되는지 검증합니다.
package com.kafkick.core.coupon.exception;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CouponExpirationPersistenceExceptionTest {

    @Test
    @DisplayName("만료 저장 실패는 만료 전용 오류 코드를 사용한다")
    void useExpirationErrorCode() {
        RuntimeException cause = new RuntimeException("database failure");

        CouponExpirationPersistenceException exception =
                new CouponExpirationPersistenceException(
                        "쿠폰 만료 저장 실패",
                        cause
                );

        assertThat(exception.getErrorCode()).isEqualTo(
                CouponExpirationErrorCode.COUPON_EXPIRATION_SAVE_FAILED
        );
        assertThat(exception.getCause()).isSameAs(cause);
    }
}
