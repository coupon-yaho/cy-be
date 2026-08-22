package com.kafkick.core.coupon.exception;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CouponStockPersistenceExceptionTest {

    @Test
    void separatesLockFailureFromReleaseFailure() {
        RuntimeException cause = new RuntimeException("database failure");

        CouponStockLockPersistenceException lockException =
                new CouponStockLockPersistenceException("lock", cause);
        CouponStockReleasePersistenceException releaseException =
                new CouponStockReleasePersistenceException("release", cause);

        assertThat(lockException.getErrorCode()).isEqualTo(
                CouponUseErrorCode.COUPON_STOCK_LOCK_FAILED
        );
        assertThat(releaseException.getErrorCode()).isEqualTo(
                CouponUseErrorCode.COUPON_STOCK_RELEASE_FAILED
        );
    }
}
