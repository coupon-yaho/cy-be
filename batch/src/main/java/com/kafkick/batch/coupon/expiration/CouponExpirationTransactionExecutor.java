// 회차별 상태 전이·재고 복원·EXPIRE 이력을 하나의 트랜잭션으로 처리합니다.
package com.kafkick.batch.coupon.expiration;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.service.CouponExpirationCommand;
import com.kafkick.core.coupon.service.CouponExpirationResult;
import com.kafkick.core.coupon.service.CouponExpirationService;

@Component
public class CouponExpirationTransactionExecutor {

    private final CouponExpirationService expirationService;

    public CouponExpirationTransactionExecutor(
            CouponExpirationService expirationService
    ) {
        this.expirationService = expirationService;
    }

    @Transactional
    public CouponExpirationResult execute(
            CouponExpirationCommand command
    ) {
        return expirationService.expire(command);
    }
}
