// 회차 재고 행 하나만 비관적으로 잠그고 현재 보유량을 1 증가시킵니다.
package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;

import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.port.CouponStockRepository;
import com.kafkick.core.support.exception.BusinessException;

@Repository
public class CouponStockRepositoryImpl implements CouponStockRepository {

    private final CouponStockJpaRepository couponStockJpaRepository;

    public CouponStockRepositoryImpl(
            CouponStockJpaRepository couponStockJpaRepository
    ) {
        this.couponStockJpaRepository = couponStockJpaRepository;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void occupyOne(Long couponRoundId, Instant updatedAt) {
        couponStockJpaRepository
                .findByCouponIdForUpdate(couponRoundId)
                .orElseThrow(() -> new BusinessException(
                        CouponIssueErrorCode.COUPON_STOCK_NOT_FOUND,
                        "couponRoundId=" + couponRoundId
                ));

        int affectedRows = couponStockJpaRepository.occupyOne(
                couponRoundId,
                updatedAt
        );
        if (affectedRows != 1) {
            throw new BusinessException(
                    CouponIssueErrorCode.STOCK_EXHAUSTED,
                    "couponRoundId=" + couponRoundId
            );
        }
    }
}
