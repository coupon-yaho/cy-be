package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;

import org.springframework.stereotype.Repository;
import org.springframework.dao.DataAccessException;

import com.kafkick.core.coupon.exception.CouponIssuePersistenceException;
import com.kafkick.core.coupon.exception.CouponStockReleasePersistenceException;
import com.kafkick.core.coupon.domain.CouponStockOccupationResult;
import com.kafkick.core.coupon.port.CouponStockRepository;

@Repository
public class CouponStockRepositoryImpl implements CouponStockRepository {

    private final CouponStockJpaRepository couponStockJpaRepository;

    public CouponStockRepositoryImpl(
            CouponStockJpaRepository couponStockJpaRepository
    ) {
        this.couponStockJpaRepository = couponStockJpaRepository;
    }

    @Override
    public CouponStockOccupationResult occupy(
            Long couponRoundId,
            Instant updatedAt
    ) {
        try {
            if (couponStockJpaRepository
                    .findByCouponIdForUpdate(couponRoundId)
                    .isEmpty()) {
                return CouponStockOccupationResult.NOT_FOUND;
            }
            int affectedRows = couponStockJpaRepository.occupyOne(
                    couponRoundId,
                    updatedAt
            );
            return affectedRows == 1
                    ? CouponStockOccupationResult.OCCUPIED
                    : CouponStockOccupationResult.SOLD_OUT;
        } catch (DataAccessException exception) {
            throw new CouponIssuePersistenceException(
                    "쿠폰 재고 점유에 실패했습니다. couponRoundId="
                            + couponRoundId,
                    exception
            );
        }
    }

    @Override
    public boolean lockForUpdate(Long couponRoundId) {
        try {
            return couponStockJpaRepository
                    .findByCouponIdForUpdate(couponRoundId)
                    .isPresent();
        } catch (DataAccessException exception) {
            throw new CouponStockReleasePersistenceException(
                    "쿠폰 재고 잠금에 실패했습니다. couponRoundId="
                            + couponRoundId,
                    exception
            );
        }
    }

    @Override
    public boolean release(
            Long couponRoundId,
            int quantity,
            Instant updatedAt
    ) {
        if (quantity <= 0) {
            throw new IllegalArgumentException(
                    "복원할 쿠폰 재고 수량은 0보다 커야 합니다."
            );
        }
        try {
            int affectedRows = couponStockJpaRepository.release(
                    couponRoundId,
                    quantity,
                    updatedAt
            );
            return affectedRows == 1;
        } catch (DataAccessException exception) {
            throw new CouponStockReleasePersistenceException(
                    "쿠폰 재고 복원에 실패했습니다. couponRoundId="
                            + couponRoundId + ", quantity=" + quantity,
                    exception
            );
        }
    }
}
