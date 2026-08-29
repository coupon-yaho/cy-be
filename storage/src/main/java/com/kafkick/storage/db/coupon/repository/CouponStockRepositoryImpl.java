package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;

import org.springframework.stereotype.Repository;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;

import com.kafkick.core.coupon.domain.CouponStockOccupationResult;
import com.kafkick.core.coupon.exception.CouponPersistenceException;
import com.kafkick.core.coupon.exception.CouponStockOverflowException;
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
    public CouponStockOccupationResult occupyOne(
            Long couponRoundId,
            Instant updatedAt
    ) {
        try {
            int affectedRows = couponStockJpaRepository.occupyOne(
                    couponRoundId,
                    updatedAt
            );
            if (affectedRows == 1) {
                return CouponStockOccupationResult.OCCUPIED;
            }
            return couponStockJpaRepository.existsById(couponRoundId)
                    ? CouponStockOccupationResult.SOLD_OUT
                    : CouponStockOccupationResult.NOT_FOUND;
        } catch (DataAccessException exception) {
            throw new CouponPersistenceException(
                    "쿠폰 재고 점유에 실패했습니다. couponRoundId="
                            + couponRoundId,
                    exception
            );
        }
    }

    @Override
    public void incrementActiveCount(Long couponRoundId, Instant updatedAt) {
        try {
            if (couponStockJpaRepository.incrementActiveCount(couponRoundId, updatedAt) != 1) {
                throw new CouponPersistenceException(
                        "쿠폰 재고 행이 없습니다. couponRoundId=" + couponRoundId,
                        new IllegalStateException("coupon stock row missing"));
            }
        } catch (DataIntegrityViolationException overflow) {
            // Redis 가 재고 판정 주체라, 이 CHECK 가 걸린 것은 매진이 아니라 Redis·DB 가
            // 갈린 사고다. 아래 일반 catch 로 뭉개면 커넥션 끊김과 같은 줄로 집계된다.
            throw new CouponStockOverflowException(
                    "쿠폰 활성 수가 총재고를 넘었습니다. couponRoundId=" + couponRoundId,
                    overflow);
        } catch (DataAccessException exception) {
            throw new CouponPersistenceException(
                    "쿠폰 활성 수 증가에 실패했습니다. couponRoundId=" + couponRoundId,
                    exception);
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
            throw new CouponPersistenceException(
                    "쿠폰 재고 복원에 실패했습니다. couponRoundId="
                            + couponRoundId + ", quantity=" + quantity,
                    exception
            );
        }
    }
}
