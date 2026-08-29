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
        // updated_at 이 NOT NULL 이라, null 을 그대로 내려보내면 이 문장이 낼 수 있는
        // 무결성 위반이 CHECK 하나가 아니게 된다 — 아래 분류가 성립하려면 여기서 막아야 한다.
        if (updatedAt == null) {
            throw new IllegalArgumentException(
                    "쿠폰 활성 수 증가에는 갱신 시각이 필요합니다. couponRoundId=" + couponRoundId);
        }
        try {
            // 조건절 없는 UPDATE 라 0행은 "매진" 이 아니라 회차의 재고 행이 없다는 뜻이다.
            // 여기서 던져 발급 트랜잭션을 중단시킨다(포트 계약).
            if (couponStockJpaRepository.incrementActiveCount(couponRoundId, updatedAt) != 1) {
                throw new CouponPersistenceException(
                        "쿠폰 재고 행이 없습니다. couponRoundId=" + couponRoundId,
                        new IllegalStateException("coupon stock row missing"));
            }
        } catch (DataIntegrityViolationException overflow) {
            // 인자를 위에서 걸렀으므로 이 문장의 무결성 위반은 ck_coupon_stock_active_range
            // 하나뿐이다. Redis 가 재고 판정 주체라 이건 매진이 아니라 Redis·DB 가 갈린
            // 사고이고, 아래 일반 catch 로 뭉개면 커넥션 끊김과 같은 줄로 집계된다.
            // coupon_stocks 에 제약을 더하면 이 분류부터 다시 봐야 한다.
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
