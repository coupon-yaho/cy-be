package com.kafkick.core.coupon.port;

import java.time.Instant;

import com.kafkick.core.coupon.domain.CouponStockOccupationResult;
import com.kafkick.core.coupon.exception.CouponPersistenceException;
import com.kafkick.core.coupon.exception.CouponStockOverflowException;

public interface CouponStockRepository {

    CouponStockOccupationResult occupyOne(
            Long couponRoundId,
            Instant updatedAt
    );

    /**
     * V2가 Redis 입장 판정 뒤 같은 DB 트랜잭션에서 파생 활성 수를 올린다.
     *
     * <p><b>거절하지 않고 중단한다.</b> 재고 판정 주체는 Redis 이므로 이 메서드에는 매진이라는
     * 결과가 없다 — 성공이 아니면 전부 사고이고, 호출한 발급 트랜잭션을 통째로 롤백시킨다.
     * 반환값이 없는 이유가 그것이다.
     *
     * @throws IllegalArgumentException {@code updatedAt} 이 {@code null} 일 때. 그대로 내려보내면
     *     저장 계층이 낼 수 있는 무결성 위반이 아래 CHECK 하나가 아니게 되어 분류가 깨진다
     * @throws CouponStockOverflowException 활성 수가 총재고를 넘어
     *     {@code ck_coupon_stock_active_range} 가 걸렸을 때. <b>Redis 와 DB 가 갈린 사고</b>이지
     *     매진이 아니다
     * @throws CouponPersistenceException 회차의 재고 행이 없어 갱신이 0행일 때, 또는 그 밖의
     *     저장 실패일 때
     */
    void incrementActiveCount(
            Long couponRoundId,
            Instant updatedAt
    );

    boolean release(
            Long couponRoundId,
            int quantity,
            Instant updatedAt
    );
}
