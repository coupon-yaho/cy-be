package com.kafkick.core.coupon.port;

import java.time.Instant;

import com.kafkick.core.coupon.domain.CouponStockOccupationResult;
import com.kafkick.core.coupon.exception.CouponPersistenceException;
import com.kafkick.core.coupon.exception.CouponStockOverflowException;

public interface CouponStockRepository {

    /**
     * 재고 한 장을 조건부로 점유한다. 총량을 넘기지 않는 것은 이 UPDATE 의 {@code WHERE} 절이고,
     * 판정은 affected rows 다 — 애플리케이션이 읽고 비교하지 않는다.
     *
     * @param couponRoundId 회차
     * @param updatedAt 갱신 시각. <b>{@code null} 이면 DB 에 가기 전에 거절한다</b> —
     *     그대로 보내면 {@code NOT NULL} 위반이 되어 호출부 버그가 재고 사고로 오분류된다
     * @return 점유했으면 {@code OCCUPIED}, 총량에 도달했으면 {@code SOLD_OUT},
     *     회차 재고 행이 없으면 {@code NOT_FOUND}
     * @throws IllegalArgumentException {@code updatedAt} 이 {@code null} 일 때
     */
    CouponStockOccupationResult occupyOne(
            Long couponRoundId,
            Instant updatedAt
    );

    /**
     * 레거시 v1 경로가 같은 DB 트랜잭션에서 파생 활성 수를 올린다.
     *
     * <p><b>거절하지 않고 중단한다.</b> 이 레거시 경로에는 매진이라는
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
