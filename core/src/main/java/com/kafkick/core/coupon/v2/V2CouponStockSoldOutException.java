package com.kafkick.core.coupon.v2;

/**
 * V2 발급 트랜잭션의 마지막 DB 재고 점유가 매진으로 끝났음을 나타낸다.
 *
 * <p>예외로 올려야 앞서 저장한 발급·이력·멱등 레코드가 같은 트랜잭션에서 전부 롤백된다. 이 예외는
 * 트랜잭션 밖에서만 SOLD_OUT 응답으로 바뀐다.
 */
final class V2CouponStockSoldOutException extends RuntimeException {

    V2CouponStockSoldOutException(long couponRoundId) {
        super("V2 쿠폰 재고가 매진되었습니다. couponRoundId=" + couponRoundId);
    }
}
