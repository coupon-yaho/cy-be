package com.kafkick.batch.coupon.v2;

/**
 * 워밍업 결과. 집계값을 함께 돌려주는 이유는 <b>호출부가 그 자리에서 대조</b>할 수 있게 하기
 * 위해서다 — 나중에 지표로만 확인하면 이미 부하가 돌고 있다.
 *
 * <p>{@link CouponRoundWarmupStatus#WARMED} 가 아니면 네 수치는 전부 {@code -1} 이다.
 * 0 으로 채우면 "재고 0 인 회차를 올렸다" 와 구분되지 않는다.
 */
public record CouponRoundWarmupResult(
        long couponRoundId,
        CouponRoundWarmupStatus status,
        long totalQuantity,
        long activeCount,
        long issuedEverCount,
        long remainingStock
) {

    private static final long UNKNOWN = -1L;

    public static CouponRoundWarmupResult rejected(long couponRoundId, CouponRoundWarmupStatus status) {
        return new CouponRoundWarmupResult(couponRoundId, status, UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN);
    }

    public boolean warmed() {
        return status == CouponRoundWarmupStatus.WARMED;
    }
}
