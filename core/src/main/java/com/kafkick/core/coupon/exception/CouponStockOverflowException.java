package com.kafkick.core.coupon.exception;

/**
 * {@code ck_coupon_stock_active_range} 위반 — 활성 수가 총재고를 넘으려 했다.
 *
 * <p>v2 에서 재고 판정은 Redis 가 한다. 그래서 이 CHECK 가 걸린다는 것은 "DB 가 매진을
 * 판정했다" 가 아니라 <b>Redis 와 DB 가 갈렸다</b>는 사고 신호다(설계 §9.6 D9).
 *
 * <p>{@link CouponPersistenceException} 을 상속해 <b>응답은 그대로</b> 둔다 — 사용자에게는
 * 재시도로 풀리는 저장 실패다.
 *
 * <p><b>아직 이 타입을 분기하는 소비자가 없다.</b> 지금 얻는 것은 예외 타입과 메시지가
 * 커넥션 끊김·락 타임아웃과 갈린다는 것뿐이고, 관제에서 이 사고를 세려면
 * {@code V2CouponIssueService} 가 이 타입을 검사해 전용 카운터를 올려야 한다. 그 소비자를
 * 만들지 않을 거면 이 타입을 되돌리고 일반 저장 실패로 두는 편이 정직하다.
 */
public class CouponStockOverflowException extends CouponPersistenceException {

    public CouponStockOverflowException(String detail, Throwable cause) {
        super(detail, cause);
    }
}
