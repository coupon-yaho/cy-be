// 회차(coupons)의 상태입니다. 발급건 상태(IssuanceStatus)와 다릅니다.
package com.kafkick.core.coupon;

/**
 * 스케줄러가 SCHEDULED 로 만들고, open_at 에 OPEN, 재고 소진 또는 close_at 에 CLOSED 로 바꿉니다.
 * 발급 요청은 OPEN 일 때만 통과합니다.
 *
 * close_at 은 재고 소진으로 닫혀도 갱신하지 않습니다 — 갱신하면 "언제 닫힐 예정이었나"가 소실됩니다.
 * 실제 소진 시각은 마지막 ISSUE 이력에서 계산합니다.
 */
public enum CouponStatus {

    SCHEDULED,
    OPEN,
    CLOSED
}
