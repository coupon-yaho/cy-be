// 회차(coupons)의 상태입니다. 발급건 상태(IssuanceStatus)와 다릅니다.
package com.kafkick.core.coupon;

/**
 * 스케줄러가 SCHEDULED 로 만들고, open_at 에 OPEN, 재고 소진 또는 close_at 에 CLOSED 로 바꿉니다.
 * 발급 요청은 OPEN 일 때만 통과합니다.
 *
 * close_at 은 재고 소진으로 닫혀도 갱신하지 않습니다 — 갱신하면 "언제 닫힐 예정이었나"가 소실됩니다.
 * 실제 소진 시각은 마지막 ISSUE 이력에서 계산합니다.
 *
 * <p><b>아직 읽는 코드가 없습니다.</b> 이 값을 쓰는 회차 상태 전이 스케줄러가 다음 티켓이라,
 * 지금은 어휘만 서 있는 상태입니다 — 버려진 것이 아니라 자리를 잡아 둔 것입니다.
 * 그 티켓이 들어와도 안 쓰이면 그때는 지웁니다.
 *
 * <p>재고 소진으로 닫는 것은 이 열거형을 읽는 스케줄러가 아니라 <b>발급 경로가 그 자리에서</b>
 * 합니다. 스케줄러를 기다리면 재고가 0인데 OPEN 인 구간이 생깁니다.
 */
public enum CouponStatus {

    SCHEDULED,
    OPEN,
    CLOSED
}
