// 회차(coupons)의 상태입니다. 발급건 상태(IssuanceStatus)와 다릅니다.
package com.kafkick.core.coupon;

/**
 * 스케줄러가 SCHEDULED 로 만들고, open_at 에 OPEN, 재고 소진 또는 close_at 에 CLOSED 로 바꿉니다.
 * 발급 요청은 OPEN 일 때만 통과합니다.
 *
 * close_at 은 재고 소진으로 닫혀도 갱신하지 않습니다 — 갱신하면 "언제 닫힐 예정이었나"가 소실됩니다.
 * 실제 소진 시각은 마지막 ISSUE 이력에서 계산합니다.
 *
 * <p><b>읽는 코드가 생겼습니다 — CY-446 의 {@code CouponRoundScheduler} 입니다.</b>
 * 다만 그 스케줄러가 다루는 것은 <b>시각으로 닫히는 전이 둘</b>({@code open_at} 도달 ·
 * {@code close_at} 도달)뿐입니다. 재고 소진 마감은 발급 경로가 그 자리에서 하고,
 * 선착순 쿠폰에서 마감의 주된 사유가 그것입니다.
 *
 * <p><b>SQL 리터럴이 이 열거형에서 나옵니다.</b> 전이 어댑터가
 * {@code "...status = '%s'".formatted(CouponStatus.OPEN)} 으로 문장을 만듭니다 —
 * {@code WHERE}·{@code SET} 에 들어가는 값이라 바인딩할 자리는 아니지만, 손으로 적으면
 * 열거형과 <b>코드로 이어지지 않고</b> 그 상태에서 이 열거형은 죽은 코드입니다.
 *
 * <p><b>DB 도 값 집합을 강제합니다</b> — {@code V16} 의 {@code ck_coupon_status}.
 * 없으면 회차 생성이 {@code 'PENDING'} 을 넣어도 INSERT 가 통과하고, 전이는 그 회차를
 * 영원히 안 열고 대기 게이지도 안 셉니다 — <b>알림도 없이 회차가 안 열립니다.</b>
 *
 * <p>재고 소진으로 닫는 것은 이 열거형을 읽는 스케줄러가 아니라 <b>발급 경로가 그 자리에서</b>
 * 합니다. 스케줄러를 기다리면 재고가 0인데 OPEN 인 구간이 생깁니다.
 */
public enum CouponStatus {

    SCHEDULED,
    OPEN,
    CLOSED
}
