// 발급건의 상태를 바꾸는 사건입니다. issuance_histories.event_type 과 같은 어휘를 씁니다.
package com.kafkick.core.coupon;

/**
 * CANCEL_USE 는 USED 에서 ISSUED 로 되돌리는 역방향 전이입니다(주문 취소).
 * 역방향이 있어야 재고가 양방향으로 움직이고, "사용취소 중복 요청 → 재고 이중 복원" 같은
 * 실제 버그가 성립합니다. 오염 유형 3 이 겨냥하는 것이 정확히 이 결함 클래스입니다.
 */
public enum IssuanceEventType {

    ISSUE,
    USE,
    CANCEL_USE,
    CANCEL,
    EXPIRE
}
