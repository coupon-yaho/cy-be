// 리플레이 입력 한 행입니다. issuance_histories 한 행을 그대로 옮깁니다.
package com.kafkick.core.verification.replay;

import java.time.LocalDateTime;

import com.kafkick.core.coupon.domain.IssuanceEventType;
import com.kafkick.core.coupon.domain.IssuanceStatus;

/**
 * {@code fromStatus} 만 null 을 허용합니다. 발급 이전에는 상태가 없기 때문입니다.
 *
 * <p>리플레이는 이 값을 상태 추적에 쓰지 않고 <b>대조에만</b> 씁니다.
 * 오염 주입이 from_status 를 위조하면 이력만 봐서는 합법으로 보이므로,
 * 추적 상태와 다른지를 따로 확인해야 합니다.
 */
public record IssuanceHistoryRecord(
        long id,
        long issuanceId,
        IssuanceEventType eventType,
        IssuanceStatus fromStatus,
        IssuanceStatus toStatus,
        LocalDateTime createdAt,

        /**
         * <b>이 이력이 붙은 발급건의 만료 시각.</b> 행마다 같은 값이 실려 낭비처럼 보이지만,
         * 리플레이가 발급건 하나를 한 번에 접으므로 그룹당 한 번만 읽는 것과 같다.
         *
         * <p><b>왜 필요한가</b> — {@code CANCEL_USE} 는 결과가 둘이다. 만료 시각을 넘긴 뒤의
         * 사용 취소는 {@code ISSUED} 가 아니라 {@code EXPIRED} 로 간다
         * ({@code CouponStateMachine.cancelUse}). 그 둘을 <b>구분 없이</b> 합법으로 받으면
         * 런타임이 틀린 쪽을 써도 V4 가 못 잡는다 — 재고까지 함께 어긋나야 V1 이 잡는데,
         * 서비스가 상태와 재고를 같이 바꾸므로 그쪽도 침묵한다.
         *
         * <p>⚠️ <b>이것은 "전이표가 시각을 받는다" 가 아니다.</b> {@code expires_at} 도
         * {@code created_at} 도 <b>저장된 값</b>이라, 같은 이력을 몇 번 접어도 같은 답이 나온다.
         * {@code docs/01} 함정 3 이 금지한 것은 <b>현재 시각</b>으로 갈래를 나누는 것이다.
         */
        LocalDateTime expiresAt
) {

    public IssuanceHistoryRecord {
        if (eventType == null) {
            throw new IllegalArgumentException("이력의 사건 종류가 필요합니다.");
        }
        if (toStatus == null) {
            throw new IllegalArgumentException("이력의 전이 결과 상태가 필요합니다.");
        }
        if (createdAt == null) {
            throw new IllegalArgumentException("이력의 생성 시각이 필요합니다.");
        }
    }
}
