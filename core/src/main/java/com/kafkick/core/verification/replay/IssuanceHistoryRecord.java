// 리플레이 입력 한 행입니다. issuance_histories 한 행을 그대로 옮깁니다.
package com.kafkick.core.verification.replay;

import java.time.LocalDateTime;

import com.kafkick.core.coupon.IssuanceEventType;
import com.kafkick.core.coupon.IssuanceStatus;

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
        LocalDateTime createdAt
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
