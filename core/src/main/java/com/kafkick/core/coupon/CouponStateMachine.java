// 발급건의 합법 상태 전이를 판정합니다. 런타임과 검증 배치가 이 클래스를 함께 씁니다.
package com.kafkick.core.coupon;

import java.util.Map;
import java.util.Optional;

/**
 * 두 벌로 갈라지면 같은 버그를 양쪽이 재현해 검증이 검증이 아니게 됩니다.
 * 그래서 상태를 바꾸는 쪽(런타임)과 대조하는 쪽(검증 배치)이 이 한 클래스를 공유합니다.
 *
 * <p>합법 전이는 다섯 가지뿐입니다.
 *
 * <pre>
 * ISSUE       (없음)  → ISSUED
 * USE         ISSUED  → USED
 * CANCEL_USE  USED    → ISSUED     역방향 허용. 주문 취소
 * CANCEL      ISSUED  → CANCELLED  종단
 * EXPIRE      ISSUED  → EXPIRED    종단
 * </pre>
 *
 * <p>{@code USED → EXPIRED} 는 불가입니다. 이미 쓴 쿠폰이 만료되지는 않습니다.
 * 오염 유형 4 가 종단 상태에서 USED 로 되돌리는 것을 심으므로 전이표가 이를 거부해야 합니다.
 *
 * <p><b>리플레이는 이력의 from_status 를 믿지 않습니다.</b> 자기가 추적한 상태를 진실로 보고,
 * from_status 가 추적 상태와 다르면 그것 자체가 finding 입니다. 오염 주입이 from_status 를
 * 위조하면 이력만 봐서는 합법으로 보이기 때문입니다.
 */
public final class CouponStateMachine {

    /** 전이표. key 의 from 이 null 이면 발급 이전(상태 없음)을 뜻합니다. */
    private static final Map<Transition, IssuanceStatus> TABLE = Map.of(
            new Transition(null, IssuanceEventType.ISSUE), IssuanceStatus.ISSUED,
            new Transition(IssuanceStatus.ISSUED, IssuanceEventType.USE), IssuanceStatus.USED,
            new Transition(IssuanceStatus.USED, IssuanceEventType.CANCEL_USE), IssuanceStatus.ISSUED,
            new Transition(IssuanceStatus.ISSUED, IssuanceEventType.CANCEL), IssuanceStatus.CANCELLED,
            new Transition(IssuanceStatus.ISSUED, IssuanceEventType.EXPIRE), IssuanceStatus.EXPIRED
    );

    private CouponStateMachine() {
    }

    /**
     * 현재 상태에서 이 사건이 일어나면 어떤 상태가 되는가.
     *
     * @param from  추적 중인 현재 상태. 발급 이전이면 null
     * @param event 일어난 사건
     * @return 합법이면 결과 상태, 불법이면 빈 값
     */
    public static Optional<IssuanceStatus> next(IssuanceStatus from, IssuanceEventType event) {
        if (event == null) {
            throw new IllegalArgumentException("사건 종류가 필요합니다.");
        }

        return Optional.ofNullable(TABLE.get(new Transition(from, event)));
    }

    /** 이력 한 행이 주장하는 (from, event, to) 삼중이 전이표에 있는가. */
    public static boolean isLegal(
            IssuanceStatus from,
            IssuanceEventType event,
            IssuanceStatus to
    ) {
        return next(from, event)
                .map(expected -> expected == to)
                .orElse(false);
    }

    /** 종단 상태에서는 어떤 사건도 합법이 아닙니다. */
    public static boolean isTerminal(IssuanceStatus status) {
        return status != null && status.isTerminal();
    }

    private record Transition(IssuanceStatus from, IssuanceEventType event) {
    }
}
