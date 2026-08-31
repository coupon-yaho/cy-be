package com.kafkick.core.queuegateway;

import java.time.Instant;
import java.util.Objects;

import com.kafkick.core.observation.SourceStatus;

/**
 * 외부 대기열 게이트웨이에 미러링할 한 쿠폰 회차의 재고 관측값입니다.
 *
 * <p>회차 ID는 양수, 재고는 0 이상이어야 합니다. 값을 싣는 상태에는 재고와 관측 시각이 모두
 * 필요하고 값을 싣지 않는 상태에는 둘 다 없어야 하며, 위반하면 생성 시점에
 * {@link IllegalArgumentException}을 던집니다.</p>
 */
public record QueueGatewayCouponRoundState(
        long couponId,
        Long remainingStock,
        SourceStatus stockStatus,
        Instant observedAt
) {

    /**
     * 모순된 관측값을 <b>만들어지는 자리에서</b> 막습니다. 게이트웨이로 나간 뒤에는 어느
     * 인스턴스가 무엇을 잘못 실었는지 되짚기 어렵습니다.
     *
     * <p>거부 조건은 넷이고 전부 즉시 실패합니다.
     *
     * <ul>
     *   <li>{@code couponId} 가 0 이하 — 회차를 특정할 수 없다</li>
     *   <li>{@code stockStatus} 가 값을 싣는다고 말하는데 재고·관측 시각이 없거나, 반대로
     *       값을 안 싣는다면서 둘이 있다 — 게이트웨이가 상태와 값을 따로 읽으므로
     *       어긋나면 <b>없는 재고를 있다고 읽는다</b></li>
     *   <li>{@code remainingStock} 과 {@code observedAt} 중 하나만 있다 — 언제 잰 값인지
     *       모르는 재고는 신선도를 판정할 수 없어 영원히 유효하거나 영원히 낡는다</li>
     *   <li>{@code remainingStock} 이 음수 — 남은 재고에 음수는 없다</li>
     * </ul>
     *
     * @throws NullPointerException {@code stockStatus} 가 {@code null} 일 때
     * @throws IllegalArgumentException 위 네 조건 중 하나에 걸릴 때
     */
    public QueueGatewayCouponRoundState {
        Objects.requireNonNull(stockStatus, "stockStatus");
        if (couponId <= 0L) {
            throw new IllegalArgumentException("couponId는 양수여야 합니다.");
        }
        boolean hasValue = remainingStock != null && observedAt != null;
        if (stockStatus.carriesValue() != hasValue) {
            throw new IllegalArgumentException("stockStatus와 재고·관측 시각 조합이 일치해야 합니다.");
        }
        if ((remainingStock == null) != (observedAt == null)) {
            throw new IllegalArgumentException("remainingStock과 observedAt은 함께 존재해야 합니다.");
        }
        if (remainingStock != null && remainingStock < 0L) {
            throw new IllegalArgumentException("remainingStock은 0 이상이어야 합니다.");
        }
    }
}
