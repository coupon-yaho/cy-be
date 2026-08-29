// 발급건의 상태입니다. issuances.status 와 asof_state.state 가 같은 어휘를 씁니다.
package com.kafkick.core.coupon.domain;

/**
 * 종단 상태(CANCELLED · EXPIRED)에서는 더 이상 전이가 없습니다.
 * 취소와 만료는 재고를 복원하므로 active_count 가 줄어듭니다 — 누적이 아닙니다.
 *
 * <p><b>{@code countsTowardStock} 은 재고 불변식의 정의를 한 곳에 둔다.</b> 원래
 * {@code feature/CY-15} 의 같은 이름 enum 에 있던 것인데, 합류하면서 main 쪽 정의가
 * 남아 사라져 되살렸다.
 *
 * <p>⚠️ <b>지금 호출자는 {@code VerificationSeed} 하나다.</b> V1 재고 대조 SQL 은 아직
 * {@code state IN ('ISSUED','USED')} 를 문자열로 박고 있어서 이 술어를 안 지난다 —
 * 즉 정의가 실질적으로 두 벌이다. 그 SQL 이 이 값을 받게 바꾸는 것이 맞지만,
 * 그때까지 <b>"검증이 쓴다" 고 적지 않는다.</b> 안 쓰는 것을 쓴다고 적으면 다음 사람이
 * 그 문장을 근거로 판단한다.
 */
public enum IssuanceStatus {

    ISSUED(false),
    USED(false),
    CANCELLED(true),
    EXPIRED(true);

    private final boolean terminal;

    IssuanceStatus(boolean terminal) {
        this.terminal = terminal;
    }

    /** 재고 불변식의 분자. 잔여 = total_quantity - (ISSUED + USED) */
    public boolean countsTowardStock() {
        return this == ISSUED || this == USED;
    }

    public boolean isTerminal() {
        return terminal;
    }
}
