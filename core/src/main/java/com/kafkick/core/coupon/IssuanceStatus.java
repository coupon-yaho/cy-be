// 발급건의 상태입니다. issuances.status 와 asof_state.state 가 같은 어휘를 씁니다.
package com.kafkick.core.coupon;

/**
 * 종단 상태(CANCELLED · EXPIRED)에서는 더 이상 전이가 없습니다.
 * 취소와 만료는 재고를 복원하므로 active_count 가 줄어듭니다 — 누적이 아닙니다.
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
