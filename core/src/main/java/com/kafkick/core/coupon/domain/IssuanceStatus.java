// 발급건의 상태입니다. issuances.status 와 asof_state.state 가 같은 어휘를 씁니다.
package com.kafkick.core.coupon.domain;

/**
 * 종단 상태(CANCELLED · EXPIRED)에서는 더 이상 전이가 없습니다.
 * 취소와 만료는 재고를 복원하므로 active_count 가 줄어듭니다 — 누적이 아닙니다.
 *
 * <p><b>두 술어는 검증 배치가 씁니다(CY-744 합류 때 되살렸다).</b> 원래
 * {@code feature/CY-15} 의 같은 이름 enum 에 있던 것인데, 합류하면서 main 쪽 정의가
 * 남아 사라졌다. 값 자체를 밖에서 다시 판정하면 재고 불변식의 정의가 두 벌이 된다 —
 * 그 어긋남은 V1 이 조용히 틀린 답을 내는 형태로만 드러난다.
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
