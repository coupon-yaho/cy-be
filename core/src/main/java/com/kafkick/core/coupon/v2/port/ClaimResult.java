package com.kafkick.core.coupon.v2.port;

import java.util.Objects;

/**
 * 선점 반환을 가두는 값 객체.
 *
 * <p>스크립트의 반환은 <b>실패면 1원소, 성공이면 2원소</b>다. 원시 {@code List} 로 흘리면
 * 그 사실이 타입에 안 드러나 호출부가 {@code get(1)} 을 하게 되고, 거절 경로에서 그 접근은
 * 예외이거나 — 더 나쁘게는 — 다른 스크립트의 결과를 잘못 읽는다.
 * <b>{@code get(1)} 은 어댑터 밖으로 나가지 않는다.</b>
 */
public final class ClaimResult {

    private final ClaimOutcome outcome;
    private final Long remainingStock;

    private ClaimResult(ClaimOutcome outcome, Long remainingStock) {
        this.outcome = Objects.requireNonNull(outcome, "outcome");
        this.remainingStock = remainingStock;
    }

    public static ClaimResult claimed(long remainingStock) {
        return new ClaimResult(ClaimOutcome.CLAIMED, remainingStock);
    }

    public static ClaimResult rejected(ClaimOutcome outcome) {
        if (outcome == ClaimOutcome.CLAIMED) {
            throw new IllegalArgumentException("선점 성공에는 잔여 재고가 있어야 합니다.");
        }
        return new ClaimResult(outcome, null);
    }

    public ClaimOutcome outcome() {
        return outcome;
    }

    /**
     * @return 선점 직후의 잔여 재고
     * @throws IllegalStateException 선점이 성공하지 않은 경우. 거절에는 잔여 재고가 <b>없다</b> —
     *     0 으로 채우면 "매진" 과 구분되지 않는다
     */
    public long remainingStock() {
        if (remainingStock == null) {
            throw new IllegalStateException("선점이 성공하지 않아 잔여 재고가 없습니다: " + outcome);
        }
        return remainingStock;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ClaimResult result
                && outcome == result.outcome
                && Objects.equals(remainingStock, result.remainingStock);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outcome, remainingStock);
    }

    @Override
    public String toString() {
        return "ClaimResult[outcome=" + outcome + ", remainingStock=" + remainingStock + "]";
    }
}
