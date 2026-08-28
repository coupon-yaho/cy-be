package com.kafkick.core.coupon.v2;

import java.util.Objects;
import java.util.Optional;

import com.kafkick.core.coupon.service.result.CouponIssueResult;
import com.kafkick.core.coupon.v2.port.ClaimOutcome;
import com.kafkick.core.coupon.v2.port.ClaimResult;
import com.kafkick.core.coupon.v2.port.CompleteOutcome;

/** S5가 HTTP로 옮길 때까지 게이트와 완료 CAS 결과를 손실 없이 보존한다. */
public record V2CouponIssueResult(
        ClaimResult claimResult,
        CouponIssueResult nullableIssueResult,
        CompleteOutcome nullableCompleteOutcome,
        boolean recoveredAfterFailure
) {

    public V2CouponIssueResult {
        Objects.requireNonNull(claimResult, "claimResult");
        if (claimResult.outcome().isClaimed()) {
            Objects.requireNonNull(nullableIssueResult, "nullableIssueResult");
            Objects.requireNonNull(nullableCompleteOutcome, "nullableCompleteOutcome");
        } else if (claimResult.outcome() == ClaimOutcome.REPLAY_DONE) {
            Objects.requireNonNull(nullableIssueResult, "nullableIssueResult");
            if (nullableCompleteOutcome != null) {
                throw new IllegalArgumentException("완료 replay는 CAS를 다시 실행하지 않습니다.");
            }
        } else if (nullableIssueResult != null || nullableCompleteOutcome != null) {
            throw new IllegalArgumentException("거절 결과에는 발급 또는 완료 CAS 결과가 있을 수 없습니다.");
        }
    }

    public static V2CouponIssueResult rejected(ClaimResult claimResult) {
        return new V2CouponIssueResult(claimResult, null, null, false);
    }

    public static V2CouponIssueResult replayed(
            ClaimResult claimResult,
            CouponIssueResult issueResult
    ) {
        return new V2CouponIssueResult(claimResult, issueResult, null, false);
    }

    public boolean replayed() {
        return claimResult.outcome()
                == ClaimOutcome.REPLAY_DONE;
    }

    public Optional<CouponIssueResult> issueResult() {
        return Optional.ofNullable(nullableIssueResult);
    }

    public Optional<CompleteOutcome> completeOutcome() {
        return Optional.ofNullable(nullableCompleteOutcome);
    }
}
