package com.kafkick.core.coupon.v2;

import java.util.Objects;
import java.util.Optional;

import com.kafkick.core.coupon.service.result.CouponIssueResult;
import com.kafkick.core.coupon.v2.port.ClaimOutcome;
import com.kafkick.core.coupon.v2.port.ClaimResult;
import com.kafkick.core.coupon.v2.port.CompensateOutcome;
import com.kafkick.core.coupon.v2.port.CompleteOutcome;

/**
 * S5가 HTTP로 옮길 때까지 게이트와 완료 CAS 결과를 손실 없이 보존한다.
 *
 * <p>선점 결과에 따라 허용되는 조합이 다르다.
 * <ul>
 *   <li>선점 성공 — 발급 결과와 완료 CAS 가 <b>둘 다</b> 있어야 한다</li>
 *   <li>완료 replay — 발급 결과만 있다. CAS 를 다시 실행하지 않으므로 완료 결과는 없다</li>
 *   <li>그 밖의 거절 — 둘 다 없어야 한다</li>
 * </ul>
 *
 * @throws NullPointerException 위 조합에서 필수인 값이 비었을 때
 * @throws IllegalArgumentException 거절인데 발급·완료 결과가 있거나, replay 인데 완료 CAS 가
 *     실려 있을 때. <b>DB 괴리 표시도 제 거절에만 붙는다</b> —
 *     {@code databaseSoldOutAfterRedisClaim} 은 {@link ClaimOutcome#SOLD_OUT},
 *     {@code databaseDuplicateAfterRedisClaim} 은 {@link ClaimOutcome#DUP_PER_MEMBER} 이
 *     아니면 거절한다. 엉뚱한 거절에 붙으면 관제가 매진과 회원 괴리를 뒤바꿔 읽는다
 */
public record V2CouponIssueResult(
        ClaimResult claimResult,
        CouponIssueResult nullableIssueResult,
        CompleteOutcome nullableCompleteOutcome,
        boolean recoveredAfterFailure,
        boolean databaseSoldOutAfterRedisClaim,
        boolean databaseDuplicateAfterRedisClaim,
        CompensateOutcome nullableCompensateOutcome
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
        if (databaseSoldOutAfterRedisClaim
                && claimResult.outcome() != ClaimOutcome.SOLD_OUT) {
            throw new IllegalArgumentException(
                    "Redis 선점 뒤 DB 매진 표시는 SOLD_OUT 거절에만 붙을 수 있습니다.");
        }
        if (databaseDuplicateAfterRedisClaim
                && claimResult.outcome() != ClaimOutcome.DUP_PER_MEMBER) {
            throw new IllegalArgumentException(
                    "Redis 선점 뒤 DB 중복 표시는 DUP_PER_MEMBER 거절에만 붙을 수 있습니다.");
        }
    }

    public static V2CouponIssueResult rejected(ClaimResult claimResult) {
        return new V2CouponIssueResult(claimResult, null, null, false, false, false, null);
    }

    /**
     * @param compensation 되돌리기 결과. {@link CompensateOutcome#REVERTED} 가 아니면 이 요청의
     *     Redis 선점이 되돌아오지 않았다는 뜻이라 관제가 그것을 따로 세야 한다
     */
    public static V2CouponIssueResult rejectedAfterDatabaseSoldOut(CompensateOutcome compensation) {
        return new V2CouponIssueResult(
                ClaimResult.rejected(ClaimOutcome.SOLD_OUT), null, null, false, true, false,
                compensation);
    }

    /**
     * 게이트는 통과시켰는데 {@code uk_coupon_member} 가 잡은 중복.
     *
     * <p>게이트가 스스로 거른 {@code DUP_PER_MEMBER} 와 <b>응답은 같지만 원인이 다르다</b> —
     * 이쪽은 게이트의 회원 집합이 DB 와 갈렸다는 뜻이고, Sentinel 승격으로 복제가 유실되면
     * 정확히 이 상태가 된다. 두 경우를 한 카운터로 뭉치면 복제 유실이 평범한 재요청 물결에
     * 묻힌다. 매진 쪽 {@link #rejectedAfterDatabaseSoldOut(CompensateOutcome)} 과 같은 자리다.
     */
    public static V2CouponIssueResult rejectedAfterDatabaseDuplicate(CompensateOutcome compensation) {
        return new V2CouponIssueResult(
                ClaimResult.rejected(ClaimOutcome.DUP_PER_MEMBER), null, null, false, false, true,
                compensation);
    }

    public static V2CouponIssueResult replayed(
            ClaimResult claimResult,
            CouponIssueResult issueResult
    ) {
        return new V2CouponIssueResult(claimResult, issueResult, null, false, false, false, null);
    }

    public static V2CouponIssueResult issued(
            ClaimResult claimResult, CouponIssueResult issueResult, CompleteOutcome completeOutcome
    ) {
        return new V2CouponIssueResult(claimResult, issueResult, completeOutcome, false, false, false, null);
    }

    public static V2CouponIssueResult recovered(
            ClaimResult claimResult, CouponIssueResult issueResult, CompleteOutcome completeOutcome
    ) {
        return new V2CouponIssueResult(claimResult, issueResult, completeOutcome, true, false, false, null);
    }

    public boolean replayed() {
        return claimResult.outcome()
                == ClaimOutcome.REPLAY_DONE;
    }

    public Optional<CouponIssueResult> issueResult() {
        return Optional.ofNullable(nullableIssueResult);
    }

    /**
     * 되돌리기 결과. DB 가 막아 거절한 두 경로에만 실린다.
     *
     * <p>{@link CompensateOutcome#REVERTED} 가 아니면 이 요청이 Redis 재고를 하나 줄인 채
     * 끝났다는 뜻이다 — 초과 발급이 아니라 <b>과소 발급</b> 방향이라 응답으로는 드러나지
     * 않고, 세지 않으면 정합성 대조에서 원인 불명의 차이로만 남는다.
     */
    public Optional<CompensateOutcome> compensateOutcome() {
        return Optional.ofNullable(nullableCompensateOutcome);
    }

    public Optional<CompleteOutcome> completeOutcome() {
        return Optional.ofNullable(nullableCompleteOutcome);
    }
}
