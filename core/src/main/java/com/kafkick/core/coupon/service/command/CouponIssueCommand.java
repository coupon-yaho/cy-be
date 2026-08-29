package com.kafkick.core.coupon.service.command;

import java.time.Instant;

import com.kafkick.core.membership.domain.MembershipGrade;

public record CouponIssueCommand(
        Long couponRoundId,
        Long memberId,
        MembershipGrade membershipGrade,
        String idempotencyKey,
        Instant issuedAt
) {

    public static String canonicalRequest(
            Long couponRoundId,
            Long memberId,
            MembershipGrade membershipGrade
    ) {
        return "ISSUE|couponRoundId=" + couponRoundId
                + "|memberId=" + memberId
                + "|membershipGrade=" + membershipGrade;
    }
}
