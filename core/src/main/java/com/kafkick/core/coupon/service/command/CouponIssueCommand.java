package com.kafkick.core.coupon.service.command;

import java.time.Instant;

import com.kafkick.core.membership.domain.MembershipGrade;

public record CouponIssueCommand(
        Long couponRoundId,
        Long memberId,
        MembershipGrade membershipGrade,
        String requestId,
        Instant issuedAt
) {
}
