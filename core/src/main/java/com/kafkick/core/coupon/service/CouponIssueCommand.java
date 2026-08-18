package com.kafkick.core.coupon.service;

import java.time.Instant;

import com.kafkick.core.coupon.domain.MembershipGrade;

public record CouponIssueCommand(
        Long couponRoundId,
        Long memberId,
        MembershipGrade membershipGrade,
        String requestId,
        Instant issuedAt
) {
}
