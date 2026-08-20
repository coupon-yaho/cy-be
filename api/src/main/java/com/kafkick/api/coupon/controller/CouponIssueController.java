package com.kafkick.api.coupon.controller;

import org.slf4j.MDC;

import jakarta.validation.constraints.Positive;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.support.auth.MemberRequestHeaders;
import com.kafkick.api.coupon.dto.response.CouponIssueResponse;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.coupon.service.command.CouponIssueCommand;
import com.kafkick.core.coupon.service.CouponIssueService;
import com.kafkick.core.support.TimeProvider;

@RestController
@RequestMapping("/api/v1/coupons")
public class CouponIssueController {

    private static final String REQUEST_ID = "requestId";

    private final CouponIssueService couponIssueService;
    private final TimeProvider timeProvider;

    public CouponIssueController(
            CouponIssueService couponIssueService,
            TimeProvider timeProvider
    ) {
        this.couponIssueService = couponIssueService;
        this.timeProvider = timeProvider;
    }

    @PostMapping("/{couponRoundId}/issue")
    public ResponseEntity<ResponseEnvelope<CouponIssueResponse>> issue(
            @PathVariable
            @Positive(message = "쿠폰 회차 ID는 0보다 커야 합니다.")
            Long couponRoundId,
            @RequestHeader(MemberRequestHeaders.MEMBER_ID)
            @Positive(message = "회원 ID는 0보다 커야 합니다.")
            Long memberId,
            @RequestHeader(MemberRequestHeaders.MEMBERSHIP_GRADE)
            MembershipGrade membershipGrade
    ) {
        Issuance issuance = couponIssueService.issue(new CouponIssueCommand(
                couponRoundId,
                memberId,
                membershipGrade,
                normalizeRequestId(MDC.get(REQUEST_ID)),
                timeProvider.instant()
        ));

        return ResponseEntity.status(HttpStatus.CREATED)
                .body(ResponseEnvelope.success(
                        CouponIssueResponse.from(issuance)
                ));
    }

    private static String normalizeRequestId(String requestId) {
        if (requestId == null || requestId.length() > 36) {
            return null;
        }
        return requestId;
    }
}
