// 가상 회원 헤더를 받아 공개된 회차의 쿠폰을 발급합니다.
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

import com.kafkick.api.coupon.MemberRequestHeaders;
import com.kafkick.api.coupon.adapter.CouponIssueTransactionalAdapter;
import com.kafkick.api.coupon.dto.CouponIssueResponse;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.MembershipGrade;

@RestController
@RequestMapping("/api/v1/coupons")
public class CouponIssueController {

    private static final String REQUEST_ID = "requestId";

    private final CouponIssueTransactionalAdapter issueAdapter;

    public CouponIssueController(
            CouponIssueTransactionalAdapter issueAdapter
    ) {
        this.issueAdapter = issueAdapter;
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
        Issuance issuance = issueAdapter.issue(
                couponRoundId,
                memberId,
                membershipGrade,
                MDC.get(REQUEST_ID)
        );

        return ResponseEntity.status(HttpStatus.CREATED)
                .body(ResponseEnvelope.success(
                        CouponIssueResponse.from(issuance)
                ));
    }
}
