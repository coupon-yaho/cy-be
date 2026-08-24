package com.kafkick.api.coupon.controller;

import jakarta.validation.constraints.Positive;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.support.auth.MemberRequestHeaders;
import com.kafkick.api.coupon.http.CouponRequestHeaders;
import com.kafkick.api.coupon.dto.response.CouponIssueResponse;
import com.kafkick.api.coupon.monitoring.CouponIssueMetrics;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.coupon.service.CouponOperationExecutionService;
import com.kafkick.core.coupon.service.result.CouponIssueResult;

@RestController
@RequestMapping("/api/v1/coupons")
public class CouponIssueController {

    private final CouponOperationExecutionService operationExecutionService;
    private final CouponIssueMetrics couponIssueMetrics;

    public CouponIssueController(
            CouponOperationExecutionService operationExecutionService,
            CouponIssueMetrics couponIssueMetrics
    ) {
        this.operationExecutionService = operationExecutionService;
        this.couponIssueMetrics = couponIssueMetrics;
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
            MembershipGrade membershipGrade,
            @RequestHeader(CouponRequestHeaders.IDEMPOTENCY_KEY)
            String idempotencyKey
    ) {
        long startedAt = System.nanoTime();
        couponIssueMetrics.recordStarted(couponRoundId, memberId);
        CouponIssueResult result;
        try {
            result = operationExecutionService.issue(
                    couponRoundId,
                    memberId,
                    membershipGrade,
                    idempotencyKey
            );
            couponIssueMetrics.recordSuccess(
                    couponRoundId,
                    memberId,
                    elapsedSince(startedAt)
            );
        } catch (BusinessException exception) {
            couponIssueMetrics.recordBusinessFailure(
                    couponRoundId,
                    memberId,
                    exception.getErrorCode(),
                    elapsedSince(startedAt)
            );
            throw exception;
        } catch (RuntimeException exception) {
            couponIssueMetrics.recordUnexpectedFailure(
                    couponRoundId,
                    memberId,
                    elapsedSince(startedAt)
            );
            throw exception;
        }

        return ResponseEntity.status(HttpStatus.CREATED)
                .body(ResponseEnvelope.success(
                        CouponIssueResponse.from(result)
                ));
    }

    private static long elapsedSince(long startedAt) {
        return System.nanoTime() - startedAt;
    }
}
