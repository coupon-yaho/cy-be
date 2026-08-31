package com.kafkick.api.coupon.controller;

import java.util.List;

import jakarta.validation.constraints.Positive;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.support.auth.MemberRequestHeaders;
import com.kafkick.api.coupon.http.CouponRequestHeaders;
import com.kafkick.api.coupon.dto.response.CouponIssueResponse;
import com.kafkick.api.coupon.monitoring.CouponIssueMetrics;
import com.kafkick.api.observation.issuance.CouponIssueObservationCoordinator;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.api.support.RequestIdFilter;
import com.kafkick.api.support.auth.MemberGradeHeaderResolver;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.coupon.service.result.CouponIssueResult;

@RestController
@RequestMapping("/api/v1/coupons")
public class CouponIssueController {

    private final CouponIssueObservationCoordinator observationCoordinator;
    private final CouponIssueMetrics couponIssueMetrics;

    public CouponIssueController(
            CouponIssueObservationCoordinator observationCoordinator,
            CouponIssueMetrics couponIssueMetrics
    ) {
        this.observationCoordinator = observationCoordinator;
        this.couponIssueMetrics = couponIssueMetrics;
    }

    @PostMapping("/{couponRoundId}/issue")
    public ResponseEntity<ResponseEnvelope<CouponIssueResponse>> issue(
            @RequestAttribute(RequestIdFilter.REQUEST_ID_ATTRIBUTE)
            String requestId,
            @PathVariable
            @Positive(message = "쿠폰 회차 ID는 0보다 커야 합니다.")
            Long couponRoundId,
            @RequestHeader(MemberRequestHeaders.MEMBER_ID)
            @Positive(message = "회원 ID는 0보다 커야 합니다.")
            Long memberId,
            @RequestHeader(value = MemberRequestHeaders.MEMBER_GRADE, required = false)
            List<String> memberGradeValues,
            @RequestHeader(CouponRequestHeaders.IDEMPOTENCY_KEY)
            String idempotencyKey
    ) {
        long startedAt = System.nanoTime();
        couponIssueMetrics.recordStarted(couponRoundId, memberId);
        CouponIssueResult result;
        try {
            result = observationCoordinator.issue(
                    requestId,
                    couponRoundId,
                    memberId,
                    MemberGradeHeaderResolver.resolve(memberGradeValues),
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
