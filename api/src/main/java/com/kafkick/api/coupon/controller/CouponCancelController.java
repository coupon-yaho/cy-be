package com.kafkick.api.coupon.controller;

import jakarta.validation.constraints.Positive;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.coupon.http.CouponRequestHeaders;
import com.kafkick.api.support.auth.MemberRequestHeaders;
import com.kafkick.api.coupon.dto.response.CouponCancelResponse;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.core.coupon.service.CouponOperationExecutionService;

@RestController
@RequestMapping("/api/v1/coupons")
public class CouponCancelController {

    private final CouponOperationExecutionService executionService;

    public CouponCancelController(
            CouponOperationExecutionService executionService
    ) {
        this.executionService = executionService;
    }

    @PostMapping("/{issuanceId}/cancel")
    public ResponseEnvelope<CouponCancelResponse> cancel(
            @PathVariable
            @Positive(message = "발급 ID는 0보다 커야 합니다.")
            Long issuanceId,
            @RequestHeader(MemberRequestHeaders.MEMBER_ID)
            @Positive(message = "회원 ID는 0보다 커야 합니다.")
            Long memberId,
            @RequestHeader(CouponRequestHeaders.IDEMPOTENCY_KEY)
            String idempotencyKey
    ) {
        return ResponseEnvelope.success(CouponCancelResponse.from(
                executionService.cancel(
                        issuanceId,
                        memberId,
                        idempotencyKey
                )
        ));
    }
}
