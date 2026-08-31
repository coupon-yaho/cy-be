package com.kafkick.api.coupon.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.coupon.http.CouponRequestHeaders;
import com.kafkick.api.support.auth.MemberRequestHeaders;
import com.kafkick.api.coupon.dto.request.CouponUseRequest;
import com.kafkick.api.coupon.dto.response.CouponUseResponse;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.api.coupon.service.CouponOperationRetryingExecutor;

@RestController
@RequestMapping("/api/v1/coupons")
public class CouponUseController {

    private final CouponOperationRetryingExecutor executionService;

    public CouponUseController(
            CouponOperationRetryingExecutor executionService
    ) {
        this.executionService = executionService;
    }

    @PostMapping("/{issuanceId}/use")
    public ResponseEnvelope<CouponUseResponse> use(
            @PathVariable
            @Positive(message = "발급 ID는 0보다 커야 합니다.")
            Long issuanceId,
            @RequestHeader(MemberRequestHeaders.MEMBER_ID)
            @Positive(message = "회원 ID는 0보다 커야 합니다.")
            Long memberId,
            @RequestHeader(CouponRequestHeaders.IDEMPOTENCY_KEY)
            String idempotencyKey,
            @Valid @RequestBody CouponUseRequest request
    ) {
        return ResponseEnvelope.success(CouponUseResponse.from(
                executionService.use(
                        issuanceId,
                        memberId,
                        request.orderAmount(),
                        idempotencyKey
                )
        ));
    }
}
