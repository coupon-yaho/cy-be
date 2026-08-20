package com.kafkick.api.coupon.controller;

import jakarta.validation.constraints.Positive;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.coupon.CouponRequestHeaders;
import com.kafkick.api.coupon.MemberRequestHeaders;
import com.kafkick.api.coupon.adapter.CouponCancelAdapter;
import com.kafkick.api.coupon.dto.CouponCancelResponse;
import com.kafkick.api.support.ResponseEnvelope;

@RestController
@RequestMapping("/api/v1/coupons")
public class CouponCancelController {

    private final CouponCancelAdapter cancelAdapter;

    public CouponCancelController(
            CouponCancelAdapter cancelAdapter
    ) {
        this.cancelAdapter = cancelAdapter;
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
        return ResponseEnvelope.success(cancelAdapter.cancel(
                issuanceId,
                memberId,
                idempotencyKey
        ));
    }
}
