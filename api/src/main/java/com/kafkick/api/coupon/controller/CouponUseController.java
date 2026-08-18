// 회원 소유의 발급 쿠폰을 주문에 멱등하게 사용하는 API를 제공합니다.
package com.kafkick.api.coupon.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.coupon.CouponRequestHeaders;
import com.kafkick.api.coupon.MemberRequestHeaders;
import com.kafkick.api.coupon.adapter.CouponUseTransactionalAdapter;
import com.kafkick.api.coupon.dto.CouponUseRequest;
import com.kafkick.api.coupon.dto.CouponUseResponse;
import com.kafkick.api.support.ResponseEnvelope;

@RestController
@RequestMapping("/api/v1/coupons")
public class CouponUseController {

    private final CouponUseTransactionalAdapter couponUseAdapter;

    public CouponUseController(
            CouponUseTransactionalAdapter couponUseAdapter
    ) {
        this.couponUseAdapter = couponUseAdapter;
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
        return ResponseEnvelope.success(couponUseAdapter.use(
                issuanceId,
                memberId,
                idempotencyKey,
                request
        ));
    }
}
