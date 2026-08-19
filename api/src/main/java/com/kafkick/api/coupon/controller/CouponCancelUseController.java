// 회원 소유 발급 쿠폰의 사용 취소를 멱등하게 처리하는 API를 제공합니다.
package com.kafkick.api.coupon.controller;

import jakarta.validation.constraints.Positive;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.coupon.CouponRequestHeaders;
import com.kafkick.api.coupon.MemberRequestHeaders;
import com.kafkick.api.coupon.adapter.CouponCancelUseTransactionalAdapter;
import com.kafkick.api.coupon.dto.CouponCancelUseResponse;
import com.kafkick.api.support.ResponseEnvelope;

@RestController
@RequestMapping("/api/v1/coupons")
public class CouponCancelUseController {

    private final CouponCancelUseTransactionalAdapter cancelUseAdapter;

    public CouponCancelUseController(
            CouponCancelUseTransactionalAdapter cancelUseAdapter
    ) {
        this.cancelUseAdapter = cancelUseAdapter;
    }

    @PostMapping("/{issuanceId}/cancel-use")
    public ResponseEnvelope<CouponCancelUseResponse> cancelUse(
            @PathVariable
            @Positive(message = "발급 ID는 0보다 커야 합니다.")
            Long issuanceId,
            @RequestHeader(MemberRequestHeaders.MEMBER_ID)
            @Positive(message = "회원 ID는 0보다 커야 합니다.")
            Long memberId,
            @RequestHeader(CouponRequestHeaders.IDEMPOTENCY_KEY)
            String idempotencyKey
    ) {
        return ResponseEnvelope.success(cancelUseAdapter.cancelUse(
                issuanceId,
                memberId,
                idempotencyKey
        ));
    }
}
