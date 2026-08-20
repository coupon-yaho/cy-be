package com.kafkick.api.coupon.adapter;

import org.springframework.stereotype.Component;

import com.kafkick.api.coupon.dto.CouponCancelResponse;
import com.kafkick.core.coupon.service.CouponOperationExecutionService;

@Component
public class CouponCancelAdapter {

    private final CouponOperationExecutionService executionService;

    public CouponCancelAdapter(
            CouponOperationExecutionService executionService
    ) {
        this.executionService = executionService;
    }

    public CouponCancelResponse cancel(
            Long issuanceId,
            Long memberId,
            String idempotencyKey
    ) {
        return CouponCancelResponse.from(executionService.cancel(
                issuanceId,
                memberId,
                idempotencyKey
        ));
    }
}
