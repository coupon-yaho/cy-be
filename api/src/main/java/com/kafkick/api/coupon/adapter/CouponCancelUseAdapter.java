package com.kafkick.api.coupon.adapter;

import org.springframework.stereotype.Component;

import com.kafkick.api.coupon.dto.CouponCancelUseResponse;
import com.kafkick.core.coupon.service.CouponOperationExecutionService;

@Component
public class CouponCancelUseAdapter {

    private final CouponOperationExecutionService executionService;

    public CouponCancelUseAdapter(
            CouponOperationExecutionService executionService
    ) {
        this.executionService = executionService;
    }

    public CouponCancelUseResponse cancelUse(
            Long issuanceId,
            Long memberId,
            String idempotencyKey
    ) {
        return CouponCancelUseResponse.from(executionService.cancelUse(
                issuanceId,
                memberId,
                idempotencyKey
        ));
    }
}
