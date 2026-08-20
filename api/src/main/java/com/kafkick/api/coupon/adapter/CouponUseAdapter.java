package com.kafkick.api.coupon.adapter;

import org.springframework.stereotype.Component;

import com.kafkick.api.coupon.dto.CouponUseRequest;
import com.kafkick.api.coupon.dto.CouponUseResponse;
import com.kafkick.core.coupon.service.CouponOperationExecutionService;

@Component
public class CouponUseAdapter {

    private final CouponOperationExecutionService executionService;

    public CouponUseAdapter(
            CouponOperationExecutionService executionService
    ) {
        this.executionService = executionService;
    }

    public CouponUseResponse use(
            Long issuanceId,
            Long memberId,
            String idempotencyKey,
            CouponUseRequest request
    ) {
        return CouponUseResponse.from(executionService.use(
                issuanceId,
                memberId,
                request.orderId(),
                request.orderAmount(),
                idempotencyKey
        ));
    }
}
