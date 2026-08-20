package com.kafkick.core.coupon.service;

import org.springframework.stereotype.Service;

import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.coupon.port.IdempotencyResultCodec;
import com.kafkick.core.coupon.service.command.CouponCancelCommand;
import com.kafkick.core.coupon.service.command.CouponCancelUseCommand;
import com.kafkick.core.coupon.service.command.CouponUseCommand;
import com.kafkick.core.coupon.service.idempotency.IdempotencyExecutionService;
import com.kafkick.core.coupon.service.idempotency.IdempotentOperationService;
import com.kafkick.core.coupon.service.result.CouponCancelResult;
import com.kafkick.core.coupon.service.result.CouponCancelUseResult;
import com.kafkick.core.coupon.service.result.CouponUseResult;

@Service
public class CouponOperationExecutionService {

    private final IdempotencyExecutionService idempotencyExecutionService;
    private final IdempotentOperationService operationService;
    private final CouponUseService couponUseService;
    private final CouponCancelUseService couponCancelUseService;
    private final CouponCancelService couponCancelService;
    private final IdempotencyResultCodec<CouponUseResult> useCodec;
    private final IdempotencyResultCodec<CouponCancelUseResult> cancelUseCodec;
    private final IdempotencyResultCodec<CouponCancelResult> cancelCodec;

    public CouponOperationExecutionService(
            IdempotencyExecutionService idempotencyExecutionService,
            IdempotentOperationService operationService,
            CouponUseService couponUseService,
            CouponCancelUseService couponCancelUseService,
            CouponCancelService couponCancelService,
            IdempotencyResultCodec<CouponUseResult> useCodec,
            IdempotencyResultCodec<CouponCancelUseResult> cancelUseCodec,
            IdempotencyResultCodec<CouponCancelResult> cancelCodec
    ) {
        this.idempotencyExecutionService = idempotencyExecutionService;
        this.operationService = operationService;
        this.couponUseService = couponUseService;
        this.couponCancelUseService = couponCancelUseService;
        this.couponCancelService = couponCancelService;
        this.useCodec = useCodec;
        this.cancelUseCodec = cancelUseCodec;
        this.cancelCodec = cancelCodec;
    }

    public CouponUseResult use(
            Long issuanceId,
            Long memberId,
            Long orderId,
            int orderAmount,
            String idempotencyKey
    ) {
        return idempotencyExecutionService.execute(
                idempotencyKey,
                () -> CouponUseCommand.canonicalRequest(
                        issuanceId, memberId, orderId, orderAmount
                ),
                CouponUseErrorCode.INVALID_COUPON_USE_REQUEST,
                claimedAt -> operationService.execute(
                        idempotencyKey,
                        memberId,
                        issuanceId,
                        claimedAt,
                        () -> couponUseService.use(new CouponUseCommand(
                                issuanceId,
                                memberId,
                                orderId,
                                orderAmount,
                                idempotencyKey,
                                claimedAt
                        )),
                        useCodec
                ),
                useCodec::read
        );
    }

    public CouponCancelUseResult cancelUse(
            Long issuanceId,
            Long memberId,
            String idempotencyKey
    ) {
        return idempotencyExecutionService.execute(
                idempotencyKey,
                () -> CouponCancelUseCommand.canonicalRequest(
                        issuanceId, memberId
                ),
                CouponUseErrorCode.INVALID_COUPON_CANCEL_USE_REQUEST,
                claimedAt -> operationService.execute(
                        idempotencyKey,
                        memberId,
                        issuanceId,
                        claimedAt,
                        () -> couponCancelUseService.cancelUse(
                                new CouponCancelUseCommand(
                                        issuanceId,
                                        memberId,
                                        idempotencyKey,
                                        claimedAt
                                )
                        ),
                        cancelUseCodec
                ),
                cancelUseCodec::read
        );
    }

    public CouponCancelResult cancel(
            Long issuanceId,
            Long memberId,
            String idempotencyKey
    ) {
        return idempotencyExecutionService.execute(
                idempotencyKey,
                () -> CouponCancelCommand.canonicalRequest(
                        issuanceId, memberId
                ),
                CouponUseErrorCode.INVALID_COUPON_CANCEL_REQUEST,
                claimedAt -> operationService.execute(
                        idempotencyKey,
                        memberId,
                        issuanceId,
                        claimedAt,
                        () -> couponCancelService.cancel(
                                new CouponCancelCommand(
                                        issuanceId,
                                        memberId,
                                        idempotencyKey,
                                        claimedAt
                                )
                        ),
                        cancelCodec
                ),
                cancelCodec::read
        );
    }
}
