// 쿠폰 사용 요청 전략을 공통 멱등 실행 템플릿에 연결합니다.
package com.kafkick.api.coupon.adapter;

import org.springframework.stereotype.Component;

import com.kafkick.api.coupon.dto.CouponUseRequest;
import com.kafkick.api.coupon.dto.CouponUseResponse;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.coupon.service.CouponUseCommand;

@Component
public class CouponUseTransactionalAdapter {

    private final IdempotencyExecutionTemplate idempotencyTemplate;
    private final CouponUseTransactionExecutor transactionExecutor;
    private final CouponUseResponseCodec responseCodec;

    public CouponUseTransactionalAdapter(
            IdempotencyExecutionTemplate idempotencyTemplate,
            CouponUseTransactionExecutor transactionExecutor,
            CouponUseResponseCodec responseCodec
    ) {
        this.idempotencyTemplate = idempotencyTemplate;
        this.transactionExecutor = transactionExecutor;
        this.responseCodec = responseCodec;
    }

    public CouponUseResponse use(
            Long issuanceId,
            Long memberId,
            String idempotencyKey,
            CouponUseRequest request
    ) {
        return idempotencyTemplate.execute(
                idempotencyKey,
                () -> canonicalRequest(issuanceId, memberId, request),
                CouponUseErrorCode.INVALID_COUPON_USE_REQUEST,
                claimedAt -> transactionExecutor.execute(
                        new CouponUseCommand(
                                issuanceId,
                                memberId,
                                request.orderId(),
                                request.orderAmount(),
                                idempotencyKey,
                                claimedAt
                        ),
                        claimedAt
                ),
                responseCodec::read
        );
    }

    private static String canonicalRequest(
            Long issuanceId,
            Long memberId,
            CouponUseRequest request
    ) {
        return "USE|issuanceId=" + issuanceId
                + "|memberId=" + memberId
                + "|orderId=" + request.orderId()
                + "|orderAmount=" + request.orderAmount();
    }
}
