// 쿠폰 발급 취소 요청 전략을 공통 멱등 실행 템플릿에 연결합니다.
package com.kafkick.api.coupon.adapter;

import org.springframework.stereotype.Component;

import com.kafkick.api.coupon.dto.CouponCancelResponse;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.coupon.service.CouponCancelCommand;

@Component
public class CouponCancelTransactionalAdapter {

    private final IdempotencyExecutionTemplate idempotencyTemplate;
    private final CouponCancelTransactionExecutor transactionExecutor;
    private final CouponCancelResponseCodec responseCodec;

    public CouponCancelTransactionalAdapter(
            IdempotencyExecutionTemplate idempotencyTemplate,
            CouponCancelTransactionExecutor transactionExecutor,
            CouponCancelResponseCodec responseCodec
    ) {
        this.idempotencyTemplate = idempotencyTemplate;
        this.transactionExecutor = transactionExecutor;
        this.responseCodec = responseCodec;
    }

    public CouponCancelResponse cancel(
            Long issuanceId,
            Long memberId,
            String idempotencyKey
    ) {
        return idempotencyTemplate.execute(
                idempotencyKey,
                () -> canonicalRequest(issuanceId, memberId),
                CouponUseErrorCode.INVALID_COUPON_CANCEL_REQUEST,
                claimedAt -> transactionExecutor.execute(
                        new CouponCancelCommand(
                                issuanceId,
                                memberId,
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
            Long memberId
    ) {
        return "CANCEL|issuanceId=" + issuanceId
                + "|memberId=" + memberId;
    }
}
