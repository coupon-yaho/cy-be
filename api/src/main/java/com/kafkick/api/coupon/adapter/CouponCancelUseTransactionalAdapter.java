// 쿠폰 사용 취소 요청 전략을 공통 멱등 실행 템플릿에 연결합니다.
package com.kafkick.api.coupon.adapter;

import org.springframework.stereotype.Component;

import com.kafkick.api.coupon.dto.CouponCancelUseResponse;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.coupon.service.CouponCancelUseCommand;

@Component
public class CouponCancelUseTransactionalAdapter {

    private final IdempotencyExecutionTemplate idempotencyTemplate;
    private final CouponCancelUseTransactionExecutor transactionExecutor;
    private final CouponCancelUseResponseCodec responseCodec;

    public CouponCancelUseTransactionalAdapter(
            IdempotencyExecutionTemplate idempotencyTemplate,
            CouponCancelUseTransactionExecutor transactionExecutor,
            CouponCancelUseResponseCodec responseCodec
    ) {
        this.idempotencyTemplate = idempotencyTemplate;
        this.transactionExecutor = transactionExecutor;
        this.responseCodec = responseCodec;
    }

    public CouponCancelUseResponse cancelUse(
            Long issuanceId,
            Long memberId,
            String idempotencyKey
    ) {
        return idempotencyTemplate.execute(
                idempotencyKey,
                () -> canonicalRequest(issuanceId, memberId),
                CouponUseErrorCode.INVALID_COUPON_CANCEL_USE_REQUEST,
                claimedAt -> transactionExecutor.execute(
                        new CouponCancelUseCommand(
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
        return "CANCEL_USE|issuanceId=" + issuanceId
                + "|memberId=" + memberId;
    }
}
