// 발급 취소 상태·재고·이력과 멱등 완료 응답을 하나의 트랜잭션으로 저장합니다.
package com.kafkick.api.coupon.adapter;

import java.time.Instant;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.api.coupon.dto.CouponCancelResponse;
import com.kafkick.core.coupon.port.IdempotencyRepository;
import com.kafkick.core.coupon.service.CouponCancelCommand;
import com.kafkick.core.coupon.service.CouponCancelResult;
import com.kafkick.core.coupon.service.CouponCancelService;

@Component
public class CouponCancelTransactionExecutor {

    private final CouponCancelService cancelService;
    private final IdempotencyRepository idempotencyRepository;
    private final CouponCancelResponseCodec responseCodec;

    public CouponCancelTransactionExecutor(
            CouponCancelService cancelService,
            IdempotencyRepository idempotencyRepository,
            CouponCancelResponseCodec responseCodec
    ) {
        this.cancelService = cancelService;
        this.idempotencyRepository = idempotencyRepository;
        this.responseCodec = responseCodec;
    }

    @Transactional
    public CouponCancelResponse execute(
            CouponCancelCommand command,
            Instant claimedAt
    ) {
        CouponCancelResult result = cancelService.cancel(command);
        CouponCancelResponse response = CouponCancelResponse.from(result);
        idempotencyRepository.complete(
                command.idempotencyKey(),
                command.memberId(),
                command.issuanceId(),
                responseCodec.write(response),
                claimedAt
        );
        return response;
    }
}
