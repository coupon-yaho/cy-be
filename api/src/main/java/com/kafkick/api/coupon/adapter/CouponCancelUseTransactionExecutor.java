// 사용 취소 상태·실적·이력과 멱등 완료 응답을 하나의 트랜잭션으로 저장합니다.
package com.kafkick.api.coupon.adapter;

import java.time.Instant;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.api.coupon.dto.CouponCancelUseResponse;
import com.kafkick.core.coupon.port.IdempotencyRepository;
import com.kafkick.core.coupon.service.CouponCancelUseCommand;
import com.kafkick.core.coupon.service.CouponCancelUseResult;
import com.kafkick.core.coupon.service.CouponCancelUseService;

@Component
public class CouponCancelUseTransactionExecutor {

    private final CouponCancelUseService cancelUseService;
    private final IdempotencyRepository idempotencyRepository;
    private final CouponCancelUseResponseCodec responseCodec;

    public CouponCancelUseTransactionExecutor(
            CouponCancelUseService cancelUseService,
            IdempotencyRepository idempotencyRepository,
            CouponCancelUseResponseCodec responseCodec
    ) {
        this.cancelUseService = cancelUseService;
        this.idempotencyRepository = idempotencyRepository;
        this.responseCodec = responseCodec;
    }

    @Transactional
    public CouponCancelUseResponse execute(
            CouponCancelUseCommand command,
            Instant claimedAt
    ) {
        CouponCancelUseResult result = cancelUseService.cancelUse(command);
        CouponCancelUseResponse response = CouponCancelUseResponse.from(
                result
        );
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
