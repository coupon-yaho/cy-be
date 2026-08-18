// 쿠폰 상태·사용 실적·이력과 멱등 완료 응답을 하나의 원자적 트랜잭션으로 저장합니다.
package com.kafkick.api.coupon.adapter;

import java.time.Instant;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.api.coupon.dto.CouponUseResponse;
import com.kafkick.core.coupon.port.IdempotencyRepository;
import com.kafkick.core.coupon.service.CouponUseCommand;
import com.kafkick.core.coupon.service.CouponUseResult;
import com.kafkick.core.coupon.service.CouponUseService;

@Component
public class CouponUseTransactionExecutor {

    private final CouponUseService couponUseService;
    private final IdempotencyRepository idempotencyRepository;
    private final CouponUseResponseCodec responseCodec;

    public CouponUseTransactionExecutor(
            CouponUseService couponUseService,
            IdempotencyRepository idempotencyRepository,
            CouponUseResponseCodec responseCodec
    ) {
        this.couponUseService = couponUseService;
        this.idempotencyRepository = idempotencyRepository;
        this.responseCodec = responseCodec;
    }

    @Transactional
    public CouponUseResponse execute(
            CouponUseCommand command,
            Instant claimedAt
    ) {
        CouponUseResult result = couponUseService.use(command);
        CouponUseResponse response = CouponUseResponse.from(result);
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
