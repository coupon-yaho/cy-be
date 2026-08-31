package com.kafkick.api.coupon.service;

import java.util.Objects;

import org.springframework.stereotype.Service;

import com.kafkick.api.support.lock.LockContentionRetry;
import com.kafkick.api.support.lock.LockRetryOperations;
import com.kafkick.core.coupon.service.CouponOperationExecutionService;
import com.kafkick.core.coupon.service.result.CouponCancelResult;
import com.kafkick.core.coupon.service.result.CouponCancelUseResult;
import com.kafkick.core.coupon.service.result.CouponUseResult;

/**
 * 사용·사용취소·발급취소를 락 경합 재시도로 감싼다.
 *
 * <h2>왜 컨트롤러가 아니라 여기인가</h2>
 *
 * <p>세 컨트롤러가 각자 재시도를 감으면 같은 코드가 셋이 되고, 나중에 한 곳만 고쳐 두는
 * 자리가 된다. 감싸는 층은 하나여야 한다.
 *
 * <h2>왜 core 가 아니라 api 인가</h2>
 *
 * <p>둘이다. 하나, 판별 기준인 {@code PessimisticLockingFailureException} 은
 * {@code spring-tx} 타입이라 도메인 층에 들이지 않는다. 둘, <b>재시도는 트랜잭션 바깥에
 * 있어야 한다</b> — 데드락 피해자로 뽑힌 트랜잭션은 이미 되감겼으므로 그 안에서 다시
 * 해 봐야 소용이 없다.
 *
 * <h2>발급이 여기 없는 이유</h2>
 *
 * <p>발급은 {@code CouponIssueObservationCoordinator} 가 관측 범위를 열고 그 안에서
 * 재시도한다. 재시도한 시도까지 한 요청으로 세어야 해서 감싸는 자리가 다르다. 판별과
 * 물러서는 방식은 {@link LockContentionRetry} 로 같은 것을 쓴다.
 *
 * <h2>같은 요청을 다시 해도 되는 근거</h2>
 *
 * <p>세 경로의 멱등 실행기는 {@code @Transactional(propagation = NEVER)} 이고 선점만
 * {@code REQUIRES_NEW} 로 따로 커밋한다. 언뜻 <b>실패한 시도가 남긴 선점에 다음 시도가
 * 막힐 것 같은데, 재 보니 아니었다</b> — 처리가 예외로 끝나면
 * {@code IdempotencyExecutionService} 가 그 선점을 풀어 준다. 그래서 다음 시도가 처음처럼
 * 선점한다.
 *
 * <p>푸는 것까지 실패하면(그 예외는 원래 예외에 suppressed 로 붙는다) 선점이 남고 다음
 * 시도는 {@code CONFLICT_IN_PROGRESS} 를 받는다. 그 경우는 재시도가 아니라
 * {@code stale-after} 회수가 처리한다.
 */
@Service
public class CouponOperationRetryingExecutor {

    private final CouponOperationExecutionService delegate;
    private final LockContentionRetry lockContentionRetry;

    public CouponOperationRetryingExecutor(
            CouponOperationExecutionService delegate,
            LockContentionRetry lockContentionRetry
    ) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.lockContentionRetry = Objects.requireNonNull(lockContentionRetry, "lockContentionRetry");
    }

    public CouponUseResult use(
            Long issuanceId,
            Long memberId,
            int orderAmount,
            String idempotencyKey
    ) {
        return lockContentionRetry.call(
                LockRetryOperations.USE,
                () -> context(issuanceId, memberId),
                () -> delegate.use(issuanceId, memberId, orderAmount, idempotencyKey)
        );
    }

    public CouponCancelUseResult cancelUse(
            Long issuanceId,
            Long memberId,
            String idempotencyKey
    ) {
        return lockContentionRetry.call(
                LockRetryOperations.CANCEL_USE,
                () -> context(issuanceId, memberId),
                () -> delegate.cancelUse(issuanceId, memberId, idempotencyKey)
        );
    }

    public CouponCancelResult cancel(
            Long issuanceId,
            Long memberId,
            String idempotencyKey
    ) {
        return lockContentionRetry.call(
                LockRetryOperations.CANCEL,
                () -> context(issuanceId, memberId),
                () -> delegate.cancel(issuanceId, memberId, idempotencyKey)
        );
    }

    /**
     * 로그에 남길 맥락. <b>식별자까지만 남긴다</b> — 이름·연락처는 넣지 않는다.
     * 멱등키도 넣지 않는다. 호출자가 만든 값이라 무엇이 들어 있을지 모른다.
     */
    private static String context(Long issuanceId, Long memberId) {
        return "issuanceId=%d memberId=%d".formatted(issuanceId, memberId);
    }
}
