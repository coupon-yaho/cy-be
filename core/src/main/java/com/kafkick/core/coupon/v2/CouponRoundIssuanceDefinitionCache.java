package com.kafkick.core.coupon.v2;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.v2.port.CouponRoundIssuanceDefinitionRepository;
import com.kafkick.core.support.exception.BusinessException;

/** 오픈 중 불변인 회차 정의를 API 인스턴스 안에서 한 번만 읽는다. */
@Component
public final class CouponRoundIssuanceDefinitionCache {

    private final CouponRoundIssuanceDefinitionRepository repository;
    private final ConcurrentHashMap<Long, CompletableFuture<CouponRoundIssuanceDefinition>>
            definitions = new ConcurrentHashMap<>();

    public CouponRoundIssuanceDefinitionCache(
            CouponRoundIssuanceDefinitionRepository repository
    ) {
        this.repository = Objects.requireNonNull(repository, "repository");
    }

    public CouponRoundIssuanceDefinition get(long couponRoundId) {
        if (couponRoundId <= 0) {
            throw new IllegalArgumentException("couponRoundId는 0보다 커야 합니다.");
        }
        // 로더 판정과 맵 등재를 한 CAS 로 끝낸다. 둘을 나누면 그 사이에 들어온 스레드가
        // 아무도 완료시키지 않는 future 를 잡고 무기한 대기한다 — join 은 상한도 인터럽트도
        // 없어 그 톰캣 스레드는 회수되지 않는다.
        CompletableFuture<CouponRoundIssuanceDefinition> mine = new CompletableFuture<>();
        CompletableFuture<CouponRoundIssuanceDefinition> existing =
                definitions.putIfAbsent(couponRoundId, mine);
        if (existing != null) {
            return join(existing);
        }
        try {
            mine.complete(load(couponRoundId));
        } catch (RuntimeException | Error loadFailure) {
            // 실패는 캐시에 남기지 않는다. 다음 요청이 다시 로드한다.
            definitions.remove(couponRoundId, mine);
            mine.completeExceptionally(loadFailure);
            throw loadFailure;
        } finally {
            // 어떤 경로로도 미완료 future 를 맵에 남기지 않는다.
            if (!mine.isDone()) {
                definitions.remove(couponRoundId, mine);
                mine.completeExceptionally(new IllegalStateException(
                        "회차 정의 로드가 완료되지 않았습니다."));
            }
        }
        return join(mine);
    }

    private static CouponRoundIssuanceDefinition join(
            CompletableFuture<CouponRoundIssuanceDefinition> pending
    ) {
        try {
            return pending.join();
        } catch (CompletionException joinFailure) {
            Throwable cause = joinFailure.getCause();
            throw cause instanceof RuntimeException runtime ? runtime : joinFailure;
        }
    }

    private CouponRoundIssuanceDefinition load(long couponRoundId) {
        return repository.lockAndFindById(couponRoundId)
                .orElseThrow(() -> new BusinessException(
                        CouponIssueErrorCode.COUPON_ROUND_NOT_FOUND,
                        "couponRoundId=" + couponRoundId));
    }
}
