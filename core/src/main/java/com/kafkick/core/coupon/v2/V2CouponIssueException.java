package com.kafkick.core.coupon.v2;

import java.util.Objects;
import java.util.Optional;

import com.kafkick.core.coupon.v2.port.CompensateOutcome;
import com.kafkick.core.observation.Dependency;

/**
 * 원 실패와 보상 CAS 결과를 함께 보존해 S5가 이상 결과를 분류할 수 있게 한다.
 *
 * <p>실패한 의존성도 함께 싣는다. api 는 게이트 어댑터를 컴파일 타임에 보지 못해
 * Redis 기술 예외를 타입으로 알아볼 수 없고, {@code DataAccessException} 만 보고
 * 판정하면 Redis 장애가 MySQL 장애로 집계된다 — Chaos 판정의 귀속이 뒤바뀐다.
 */
public final class V2CouponIssueException extends RuntimeException {

    private final CompensateOutcome nullableCompensateOutcome;
    private final Dependency dependency;
    private final boolean claimFailedBeforeResult;

    public V2CouponIssueException(
            RuntimeException cause,
            CompensateOutcome nullableCompensateOutcome,
            Dependency dependency
    ) {
        this(cause, nullableCompensateOutcome, dependency, false);
    }

    public V2CouponIssueException(
            RuntimeException cause,
            CompensateOutcome nullableCompensateOutcome,
            Dependency dependency,
            boolean claimFailedBeforeResult
    ) {
        super(cause.getMessage(), cause);
        this.nullableCompensateOutcome = nullableCompensateOutcome;
        this.dependency = Objects.requireNonNull(dependency, "dependency");
        this.claimFailedBeforeResult = claimFailedBeforeResult;
    }

    /** 실패를 일으킨 직접 의존성. 게이트 호출이면 REDIS, 발급 트랜잭션이면 MYSQL. */
    public Dependency dependency() {
        return dependency;
    }

    public Optional<CompensateOutcome> compensateOutcome() {
        return Optional.ofNullable(nullableCompensateOutcome);
    }

    /** true면 선점 결과조차 받지 못한 통신·차단기 실패다. */
    public boolean claimFailedBeforeResult() {
        return claimFailedBeforeResult;
    }
}
