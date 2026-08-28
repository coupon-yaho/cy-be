package com.kafkick.api.observation.issuance;

import java.time.Duration;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 관측 기록 실패를 세고, 같은 실패를 다시 로그로 남길 차례인지 판단한다.
 *
 * <p>부하 회차에는 로그 자체가 측정을 오염시키므로 간격을 늘려야 하고 디버깅 중에는 줄여야 한다.
 * 그래서 간격은 상수가 아니라 {@code observation.issuance.attempt-failure-log-interval} 에서 온다.
 */
final class FailureLogThrottle {

    private final long intervalNanos;
    private final AtomicLong failures = new AtomicLong();
    private final AtomicLong nextLogAtNanos = new AtomicLong(System.nanoTime());

    FailureLogThrottle(Duration interval) {
        Objects.requireNonNull(interval, "failureLogInterval");
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("failureLogInterval must be positive");
        }
        this.intervalNanos = interval.toNanos();
    }

    /**
     * 실패 한 건을 누적하고, 지금이 로그를 남길 차례면 누적 건수를 돌려준다.
     *
     * @return 로그를 남길 차례면 누적 실패 건수, 아니면 {@link OptionalLong#empty()}
     */
    OptionalLong recordFailure() {
        long total = failures.incrementAndGet();
        long now = System.nanoTime();
        long due = nextLogAtNanos.get();
        if (now - due < 0 || !nextLogAtNanos.compareAndSet(due, now + intervalNanos)) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(total);
    }
}
