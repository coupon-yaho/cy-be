package com.kafkick.api.observation.issuance;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import com.kafkick.core.observation.EventRecorder;
import com.kafkick.core.observation.IssuanceFlowEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Delivers one issuance event to each independent observation sink. */
public final class CompositeEventRecorder implements EventRecorder {

    private static final Logger log = LoggerFactory.getLogger(CompositeEventRecorder.class);
    private static final long FAILURE_LOG_INTERVAL_NANOS =
            IssuanceObservationService.DEFAULT_LOG_INTERVAL.toNanos();

    private final List<EventRecorder> delegates;
    private final AtomicLong failures = new AtomicLong();
    private final AtomicLong nextFailureLogAtNanos = new AtomicLong(System.nanoTime());

    public CompositeEventRecorder(EventRecorder... delegates) {
        this.delegates = Arrays.stream(Objects.requireNonNull(delegates, "delegates"))
                .map(delegate -> Objects.requireNonNull(delegate, "delegate"))
                .toList();
        if (this.delegates.isEmpty()) {
            throw new IllegalArgumentException("delegates must not be empty");
        }
    }

    @Override
    public void record(IssuanceFlowEvent event) {
        for (EventRecorder delegate : delegates) {
            try {
                delegate.record(event);
            } catch (RuntimeException exception) {
                logFailureAtMostOncePerInterval(delegate, exception);
            }
        }
    }

    private void logFailureAtMostOncePerInterval(EventRecorder delegate, RuntimeException exception) {
        long total = failures.incrementAndGet();
        long now = System.nanoTime();
        long due = nextFailureLogAtNanos.get();
        if (now - due < 0 || !nextFailureLogAtNanos.compareAndSet(
                due, now + FAILURE_LOG_INTERVAL_NANOS)) {
            return;
        }
        log.warn("발급 관측 전달에 실패했습니다. 다른 관측기는 계속 기록합니다. "
                        + "누적 {}건, recorder={}, cause={}",
                total, delegate.getClass().getName(), exception.getClass().getSimpleName());
    }
}
