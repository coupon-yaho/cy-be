package com.kafkick.api.observation.http;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import com.kafkick.api.observation.MeterNames;

/** 프로세스 안에서 처리 중인 HTTP 요청 수를 보관한다. */
@Component
public final class InFlightRegistry {

    private final AtomicInteger current = new AtomicInteger();

    public InFlightRegistry(MeterRegistry meterRegistry) {
        Gauge.builder(MeterNames.IN_FLIGHT, current, AtomicInteger::get)
                .description("Non-admin HTTP requests executing, including requests without a route")
                .strongReference(true)
                .register(meterRegistry);
    }

    public InFlightToken enter() {
        current.incrementAndGet();
        return new InFlightToken(this);
    }

    private void leave() {
        current.decrementAndGet();
    }

    public static final class InFlightToken implements AutoCloseable {

        private final InFlightRegistry owner;
        private final AtomicBoolean closed = new AtomicBoolean();

        private InFlightToken(InFlightRegistry owner) {
            this.owner = owner;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                owner.leave();
            }
        }
    }
}
