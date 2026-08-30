package com.kafkick.infra.redis.lifecycle;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;

import com.kafkick.core.observation.CouponRoundClosedEvent;
import com.kafkick.core.observation.CouponRoundLifecycleRecorder;

import tools.jackson.databind.ObjectMapper;

public final class RedisCouponRoundClosedEventSubscriber
        implements MessageListener {

    private static final long LOG_INTERVAL_NANOS =
            TimeUnit.SECONDS.toNanos(10);

    private static final Logger log = LoggerFactory.getLogger(
            RedisCouponRoundClosedEventSubscriber.class
    );

    private final ObjectMapper objectMapper;
    private final CouponRoundLifecycleRecorder recorder;
    private final AtomicLong failureCount = new AtomicLong();
    private final AtomicLong nextLogAtNanos = new AtomicLong();

    public RedisCouponRoundClosedEventSubscriber(
            ObjectMapper objectMapper,
            CouponRoundLifecycleRecorder recorder
    ) {
        this.objectMapper = Objects.requireNonNull(objectMapper);
        this.recorder = Objects.requireNonNull(recorder);
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        try {
            CouponRoundClosedEvent event = objectMapper.readValue(
                    message.getBody(),
                    CouponRoundClosedEvent.class
            );
            recorder.retireCouponRound(
                    event.couponId(),
                    event.closedAt()
            );
        } catch (Exception exception) {
            logFailure(exception);
        }
    }

    private void logFailure(Exception exception) {
        long count = failureCount.incrementAndGet();
        long now = System.nanoTime();
        long due = nextLogAtNanos.get();
        boolean logDue = now - due >= 0
                && nextLogAtNanos.compareAndSet(
                        due,
                        now + LOG_INTERVAL_NANOS
                );
        if (logDue) {
            log.warn(
                    "쿠폰 회차 종료 Redis 수신 처리 실패 누적 {}건. cause={}",
                    count,
                    exception.toString()
            );
        }
    }
}
