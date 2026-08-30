package com.kafkick.infra.redis.lifecycle;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.kafkick.core.observation.CouponRoundClosedEvent;

import tools.jackson.databind.ObjectMapper;

public class RedisCouponRoundClosedEventPublisher {

    private static final long LOG_INTERVAL_NANOS =
            TimeUnit.SECONDS.toNanos(10);

    private static final Logger log = LoggerFactory.getLogger(
            RedisCouponRoundClosedEventPublisher.class
    );

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final String channel;
    private final AtomicLong failureCount = new AtomicLong();
    private final AtomicLong nextLogAtNanos = new AtomicLong();

    public RedisCouponRoundClosedEventPublisher(
            StringRedisTemplate redis,
            ObjectMapper objectMapper,
            String channel
    ) {
        this.redis = Objects.requireNonNull(redis);
        this.objectMapper = Objects.requireNonNull(objectMapper);
        if (channel == null || channel.isBlank()) {
            throw new IllegalArgumentException(
                    "쿠폰 회차 종료 Redis 채널은 빈 값일 수 없습니다."
            );
        }
        this.channel = channel;
    }

    @EventListener
    public void publish(CouponRoundClosedEvent event) {
        Objects.requireNonNull(event, "event");
        try {
            String payload = objectMapper.writeValueAsString(event);
            redis.convertAndSend(channel, payload);
        } catch (Exception exception) {
            logFailure(event, exception);
        }
    }

    private void logFailure(
            CouponRoundClosedEvent event,
            Exception exception
    ) {
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
                    "쿠폰 회차 종료 Redis 발행 실패 누적 {}건. couponId={}, cause={}",
                    count,
                    event.couponId(),
                    exception.toString()
            );
        }
    }
}
