package com.kafkick.infra.redis.coupon.v2;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("coupon.v2.redis-circuit-breaker")
public class RedisCircuitBreakerProperties {
    private int slidingWindowSize = 5;
    private int minimumNumberOfCalls = 5;
    private float failureRateThreshold = 100;
    private Duration waitDurationInOpenState = Duration.ofSeconds(1);
    public int getSlidingWindowSize() { return slidingWindowSize; }
    public void setSlidingWindowSize(int value) {
        requirePositive(value, "slidingWindowSize");
        slidingWindowSize = value;
    }
    public int getMinimumNumberOfCalls() { return minimumNumberOfCalls; }
    public void setMinimumNumberOfCalls(int value) {
        requirePositive(value, "minimumNumberOfCalls");
        minimumNumberOfCalls = value;
    }
    public float getFailureRateThreshold() { return failureRateThreshold; }
    public void setFailureRateThreshold(float value) {
        requirePercent(value, "failureRateThreshold");
        failureRateThreshold = value;
    }
    public Duration getWaitDurationInOpenState() { return waitDurationInOpenState; }
    public void setWaitDurationInOpenState(Duration value) {
        requirePositive(value, "waitDurationInOpenState");
        waitDurationInOpenState = value;
    }
    private static void requirePositive(int value, String name) {
        if (value <= 0) throw new IllegalArgumentException(name + "는 양수여야 합니다.");
    }

    private static void requirePositive(Duration value, String name) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + "는 양수여야 합니다.");
        }
    }

    private static void requirePercent(float value, String name) {
        if (value <= 0 || value > 100) {
            throw new IllegalArgumentException(name + "는 0 초과 100 이하여야 합니다.");
        }
    }
}
