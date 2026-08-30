package com.kafkick.infra.redis.coupon.v2;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 발급 게이트 차단기의 임계치. 부하 중 재기동 없이 바꿀 수 있게 전부 외부화한다.
 *
 * <p><b>잘못된 값은 바인딩 단계에서 기동을 멈춘다</b>({@link IllegalArgumentException}).
 * 조용히 기본값으로 되돌리면 측정 회차가 의도한 임계치가 아닌 값으로 돌고, 그 사실이
 * 결과에 드러나지 않는다.
 *
 * <ul>
 *   <li>{@code sliding-window-size}·{@code minimum-number-of-calls} — <b>양의 정수</b></li>
 *   <li>{@code failure-rate-threshold} — <b>0 초과 100 이하</b></li>
 *   <li>{@code wait-duration-in-open-state} — <b>0 보다 큰 기간</b>(null·0·음수 거절)</li>
 * </ul>
 *
 * <p>{@code minimum-number-of-calls > sliding-window-size} 조합은 값 하나만으로는 판정할 수
 * 없어 조립부({@code IssuanceGateRedisAutoConfiguration})가 막는다.
 */
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
