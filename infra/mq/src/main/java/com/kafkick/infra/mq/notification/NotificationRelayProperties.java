package com.kafkick.infra.mq.notification;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("kafka.notification.relay")
public class NotificationRelayProperties {
    private Duration lease = Duration.ofSeconds(30);
    private long fixedDelayMs = 100L;

    /**
     * Full Jitter 의 기본 간격. 첫 재시도 상한이 {@code base × 2} 다.
     *
     * <p>200ms 인 이유 — 이보다 짧으면 실패한 것들이 사실상 즉시 다시 와 지터가 무의미하고,
     * 길면 정상 회복이 느려진다. 상한({@link #backoffCap})이 폭주를 막으므로 여기는 짧게 둔다.
     */
    private Duration backoffBase = Duration.ofMillis(200);

    /**
     * Full Jitter 의 지연 상한.
     *
     * <p><b>이 값은 혼자 정해지지 않는다.</b> {@code failure_count} 상한이 10 이므로 최악의
     * 재시도 사슬은 {@code 10 × cap} 이다. 소비하는 쪽에 "이 시각까지 종착 상태여야 한다" 는
     * 마감이 있으면 그 곱이 마감을 넘으면 안 된다 — 넘으면 아직 재시도 중인 것을 마감 쪽이
     * 먼저 실패로 닫고, 뒤늦게 성공한 결과가 갈 곳을 잃는다.
     *
     * <p>이 저장소에는 아직 그 마감이 없다(알림은 종착 마감이 없다). 20초로 두는 것은
     * <b>그 제약이 생겼을 때 곱이 200초라 대부분의 마감 아래</b>이기 때문이고,
     * 마감을 두는 쪽을 만들 때 이 관계를 기동 시 검증해야 한다.
     */
    private Duration backoffCap = Duration.ofSeconds(20);

    public Duration getBackoffBase() {
        return backoffBase;
    }

    public void setBackoffBase(Duration backoffBase) {
        this.backoffBase = requirePositive(backoffBase, "backoff base");
    }

    public Duration getBackoffCap() {
        return backoffCap;
    }

    public void setBackoffCap(Duration backoffCap) {
        this.backoffCap = requirePositive(backoffCap, "backoff cap");
    }

    private static Duration requirePositive(Duration value, String name) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + "는 양수여야 합니다.");
        }
        return value;
    }

    public Duration getLease() {
        return lease;
    }

    public void setLease(Duration lease) {
        if (lease == null || lease.isZero() || lease.isNegative()) {
            throw new IllegalArgumentException("relay lease는 양수여야 합니다.");
        }
        this.lease = lease;
    }

    public long getFixedDelayMs() {
        return fixedDelayMs;
    }

    public void setFixedDelayMs(long fixedDelayMs) {
        if (fixedDelayMs < 1) {
            throw new IllegalArgumentException("relay fixed delay는 양수여야 합니다.");
        }
        this.fixedDelayMs = fixedDelayMs;
    }
}
