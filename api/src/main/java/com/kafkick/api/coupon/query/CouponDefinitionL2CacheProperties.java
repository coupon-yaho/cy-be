package com.kafkick.api.coupon.query;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 인스턴스 사이에서 공유하는 정의 캐시(L2)의 예산이다.
 *
 * <p>{@code waitTimeout} 은 <b>L1 의 load-timeout 보다 작아야 한다.</b> 로드 권한을 못 얻은
 * 인스턴스는 그 시간만큼 L2 를 폴링하는데, 그 대기가 호출자 예산보다 길면 대기가 끝나기 전에
 * 호출자가 전부 물러난다 — 락을 두고도 herd 가 그대로 DB 로 간다. 두 값의 관계는 조립할 때
 * 검사한다({@code CouponDefinitionL1CacheConfiguration}).
 */
@ConfigurationProperties(prefix = "coupon.definition-cache.l2")
public record CouponDefinitionL2CacheProperties(
        Duration ttl,
        Duration lockLease,
        Duration waitTimeout,
        Duration pollInterval
) {
    public CouponDefinitionL2CacheProperties {
        ttl = ttl == null ? Duration.ofSeconds(10) : ttl;
        lockLease = lockLease == null ? Duration.ofSeconds(3) : lockLease;
        waitTimeout = waitTimeout == null ? Duration.ofMillis(60) : waitTimeout;
        pollInterval = pollInterval == null ? Duration.ofMillis(10) : pollInterval;
        if (ttl.isNegative() || ttl.isZero()
                || lockLease.isNegative() || lockLease.isZero()
                || waitTimeout.isNegative()
                || pollInterval.isNegative() || pollInterval.isZero()) {
            throw new IllegalArgumentException("쿠폰 정의 L2 설정이 유효하지 않습니다.");
        }
        if (pollInterval.compareTo(waitTimeout) > 0) {
            throw new IllegalArgumentException(
                    "쿠폰 정의 L2 poll-interval 은 wait-timeout 이하여야 합니다.");
        }
    }
}
