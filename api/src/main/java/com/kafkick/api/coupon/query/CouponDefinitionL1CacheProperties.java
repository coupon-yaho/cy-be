package com.kafkick.api.coupon.query;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "coupon.definition-cache.l1")
public record CouponDefinitionL1CacheProperties(
        Duration ttl,
        Duration loadTimeout,
        Duration staleMaxAge,
        Long maximumSize
) {
    public CouponDefinitionL1CacheProperties {
        ttl = ttl == null ? Duration.ofSeconds(10) : ttl;
        loadTimeout = loadTimeout == null ? Duration.ofMillis(100) : loadTimeout;
        staleMaxAge = staleMaxAge == null ? Duration.ofSeconds(60) : staleMaxAge;
        maximumSize = maximumSize == null ? 1 : maximumSize;
        if (ttl.isNegative() || ttl.isZero() || loadTimeout.isNegative() || loadTimeout.isZero()
                || staleMaxAge.isNegative() || staleMaxAge.isZero() || maximumSize <= 0) {
            throw new IllegalArgumentException("쿠폰 정의 L1 설정이 유효하지 않습니다.");
        }
        if (staleMaxAge.compareTo(ttl) < 0) {
            throw new IllegalArgumentException(
                    "쿠폰 정의 L1 stale-max-age 는 ttl 이상이어야 합니다.");
        }
    }
}
