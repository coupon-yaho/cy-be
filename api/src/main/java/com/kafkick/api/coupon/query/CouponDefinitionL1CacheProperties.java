package com.kafkick.api.coupon.query;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 인스턴스 안 정의 캐시(L1)의 예산이다.
 *
 * <p><b>아래 값을 어기면 설정 바인딩 시점에 {@link IllegalArgumentException} 으로 기동이
 * 실패한다.</b> 운영 설정 오류를 살려 두면 캐시가 조용히 무의미해지고, 그 사실은 부하 시험
 * 뒤 DB 질의 수를 셀 때에야 드러난다.
 *
 * <ul>
 *   <li>{@code ttl} · {@code load-timeout} · {@code stale-max-age} 는 <b>0 이나 음수일 수 없다</b>
 *       — 0 이면 매 요청이 miss 이거나 즉시 물러난다.</li>
 *   <li>{@code maximum-size} 는 <b>1 이상</b>이어야 한다 — 0 이면 캐시가 아무것도 안 담는다.</li>
 *   <li>{@code stale-max-age} 는 <b>{@code ttl} 이상</b>이어야 한다 — 더 짧으면 신선한 값이
 *       살아 있는 동안 완충용 stale 이 먼저 사라져, 장애 때 되돌려 줄 값이 없다.</li>
 * </ul>
 *
 * <p>{@code load-timeout} 과 L2 대기 예산의 관계는 {@link CouponDefinitionL2CacheProperties}
 * 에 적어 두었고, 그쪽은 조립 시점에 검사한다.
 */
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
