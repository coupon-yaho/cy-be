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
 *
 * <p><b>{@code lockLease} 는 최악의 로드 시간보다 길어야 한다.</b> 짧으면 lease 가 먼저 끝나고,
 * 뒤늦게 끝난 로더가 그 사이 다른 인스턴스가 올린 새 값을 덮어쓴다. 근거는 storage.yml 의
 * 실제 값이다 — Hikari {@code connection-timeout} 3000ms(커넥션 대기 상한)에 정의 질의의
 * {@code jakarta.persistence.query.timeout} 300ms 를 더한 3.3초가 로드의 상한이다. 기본값
 * 5초는 거기에 여유를 둔 값이다. <b>storage.yml 의 그 두 값을 바꾸면 여기도 같이 본다.</b>
 *
 * <p><b>아래 값을 어기면 설정 바인딩 시점에 {@link IllegalArgumentException} 으로 기동이
 * 실패한다.</b>
 *
 * <ul>
 *   <li>{@code ttl} · {@code lock-lease} · {@code poll-interval} 은 <b>0 이나 음수일 수 없다</b>
 *       — {@code poll-interval} 이 0 이면 대기가 바쁜 루프가 된다.</li>
 *   <li>{@code wait-timeout} 은 <b>음수일 수 없다</b>. 0 은 "기다리지 않는다" 는 뜻으로 허용한다.</li>
 *   <li>{@code poll-interval} 은 <b>{@code wait-timeout} 이하</b>여야 한다 — 더 길면 한 번도
 *       못 보고 대기가 끝나, 기다린 시간이 통째로 버려진다.</li>
 * </ul>
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
        lockLease = lockLease == null ? Duration.ofSeconds(5) : lockLease;
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
