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
 * <p><b>{@code lockLease} 는 {@code maxLoadTime} 보다 길어야 하고, 이 관계는 검증한다.</b>
 * 짧으면 lease 가 먼저 끝나고, 뒤늦게 끝난 로더가 그 사이 다른 인스턴스가 올린 새 값을 덮어쓴다.
 * 문서로만 두면 환경변수로 낮춘 배포에서 그 약속이 조용히 깨진다.
 *
 * <p>{@code maxLoadTime} 은 <b>권한을 쥐고 있는 구간 전체</b>의 상한을 이름 붙여 드러낸
 * 가정이다. 그 구간은 DB 질의에서 끝나지 않는다 — 권한은 질의 전에 얻고, <b>L2 게시가 끝난
 * 뒤에야</b> {@code finally} 에서 반납된다. 그래서 Redis 왕복도 이 값에 든다.
 *
 * <pre>
 *   tryAcquireLoad ─┬─ Hikari connection-timeout   3000ms  (storage.yml)
 *                   ├─ 정의 질의 query.timeout       300ms  (CouponRoundJpaRepository)
 *                   ├─ Redis connect-timeout       1000ms  (redis.yml)
 *                   └─ Redis command timeout        500ms  (redis.yml)
 *   releaseLoad                                  = 4800ms
 * </pre>
 *
 * <p>네 값이 세 파일에 흩어져 있어 한쪽만 바뀌면 관계가 조용히 깨지므로,
 * {@code CouponDefinitionL2LeaseBudgetTest} 가 그 파일들을 직접 읽어 기본값을 고정한다.
 *
 * <p><b>아래 값을 어기면 설정 바인딩 시점에 {@link IllegalArgumentException} 으로 기동이
 * 실패한다.</b>
 *
 * <ul>
 *   <li>{@code ttl} · {@code lock-lease} · {@code poll-interval} 은 <b>0 이나 음수일 수 없다</b>
 *       — {@code poll-interval} 이 0 이면 대기가 바쁜 루프가 된다.</li>
 *   <li>{@code wait-timeout} 은 <b>{@code poll-interval} 이상</b>이어야 한다. 따라서 최소 1ms 다 —
 *       기다리지 않으려면 이 값을 줄이는 것이 아니라 L2 자체를 쓰지 않는다.</li>
 *   <li>{@code poll-interval} 은 <b>{@code wait-timeout} 이하</b>여야 한다 — 더 길면 한 번도
 *       못 보고 대기가 끝나, 기다린 시간이 통째로 버려진다.</li>
 *   <li>{@code poll-interval} 은 <b>1ms 이상</b>이어야 한다 — 대기가 밀리초로 절삭되므로
 *       그보다 짧은 값은 {@code sleep(0)} 이 되어 대기 내내 Redis 를 바쁜 루프로 조회한다.</li>
 *   <li>{@code lock-lease} 는 <b>{@code max-load-time} 보다 길어야</b> 한다.</li>
 * </ul>
 */
@ConfigurationProperties(prefix = "coupon.definition-cache.l2")
public record CouponDefinitionL2CacheProperties(
        Duration ttl,
        Duration lockLease,
        Duration waitTimeout,
        Duration pollInterval,
        Duration maxLoadTime
) {
    public CouponDefinitionL2CacheProperties {
        ttl = ttl == null ? Duration.ofSeconds(10) : ttl;
        lockLease = lockLease == null ? Duration.ofSeconds(6) : lockLease;
        waitTimeout = waitTimeout == null ? Duration.ofMillis(60) : waitTimeout;
        pollInterval = pollInterval == null ? Duration.ofMillis(10) : pollInterval;
        maxLoadTime = maxLoadTime == null ? Duration.ofMillis(4_800) : maxLoadTime;
        if (ttl.isNegative() || ttl.isZero()
                || lockLease.isNegative() || lockLease.isZero()
                || waitTimeout.isNegative()
                || pollInterval.isNegative() || pollInterval.isZero()
                || maxLoadTime.isNegative() || maxLoadTime.isZero()) {
            throw new IllegalArgumentException("쿠폰 정의 L2 설정이 유효하지 않습니다.");
        }
        // 대기는 밀리초로 절삭된다. 1ms 미만은 sleep(0) 이 되어 대기 내내 바쁜 루프가 된다.
        if (pollInterval.toMillis() < 1) {
            throw new IllegalArgumentException(
                    "쿠폰 정의 L2 poll-interval 은 1ms 이상이어야 합니다.");
        }
        if (lockLease.compareTo(maxLoadTime) <= 0) {
            throw new IllegalArgumentException(
                    "쿠폰 정의 L2 lock-lease(" + lockLease + ") 는 max-load-time("
                            + maxLoadTime + ") 보다 길어야 합니다.");
        }
        if (pollInterval.compareTo(waitTimeout) > 0) {
            throw new IllegalArgumentException(
                    "쿠폰 정의 L2 poll-interval 은 wait-timeout 이하여야 합니다.");
        }
    }
}
