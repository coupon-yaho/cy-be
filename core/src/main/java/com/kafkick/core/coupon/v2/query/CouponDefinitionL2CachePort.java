package com.kafkick.core.coupon.v2.query;

import java.time.Duration;
import java.util.Optional;

/**
 * 여러 API 인스턴스가 공유하는 정의 목록 캐시와, 그 miss 를 하나로 합치는 로드 권한이다.
 *
 * <p><b>구현은 절대 예외를 밖으로 내지 않는다.</b> L2 는 DB 부하를 줄이는 보조 계층이지 조회
 * 경로의 새 필수 의존성이 아니다. Redis 가 죽었다고 목록이 503 이 되면, L2 를 넣기 전보다
 * 가용성이 나빠진다 — 실패는 "L2 가 없는 것"과 같게 보이고 호출자는 DB 로 간다.
 */
public interface CouponDefinitionL2CachePort {

    /** 공유 캐시의 현재 값. 없거나 읽지 못하면 empty. */
    Optional<CouponDefinitionSnapshot> find();

    /** 공유 캐시를 갱신한다. 실패는 삼킨다. */
    void put(CouponDefinitionSnapshot snapshot, Duration ttl);

    /**
     * DB 로 내려갈 권한을 얻는다. 다른 인스턴스가 이미 로드 중이면 empty.
     *
     * <p>{@code lease} 는 권한을 쥔 인스턴스가 죽었을 때의 상한이다 — 없으면 그 회차 동안
     * 아무도 로드하지 못한다.
     */
    Optional<String> tryAcquireLoad(Duration lease);

    /** 자기가 얻은 권한만 반납한다. 토큰이 다르면 아무것도 하지 않는다. */
    void releaseLoad(String token);
}
