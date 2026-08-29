package com.kafkick.core.coupon.v2.query;

import java.time.Duration;
import java.util.Optional;

/**
 * Redis 통로가 없는 배포에서 쓰는 L2 다. 항상 miss 이고 로드 권한은 항상 자기 것이다.
 *
 * <p>이 빈이 존재하는 이유는 소비자가 <b>무조건 뜨는 빈</b>이기 때문이다. 공급자만 조건부로
 * 두면 Redis 없는 회차에서 조회 경로가 기동에서 죽는다 — 그 회차의 올바른 동작은 "L1 만으로
 * 돈다" 이지 "앱이 안 뜬다" 가 아니다.
 */
public final class DisabledCouponDefinitionL2Cache implements CouponDefinitionL2CachePort {

    private static final String TOKEN = "disabled";

    @Override
    public Optional<CouponDefinitionSnapshot> find() {
        return Optional.empty();
    }

    @Override
    public void put(CouponDefinitionSnapshot snapshot, Duration ttl) {
    }

    @Override
    public Optional<String> tryAcquireLoad(Duration lease) {
        // 공유 락이 없으면 인스턴스마다 자기 로드를 한다. L1 single-flight 는 그대로 산다.
        return Optional.of(TOKEN);
    }

    @Override
    public void releaseLoad(String token) {
    }
}
