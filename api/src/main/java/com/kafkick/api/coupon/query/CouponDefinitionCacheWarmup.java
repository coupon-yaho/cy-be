package com.kafkick.api.coupon.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import com.kafkick.core.support.TimeProvider;

/**
 * 기동 직후 정의 목록을 한 번 채운다.
 *
 * <p>없으면 콜드 스타트에서 목록이 통째로 503 이다 — 폴백은 stale 하나뿐인데, stale 은 성공한
 * 로드가 한 번 있어야 생긴다. 첫 로드는 커넥션 생성·콜드 버퍼풀이 겹쳐 호출자 예산(100ms)을
 * 넘기 쉽고, 그 사이 도착한 요청은 되돌려 줄 값이 없다. 오픈 정각에 램프업하는 회차에서는
 * 그 구간이 그대로 실패율이 된다.
 *
 * <p><b>실패해도 기동은 계속한다.</b> DB 가 아직 안 떠 있는 배포 순서에서 앱이 죽으면, 준비가
 * 끝난 뒤에도 아무도 서비스하지 않는다. 그 경우의 올바른 동작은 첫 요청이 스스로 로드하는 것이다.
 */
@Component
public class CouponDefinitionCacheWarmup {

    private static final Logger log = LoggerFactory.getLogger(CouponDefinitionCacheWarmup.class);

    private final V2IssuableCouponRoundQuery query;
    private final TimeProvider timeProvider;

    public CouponDefinitionCacheWarmup(
            V2IssuableCouponRoundQuery query, TimeProvider timeProvider) {
        this.query = query;
        this.timeProvider = timeProvider;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void warmUp() {
        try {
            int loaded = query.findOpenDefinitions(timeProvider.instant()).size();
            log.info("쿠폰 정의 캐시 예열 완료: openDefinitions={}", loaded);
        } catch (RuntimeException failure) {
            log.warn("쿠폰 정의 캐시 예열 실패 — 첫 요청이 직접 로드한다: {}", failure.toString());
        }
    }
}
