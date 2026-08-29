package com.kafkick.infra.redis.coupon.v2;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.kafkick.core.coupon.v2.query.CouponDefinitionL2CachePort;
import com.kafkick.core.coupon.v2.query.DisabledCouponDefinitionL2Cache;

import tools.jackson.databind.ObjectMapper;

/**
 * 공유 정의 캐시(L2) 조립.
 *
 * <p><b>물러남이 아니라 대체다.</b> 게이트({@link IssuanceGateRedisAutoConfiguration})는 Redis 가
 * 없으면 빈을 안 만드는 것이 옳다 — 없는 통로를 세워 두면 첫 발급에서야 드러나기 때문이다.
 * L2 는 반대다. 소비자(V2 목록 조회)가 <b>Redis 여부와 무관하게 무조건 뜨는 빈</b>이라, 여기서
 * 그냥 물러나면 Redis 없는 배포는 조회 경로가 통째로 기동에서 죽는다. 그래서 Redis 가 없을 때는
 * 항상 miss 인 no-op 을 대신 세운다.
 *
 * <p>두 빈의 선언 순서가 계약이다. 아래쪽 no-op 의 {@code @ConditionalOnMissingBean} 은 같은
 * 클래스 안에서 위 빈이 먼저 평가된 뒤에만 옳은 답을 낸다. 컴포넌트 스캔되는 {@code @Configuration}
 * 으로 옮기면 {@code StringRedisTemplate} 등록 전에 조건이 평가돼 <b>항상 no-op 이 이긴다</b> —
 * 예외도 로그도 없이 L2 가 영영 안 쓰인다.
 */
@AutoConfiguration(after = DataRedisAutoConfiguration.class)
public class CouponDefinitionL2RedisAutoConfiguration {

    @Bean
    @ConditionalOnBean(StringRedisTemplate.class)
    @ConditionalOnMissingBean(CouponDefinitionL2CachePort.class)
    CouponDefinitionL2CachePort redisCouponDefinitionL2Cache(
            StringRedisTemplate redisTemplate, ObjectMapper objectMapper) {
        return new RedisCouponDefinitionL2Cache(redisTemplate, objectMapper);
    }

    @Bean
    @ConditionalOnMissingBean(CouponDefinitionL2CachePort.class)
    CouponDefinitionL2CachePort disabledCouponDefinitionL2Cache() {
        return new DisabledCouponDefinitionL2Cache();
    }
}
