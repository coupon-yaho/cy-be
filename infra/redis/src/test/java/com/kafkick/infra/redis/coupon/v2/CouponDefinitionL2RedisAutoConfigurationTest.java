package com.kafkick.infra.redis.coupon.v2;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.context.annotation.ImportCandidates;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import com.kafkick.core.coupon.v2.query.CouponDefinitionL2CachePort;
import com.kafkick.core.coupon.v2.query.DisabledCouponDefinitionL2Cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * L2 는 게이트와 반대로 <b>물러나면 안 된다</b>. 소비자(V2 목록 조회)가 Redis 여부와 무관하게
 * 무조건 뜨는 빈이라, 여기서 빈을 안 만들면 Redis 없는 배포는 조회 경로가 통째로 기동에서 죽는다.
 *
 * <p>그래서 양쪽을 다 본다: Redis 가 있을 때 <b>Redis 구현</b>이 서는지, 없을 때 <b>no-op 이
 * 대신</b> 서는지. 한쪽만 보면 조건을 뒤집어도 초록이다.
 */
class CouponDefinitionL2RedisAutoConfigurationTest {

    @Test
    @DisplayName("자동설정으로 등록돼 있어야 한다 — imports 에서 빠지면 소비자가 빈을 못 찾아 기동에서 죽는다")
    void isRegisteredAsAutoConfiguration() {
        assertThat(ImportCandidates.load(AutoConfiguration.class, getClass().getClassLoader()))
                .as("아래 테스트들은 이 클래스를 직접 등록하므로 imports 에서 빠져도 통과한다")
                .contains(CouponDefinitionL2RedisAutoConfiguration.class.getName());
    }

    @Test
    @DisplayName("Redis 통로가 있으면 공유 L2 가 선다")
    void registersRedisCacheWithRedisAutoConfiguration() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        JacksonAutoConfiguration.class, DataRedisAutoConfiguration.class,
                        CouponDefinitionL2RedisAutoConfiguration.class))
                .run(context -> {
                    assertThat(context).hasSingleBean(CouponDefinitionL2CachePort.class);
                    assertThat(context.getBean(CouponDefinitionL2CachePort.class))
                            .isInstanceOf(RedisCouponDefinitionL2Cache.class);
                });
    }

    @Test
    @DisplayName("Redis 통로가 없으면 no-op 이 대신 선다 — 여기서 물러나면 조회 경로가 기동에서 죽는다")
    void fallsBackToTheDisabledCacheWithoutRedisTemplate() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        CouponDefinitionL2RedisAutoConfiguration.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(CouponDefinitionL2CachePort.class);
                    assertThat(context.getBean(CouponDefinitionL2CachePort.class))
                            .isInstanceOf(DisabledCouponDefinitionL2Cache.class);
                });
    }

    @Test
    @DisplayName("이미 등록된 L2 가 있으면 둘 다 물러난다 — 테스트 대역이 어댑터에 밀리지 않는다")
    void userBeanWins() {
        CouponDefinitionL2CachePort userCache = mock(CouponDefinitionL2CachePort.class);
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        JacksonAutoConfiguration.class, DataRedisAutoConfiguration.class,
                        CouponDefinitionL2RedisAutoConfiguration.class))
                .withBean(CouponDefinitionL2CachePort.class, () -> userCache)
                .run(context -> assertThat(context.getBean(CouponDefinitionL2CachePort.class))
                        .isSameAs(userCache));
    }
}
