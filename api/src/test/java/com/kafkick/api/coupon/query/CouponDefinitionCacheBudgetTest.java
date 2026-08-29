package com.kafkick.api.coupon.query;

import java.time.Clock;
import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 두 예산의 관계는 조립할 때만 증명된다. 어긋난 값으로도 앱은 정상 기동하고 로그도 없다 —
 * 락을 두고도 herd 가 DB 로 가는 사실은 부하 시험 뒤 질의 수를 셀 때에야 드러난다.
 */
class CouponDefinitionCacheBudgetTest {

    private final CouponDefinitionL1CacheConfiguration configuration =
            new CouponDefinitionL1CacheConfiguration();

    @Test
    void rejectsAnL2WaitThatOutlastsTheL1CallerBudget() {
        assertThatThrownBy(() -> build(Duration.ofMillis(100), Duration.ofMillis(100)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("wait-timeout");
    }

    @Test
    void acceptsAnL2WaitShorterThanTheL1CallerBudget() {
        assertThat(build(Duration.ofMillis(100), Duration.ofMillis(60))).isNotNull();
    }

    private Object build(Duration loadTimeout, Duration waitTimeout) {
        return configuration.couponDefinitionL1Cache(
                Clock.systemUTC(),
                new CouponDefinitionL1CacheProperties(
                        Duration.ofSeconds(10), loadTimeout, Duration.ofSeconds(60), 1L),
                new CouponDefinitionL2CacheProperties(
                        Duration.ofSeconds(10), Duration.ofSeconds(3),
                        waitTimeout, Duration.ofMillis(10)),
                Runnable::run,
                registryProvider());
    }

    private static ObjectProvider<MeterRegistry> registryProvider() {
        return new ObjectProvider<>() {
            @Override public MeterRegistry getObject() { return new SimpleMeterRegistry(); }
            @Override public MeterRegistry getObject(Object... args) { return getObject(); }
            @Override public MeterRegistry getIfAvailable() { return getObject(); }
            @Override public MeterRegistry getIfUnique() { return getObject(); }
        };
    }
}
