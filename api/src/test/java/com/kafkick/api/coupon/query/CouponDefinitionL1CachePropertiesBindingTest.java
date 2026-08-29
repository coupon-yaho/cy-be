package com.kafkick.api.coupon.query;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CouponDefinitionL1CachePropertiesBindingTest {

    @Test
    void kebabCaseKeysBindToAllL1Properties() {
        CouponDefinitionL1CacheProperties bound = new Binder(new MapConfigurationPropertySource(Map.of(
                "coupon.definition-cache.l1.ttl", "7s",
                "coupon.definition-cache.l1.load-timeout", "80ms",
                "coupon.definition-cache.l1.stale-max-age", "55s",
                "coupon.definition-cache.l1.maximum-size", "77")))
                .bind("coupon.definition-cache.l1", CouponDefinitionL1CacheProperties.class)
                .get();

        assertThat(bound.ttl()).isEqualTo(Duration.ofSeconds(7));
        assertThat(bound.loadTimeout()).isEqualTo(Duration.ofMillis(80));
        assertThat(bound.staleMaxAge()).isEqualTo(Duration.ofSeconds(55));
        assertThat(bound.maximumSize()).isEqualTo(77L);
    }

    @Test
    void rejectsAStaleWindowShorterThanTheFreshTtl() {
        assertThatThrownBy(() -> new CouponDefinitionL1CacheProperties(
                Duration.ofSeconds(10), Duration.ofMillis(100), Duration.ofSeconds(5), 100L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stale-max-age");
    }

    @Test
    void allowsAStaleWindowEqualToTheFreshTtl() {
        assertThat(new CouponDefinitionL1CacheProperties(
                Duration.ofSeconds(10), Duration.ofMillis(100), Duration.ofSeconds(10), 100L))
                .isNotNull();
    }

    @Test
    void kebabCaseKeysBindToAllL2Properties() {
        CouponDefinitionL2CacheProperties bound = new Binder(new MapConfigurationPropertySource(Map.of(
                "coupon.definition-cache.l2.ttl", "9s",
                "coupon.definition-cache.l2.lock-lease", "4s",
                "coupon.definition-cache.l2.wait-timeout", "70ms",
                "coupon.definition-cache.l2.poll-interval", "5ms")))
                .bind("coupon.definition-cache.l2", CouponDefinitionL2CacheProperties.class)
                .get();

        assertThat(bound.ttl()).isEqualTo(Duration.ofSeconds(9));
        assertThat(bound.lockLease()).isEqualTo(Duration.ofSeconds(4));
        assertThat(bound.waitTimeout()).isEqualTo(Duration.ofMillis(70));
        assertThat(bound.pollInterval()).isEqualTo(Duration.ofMillis(5));
    }

    @Test
    void rejectsAPollIntervalLongerThanTheWaitBudget() {
        assertThatThrownBy(() -> new CouponDefinitionL2CacheProperties(
                Duration.ofSeconds(10), Duration.ofSeconds(3),
                Duration.ofMillis(10), Duration.ofMillis(50)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("poll-interval");
    }
}
