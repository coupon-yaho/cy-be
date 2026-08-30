package com.kafkick.infra.redis.queuegateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.queuegateway.QueueGatewayCouponRoundState;

class RedisQueueGatewayStateAdapterTest {

    private static final Instant NOW = Instant.parse("2026-08-31T01:02:03Z");

    @Test
    void reportsAndRemovesCapacityInTheGatewayHash() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        @SuppressWarnings("unchecked")
        HashOperations<String, Object, Object> hashes = mock(HashOperations.class);
        when(redis.opsForHash()).thenReturn(hashes);
        RedisQueueGatewayStateAdapter adapter = adapter(redis);

        adapter.reportCapacity("api-1", 120L, NOW);
        adapter.removeCapacity("api-1");

        verify(hashes).put(eq("capacity:coupon-svc:v1"), eq("api-1"),
                eq("{\"credits\":120,\"ts\":1788138123}"));
        verify(hashes).delete("capacity:coupon-svc:v1", "api-1");
    }

    @Test
    void passesAFullSnapshotToOneAtomicRedisScript() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        when(redis.execute(any(), eq(List.of("coupons:active", "coupon:policy")),
                eq("ALWAYS"), eq("2"), eq("10"), eq("1"), eq("7"),
                eq("11"), eq("0"), eq(""))).thenReturn(1L);
        RedisQueueGatewayStateAdapter adapter = adapter(redis);

        adapter.publishCouponRounds(List.of(
                new QueueGatewayCouponRoundState(10L, 7L, SourceStatus.VALID, NOW),
                new QueueGatewayCouponRoundState(11L, null, SourceStatus.UNAVAILABLE, null)),
                QueueMode.ALWAYS);

        verify(redis).execute(any(), eq(List.of("coupons:active", "coupon:policy")),
                eq("ALWAYS"), eq("2"), eq("10"), eq("1"), eq("7"),
                eq("11"), eq("0"), eq(""));
    }

    @Test
    void rejectsInvalidCapacityAndDuplicateRoundsBeforeRedis() {
        RedisQueueGatewayStateAdapter adapter = adapter(mock(StringRedisTemplate.class));

        assertThatThrownBy(() -> adapter.reportCapacity(" ", 1L, NOW))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> adapter.reportCapacity("api", -1L, NOW))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> adapter.publishCouponRounds(List.of(
                new QueueGatewayCouponRoundState(10L, 7L, SourceStatus.VALID, NOW),
                new QueueGatewayCouponRoundState(10L, 6L, SourceStatus.VALID, NOW)), QueueMode.OFF))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void exposesOnlyTheExternalGatewayKeyNamespace() {
        assertThat(RedisQueueGatewayStateAdapter.CAPACITY_KEY).isEqualTo("capacity:coupon-svc:v1");
        assertThat(RedisQueueGatewayStateAdapter.ACTIVE_COUPONS_KEY).isEqualTo("coupons:active");
        assertThat(RedisQueueGatewayStateAdapter.POLICY_KEY).isEqualTo("coupon:policy");
        assertThat(RedisQueueGatewayStateAdapter.stockKey(10L)).isEqualTo("stock:{10}");
    }

    private static RedisQueueGatewayStateAdapter adapter(StringRedisTemplate redis) {
        return new RedisQueueGatewayStateAdapter(redis);
    }
}
