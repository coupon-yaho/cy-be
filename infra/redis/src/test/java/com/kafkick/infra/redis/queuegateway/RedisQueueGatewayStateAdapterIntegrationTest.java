package com.kafkick.infra.redis.queuegateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.queuegateway.QueueGatewayCouponRoundState;

@Testcontainers(disabledWithoutDocker = true)
class RedisQueueGatewayStateAdapterIntegrationTest {

    @Container
    private static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7.4-alpine")).withExposedPorts(6379);
    private static final Instant NOW = Instant.parse("2026-08-31T03:00:00Z");

    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate redis;
    private RedisQueueGatewayStateAdapter adapter;

    @BeforeAll
    static void startRedis() {
        factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getFirstMappedPort()));
        factory.afterPropertiesSet();
        factory.start();
        redis = new StringRedisTemplate(factory);
        redis.afterPropertiesSet();
    }

    @AfterAll
    static void stopRedis() {
        if (factory != null) {
            factory.destroy();
        }
    }

    @BeforeEach
    void reset() {
        redis.getConnectionFactory().getConnection().serverCommands().flushDb();
        adapter = new RedisQueueGatewayStateAdapter(redis);
    }

    @Test
    void publishesCapacityAndAtomicallyReconcilesTheFullCouponSnapshot() {
        redis.opsForSet().add("coupons:active", "9");
        redis.opsForHash().put("coupon:policy", "9", "{\"mode\":\"OFF\"}");
        redis.opsForValue().set("stock:{9}", "1");

        adapter.reportCapacity("api-1", 250L, NOW);
        adapter.publishCouponRounds(adapter.reserveCouponRoundSnapshotVersion(), List.of(
                state(10L, 7L),
                new QueueGatewayCouponRoundState(11L, null, SourceStatus.UNAVAILABLE, null)),
                QueueMode.ALWAYS);

        assertThat(redis.opsForHash().get("capacity:coupon-svc:v1", "api-1"))
                .isEqualTo("{\"credits\":250,\"ts\":1788145200}");
        assertThat(redis.opsForSet().members("coupons:active")).containsExactlyInAnyOrder("10", "11");
        assertThat(redis.opsForValue().get("stock:{10}")).isEqualTo("7");
        assertThat(redis.hasKey("stock:{11}")).isFalse();
        assertThat(redis.hasKey("stock:{9}")).isFalse();
        assertThat(redis.opsForHash().get("coupon:policy", "10"))
                .isEqualTo("{\"mode\":\"ALWAYS\"}");
        assertThat(redis.opsForHash().get("coupon:policy", "11"))
                .isEqualTo("{\"mode\":\"ALWAYS\"}");

        adapter.removeCapacity("api-1");
        assertThat(redis.opsForHash().hasKey("capacity:coupon-svc:v1", "api-1")).isFalse();
    }

    @Test
    void unavailableOpenRoundPreservesLastNormalStockAndRepeatedSnapshotsConverge() {
        redis.opsForValue().set("stock:{11}", "5");
        List<QueueGatewayCouponRoundState> snapshot = List.of(
                new QueueGatewayCouponRoundState(11L, null, SourceStatus.UNAVAILABLE, null));

        adapter.publishCouponRounds(adapter.reserveCouponRoundSnapshotVersion(),
                snapshot, QueueMode.ADAPTIVE);
        adapter.publishCouponRounds(adapter.reserveCouponRoundSnapshotVersion(),
                snapshot, QueueMode.ADAPTIVE);

        assertThat(redis.opsForSet().members("coupons:active")).containsExactly("11");
        assertThat(redis.opsForValue().get("stock:{11}")).isEqualTo("5");
        assertThat(redis.opsForHash().get("coupon:policy", "11"))
                .isEqualTo("{\"mode\":\"ADAPTIVE\"}");
    }

    @Test
    void preflightTypeFailureDoesNotPartiallyChangeAHealthySnapshot() {
        redis.opsForSet().add("coupons:active", "10");
        redis.opsForValue().set("stock:{10}", "7");
        redis.opsForValue().set("coupon:policy", "wrong-type");

        assertThatThrownBy(() -> adapter.publishCouponRounds(
                adapter.reserveCouponRoundSnapshotVersion(), List.of(state(10L, 8L)), QueueMode.OFF))
                .isInstanceOf(RuntimeException.class);

        assertThat(redis.opsForValue().get("stock:{10}")).isEqualTo("7");
        assertThat(redis.opsForSet().members("coupons:active")).containsExactly("10");
    }

    @Test
    void olderSnapshotCannotOverwriteANewerSnapshot() {
        long older = adapter.reserveCouponRoundSnapshotVersion();
        long newer = adapter.reserveCouponRoundSnapshotVersion();

        adapter.publishCouponRounds(newer, List.of(state(20L, 9L)), QueueMode.ALWAYS);
        adapter.publishCouponRounds(older, List.of(state(10L, 7L)), QueueMode.OFF);

        assertThat(redis.opsForSet().members("coupons:active")).containsExactly("20");
        assertThat(redis.opsForValue().get("stock:{20}")).isEqualTo("9");
        assertThat(redis.hasKey("stock:{10}")).isFalse();
        assertThat(redis.opsForHash().get("coupon-svc:queue-gateway:snapshot", "applied"))
                .isEqualTo(Long.toString(newer));
    }

    private static QueueGatewayCouponRoundState state(long couponId, long stock) {
        return new QueueGatewayCouponRoundState(couponId, stock, SourceStatus.VALID, NOW);
    }
}
