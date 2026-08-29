package com.kafkick.infra.redis.coupon.v2;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.kafkick.core.admin.couponmetrics.CouponMetricsSource;
import com.kafkick.core.admin.stock.AdminStockSnapshot;
import com.kafkick.core.admin.stock.V2AdminStockReader;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.SourceStatus;

/** 실제 Redis에서 관리자 V2 재고의 값·파손·미준비 상태 계약을 검증합니다. */
@Testcontainers(disabledWithoutDocker = true)
class RedisV2AdminStockReaderIntegrationTest {

    @Container
    private static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7.4-alpine")).withExposedPorts(6379);

    private static final Instant SNAPSHOT = Instant.parse("2026-08-29T09:00:00Z");
    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate redis;
    private RedisV2AdminStockReader reader;

    @BeforeAll
    static void startRedis() {
        factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getFirstMappedPort()));
        factory.afterPropertiesSet();
        factory.start();
        redis = new StringRedisTemplate(factory);
    }

    @AfterAll
    static void stopRedis() {
        factory.destroy();
    }

    @BeforeEach
    void reset() {
        redis.getConnectionFactory().getConnection().serverCommands().flushDb();
        reader = new RedisV2AdminStockReader(redis);
    }

    /** V2 정본의 total과 stock을 읽어 발급 엔진 중립 재고로 반환하는지 검증합니다. */
    @Test
    @DisplayName("정상 meta와 stock은 VALID 권위 재고다")
    void readsValidStock() {
        writeStock(10L, "OPEN", "100", "25");

        CouponMetricsSource.Observation<AdminStockSnapshot> result = read(
                new V2AdminStockReader.Request(10L, CouponRoundStatus.OPEN, 100L)).get(10L);

        assertThat(result.status()).isEqualTo(SourceStatus.VALID);
        assertThat(result.value()).isEqualTo(new AdminStockSnapshot(100L, 25L));
        assertThat(result.observedAt()).isEqualTo(SNAPSHOT);
    }

    /** 워밍업 전 예약 회차와 이미 열려야 하는 회차의 키 부재를 다르게 판정하는지 검증합니다. */
    @Test
    @DisplayName("키 부재는 SCHEDULED면 PENDING이고 OPEN이면 UNAVAILABLE이다")
    void distinguishesPendingWarmupFromMissingOpenStock() {
        Map<Long, CouponMetricsSource.Observation<AdminStockSnapshot>> result = read(List.of(
                new V2AdminStockReader.Request(10L, CouponRoundStatus.SCHEDULED, 100L),
                new V2AdminStockReader.Request(11L, CouponRoundStatus.OPEN, 100L)));

        assertThat(result.get(10L).status()).isEqualTo(SourceStatus.PENDING);
        assertThat(result.get(11L).status()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /** 거짓 정상값을 만들 수 있는 부분 키·파손 숫자·DB total 불일치를 모두 거부하는지 검증합니다. */
    @Test
    @DisplayName("부분 상태와 파손 값과 DB total 불일치는 UNAVAILABLE이다")
    void rejectsPartialCorruptAndMismatchedState() {
        IssuanceKeys partial = IssuanceKeys.of(10L);
        redis.opsForValue().set(partial.stock(), "25");
        writeStock(11L, "OPEN", "100", "-1");
        writeStock(12L, "OPEN", "99", "25");

        Map<Long, CouponMetricsSource.Observation<AdminStockSnapshot>> result = read(List.of(
                new V2AdminStockReader.Request(10L, CouponRoundStatus.OPEN, 100L),
                new V2AdminStockReader.Request(11L, CouponRoundStatus.OPEN, 100L),
                new V2AdminStockReader.Request(12L, CouponRoundStatus.OPEN, 100L)));

        assertThat(result.values()).allSatisfy(
                observation -> assertThat(observation.status()).isEqualTo(SourceStatus.UNAVAILABLE));
    }

    private Map<Long, CouponMetricsSource.Observation<AdminStockSnapshot>> read(
            V2AdminStockReader.Request request
    ) {
        return read(List.of(request));
    }

    private Map<Long, CouponMetricsSource.Observation<AdminStockSnapshot>> read(
            List<V2AdminStockReader.Request> requests
    ) {
        return reader.read(requests, SNAPSHOT);
    }

    private static void writeStock(long couponId, String status, String total, String remaining) {
        IssuanceKeys keys = IssuanceKeys.of(couponId);
        redis.opsForHash().putAll(keys.meta(), Map.of(
                RedisIssuanceGate.META_STATUS, status,
                RedisIssuanceGate.META_TOTAL_QUANTITY, total));
        redis.opsForValue().set(keys.stock(), remaining);
    }
}
