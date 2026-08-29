package com.kafkick.infra.redis.coupon.v2;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.v2.query.CouponDefinition;
import com.kafkick.core.coupon.v2.query.CouponDefinitionSnapshot;
import com.kafkick.core.coupontemplate.domain.CouponPolicyType;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * L2 는 조회 경로의 새 필수 의존성이 아니다. Redis 가 어떤 식으로 실패하든 이 클래스에서 끝나야
 * 하고, 밖에서는 "L2 가 없는 것"과 구별되지 않아야 한다 — 여기서 던지면 Redis 장애가 곧
 * 목록 503 이 되어 L2 를 넣기 전보다 가용성이 나빠진다.
 */
class RedisCouponDefinitionL2CacheTest {

    private static final Instant NOW = Instant.parse("2026-08-28T00:00:00Z");
    private final ObjectMapper objectMapper = JsonMapper.builder().build();

    @Test
    void roundTripsASnapshotThroughItsJsonForm() {
        CouponDefinitionSnapshot snapshot = snapshot();

        String json = objectMapper.writeValueAsString(snapshot);

        assertThat(objectMapper.readValue(json, CouponDefinitionSnapshot.class))
                .isEqualTo(snapshot);
    }

    @Test
    void treatsAReadFailureAsAMiss() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        ValueOperations<String, String> values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        when(values.get(anyString())).thenThrow(new QueryTimeoutException("redis down"));

        assertThat(new RedisCouponDefinitionL2Cache(redis, objectMapper).find()).isEmpty();
    }

    @Test
    void treatsACorruptValueAsAMiss() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        ValueOperations<String, String> values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        when(values.get(anyString())).thenReturn("{not json");

        assertThat(new RedisCouponDefinitionL2Cache(redis, objectMapper).find()).isEmpty();
    }

    @Test
    void swallowsAWriteFailure() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        ValueOperations<String, String> values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        org.mockito.Mockito.doThrow(new QueryTimeoutException("redis down"))
                .when(values).set(anyString(), anyString(), any(Duration.class));

        assertThatCode(() -> new RedisCouponDefinitionL2Cache(redis, objectMapper)
                .put(snapshot(), Duration.ofSeconds(10))).doesNotThrowAnyException();
    }

    @Test
    void grantsTheLoadPermitToItselfWhenRedisCannotAnswer() {
        // 공유 락을 못 쓰는 상태다. 각 인스턴스가 자기 로드를 하는 편이 503 보다 낫다.
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        ValueOperations<String, String> values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        when(values.setIfAbsent(anyString(), anyString(), any(Duration.class)))
                .thenThrow(new QueryTimeoutException("redis down"));

        assertThat(new RedisCouponDefinitionL2Cache(redis, objectMapper)
                .tryAcquireLoad(Duration.ofSeconds(3))).isPresent();
    }

    @Test
    void withholdsTheLoadPermitWhenAnotherInstanceHoldsIt() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        ValueOperations<String, String> values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        when(values.setIfAbsent(eq(RedisCouponDefinitionL2Cache.LOAD_KEY), anyString(),
                any(Duration.class))).thenReturn(false);

        assertThat(new RedisCouponDefinitionL2Cache(redis, objectMapper)
                .tryAcquireLoad(Duration.ofSeconds(3))).isEmpty();
    }

    @Test
    void swallowsAReleaseFailure() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        when(redis.execute(any(org.springframework.data.redis.core.script.RedisScript.class),
                any(List.class), any())).thenThrow(new QueryTimeoutException("redis down"));

        assertThatCode(() -> new RedisCouponDefinitionL2Cache(redis, objectMapper)
                .releaseLoad("token")).doesNotThrowAnyException();
    }

    private static CouponDefinitionSnapshot snapshot() {
        return new CouponDefinitionSnapshot(List.of(new CouponDefinition(
                7L, 2L, "영화 할인", CouponPolicyType.FIXED_AMOUNT, null, null, 3_000, 30,
                NOW.minusSeconds(1), NOW.plusSeconds(60), CouponRoundStatus.OPEN)),
                NOW.plusSeconds(60));
    }
}
