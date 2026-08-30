package com.kafkick.infra.redis.coupon.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import com.kafkick.core.admin.preparation.V2AdminPreparationReader;
import com.kafkick.core.admin.preparation.V2PreparationSource;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.SourceStatus;

/** Redis 통신·응답 경계 실패가 V2 준비 상태로 격리되는지 검증합니다. */
class RedisV2AdminPreparationReaderTest {

    private static final Instant SNAPSHOT = Instant.parse("2026-08-29T09:00:00Z");
    private static final Instant OPENS_AT = Instant.parse("2026-08-29T10:00:00Z");
    private static final Instant CLOSES_AT = Instant.parse("2026-08-29T11:00:00Z");

    /** Redis 연결 자체가 끊기면 일부 회차만 정상처럼 남지 않고 요청 전체가 미판정인지 검증합니다. */
    @Test
    @DisplayName("Redis 통신 실패는 요청한 모든 V2 회차를 UNAVAILABLE로 반환한다")
    void mapsCommunicationFailureToUnavailableForEveryRequest() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        when(redisTemplate.executePipelined(any(SessionCallback.class)))
                .thenThrow(new RedisConnectionFailureException("down"));
        RedisV2AdminPreparationReader reader = new RedisV2AdminPreparationReader(redisTemplate);

        Map<Long, V2PreparationSource> result = reader.read(
                List.of(request(10L), request(11L)), SNAPSHOT);

        assertThat(result).containsOnlyKeys(10L, 11L);
        assertThat(result.values()).allSatisfy(source -> {
            assertThat(source.status()).isEqualTo(SourceStatus.UNAVAILABLE);
            assertThat(source.warmupReady()).isNull();
            assertThat(source.gateReady()).isNull();
        });
    }

    /** 회차 수와 pipeline 응답 수가 갈리면 ID가 잘못 짝지어지는 대신 전체를 미판정하는지 검증합니다. */
    @Test
    @DisplayName("pipeline 응답 수 불일치는 요청 전체를 UNAVAILABLE로 반환한다")
    void rejectsPipelineCardinalityMismatch() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        when(redisTemplate.executePipelined(any(SessionCallback.class)))
                .thenReturn(List.of(List.of(1L, 1L, 1L, 0L)));
        RedisV2AdminPreparationReader reader = new RedisV2AdminPreparationReader(redisTemplate);

        Map<Long, V2PreparationSource> result = reader.read(
                List.of(request(10L), request(11L)), SNAPSHOT);

        assertThat(result).containsOnlyKeys(10L, 11L);
        assertThat(result.values()).extracting(V2PreparationSource::status)
                .containsOnly(SourceStatus.UNAVAILABLE);
    }

    /** 한 회차의 파손 응답이 이웃 회차의 정상 판정까지 지우지 않는지 검증합니다. */
    @Test
    @DisplayName("형식이 잘못된 회차 응답만 UNAVAILABLE로 격리한다")
    void isolatesMalformedCampaignResponse() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        when(redisTemplate.executePipelined(any(SessionCallback.class)))
                .thenReturn(List.of(List.of(1L, 1L, 1L, 0L), List.of(1L, 2L, 0L, 0L)));
        RedisV2AdminPreparationReader reader = new RedisV2AdminPreparationReader(redisTemplate);

        Map<Long, V2PreparationSource> result = reader.read(
                List.of(request(10L), request(11L)), SNAPSHOT);

        assertThat(result.get(10L)).isEqualTo(
                new V2PreparationSource(true, true, SourceStatus.VALID, SNAPSHOT));
        assertThat(result.get(11L).status()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /** Lua의 크기 검사가 통과해도 Hash 값이 발급 코덱과 다르면 워밍업 실패인지 검증합니다. */
    @Test
    @DisplayName("issued Hash의 파손 값은 VALID 워밍업 실패로 반환한다")
    @SuppressWarnings("unchecked")
    void mapsCorruptIssuedValueToWarmupFailure() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        HashOperations<String, String, String> hashOperations = mock(HashOperations.class);
        Cursor<Map.Entry<String, String>> cursor = mock(Cursor.class);
        when(redisTemplate.executePipelined(any(SessionCallback.class)))
                .thenReturn(List.of(List.of(1L, 1L, 1L, 1L)));
        when(redisTemplate.<String, String>opsForHash()).thenReturn(hashOperations);
        when(hashOperations.scan(any(String.class), any())).thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true, false);
        when(cursor.next()).thenReturn(Map.entry("1", "BROKEN"));
        RedisV2AdminPreparationReader reader = new RedisV2AdminPreparationReader(redisTemplate);

        V2PreparationSource result = reader.read(List.of(request(10L)), SNAPSHOT).get(10L);

        assertThat(result).isEqualTo(
                new V2PreparationSource(false, true, SourceStatus.VALID, SNAPSHOT));
    }

    /** 한 회차의 증분 스캔 실패가 같은 pipeline의 다른 정상 회차까지 지우지 않는지 검증합니다. */
    @Test
    @DisplayName("issued 스캔 실패는 해당 회차만 UNAVAILABLE로 격리한다")
    @SuppressWarnings("unchecked")
    void isolatesIssuedScanFailureToAffectedCampaign() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        HashOperations<String, String, String> hashOperations = mock(HashOperations.class);
        when(redisTemplate.executePipelined(any(SessionCallback.class))).thenReturn(List.of(
                List.of(1L, 1L, 1L, 1L),
                List.of(1L, 1L, 1L, 0L)));
        when(redisTemplate.<String, String>opsForHash()).thenReturn(hashOperations);
        when(hashOperations.scan(any(String.class), any()))
                .thenThrow(new RedisConnectionFailureException("scan down"));
        RedisV2AdminPreparationReader reader = new RedisV2AdminPreparationReader(redisTemplate);

        Map<Long, V2PreparationSource> result = reader.read(
                List.of(request(10L), request(11L)), SNAPSHOT);

        assertThat(result.get(10L).status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(result.get(11L)).isEqualTo(
                new V2PreparationSource(true, true, SourceStatus.VALID, SNAPSHOT));
    }

    /** HSCAN 동안 모집단이 바뀌면 혼합 시점의 값을 정상으로 추측하지 않는지 검증합니다. */
    @Test
    @DisplayName("issued 스캔 전후 모집단이 바뀌면 UNAVAILABLE이다")
    @SuppressWarnings({"rawtypes", "unchecked"})
    void mapsIssuedMutationDuringScanToUnavailable() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        HashOperations<String, String, String> hashOperations = mock(HashOperations.class);
        Cursor<Map.Entry<String, String>> cursor = mock(Cursor.class);
        when(redisTemplate.executePipelined(any(SessionCallback.class)))
                .thenReturn(List.of(List.of(1L, 1L, 1L, 1L)));
        when(redisTemplate.<String, String>opsForHash()).thenReturn(hashOperations);
        when(hashOperations.scan(any(String.class), any())).thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true, false);
        when(cursor.next()).thenReturn(Map.entry("1", "D|1|token|key"));
        when(redisTemplate.execute(any(RedisScript.class), any(List.class), any(Object[].class)))
                .thenReturn(List.of(1L, 1L, 1L, 2L));
        RedisV2AdminPreparationReader reader = new RedisV2AdminPreparationReader(redisTemplate);

        V2PreparationSource result = reader.read(List.of(request(10L)), SNAPSHOT).get(10L);

        assertThat(result.status()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /** HSCAN의 허용된 중복 반환을 실제 Hash 크기 증가로 오인하지 않는지 검증합니다. */
    @Test
    @DisplayName("issued 스캔의 중복 field는 한 건으로 계산한다")
    @SuppressWarnings({"rawtypes", "unchecked"})
    void deduplicatesIssuedFieldsReturnedByScan() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        HashOperations<String, String, String> hashOperations = mock(HashOperations.class);
        Cursor<Map.Entry<String, String>> cursor = mock(Cursor.class);
        when(redisTemplate.executePipelined(any(SessionCallback.class)))
                .thenReturn(List.of(List.of(1L, 1L, 1L, 1L)));
        when(redisTemplate.<String, String>opsForHash()).thenReturn(hashOperations);
        when(hashOperations.scan(any(String.class), any())).thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true, true, false);
        when(cursor.next()).thenReturn(
                Map.entry("1", "D|1|token|key"),
                Map.entry("1", "D|1|token|key"));
        when(redisTemplate.execute(any(RedisScript.class), any(List.class), any(Object[].class)))
                .thenReturn(List.of(1L, 1L, 1L, 1L));
        RedisV2AdminPreparationReader reader = new RedisV2AdminPreparationReader(redisTemplate);

        V2PreparationSource result = reader.read(List.of(request(10L)), SNAPSHOT).get(10L);

        assertThat(result).isEqualTo(
                new V2PreparationSource(true, true, SourceStatus.VALID, SNAPSHOT));
    }

    /** 반복 테스트가 사용하는 정상 예약 회차 비교값을 생성합니다. */
    private static V2AdminPreparationReader.Request request(long couponId) {
        return new V2AdminPreparationReader.Request(
                couponId, CouponRoundStatus.SCHEDULED, OPENS_AT, CLOSES_AT, 3, 100L, 100L);
    }
}
