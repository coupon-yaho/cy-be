package com.kafkick.infra.redis.coupon.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

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
                .thenReturn(List.of(List.of(1L, 1L, 1L)));
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
                .thenReturn(List.of(List.of(1L, 1L, 1L), List.of(1L, 2L, 0L)));
        RedisV2AdminPreparationReader reader = new RedisV2AdminPreparationReader(redisTemplate);

        Map<Long, V2PreparationSource> result = reader.read(
                List.of(request(10L), request(11L)), SNAPSHOT);

        assertThat(result.get(10L)).isEqualTo(
                new V2PreparationSource(true, true, SourceStatus.VALID, SNAPSHOT));
        assertThat(result.get(11L).status()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /** 관리자 요청이 issued 크기와 무관하게 Hash 스캔 없이 끝나는지 검증합니다. */
    @Test
    @DisplayName("준비 상태 조회는 issued Hash를 스캔하지 않는다")
    void doesNotScanIssuedHashOnRequestPath() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        when(redisTemplate.executePipelined(any(SessionCallback.class)))
                .thenReturn(List.of(List.of(1L, 1L, 1L)));
        RedisV2AdminPreparationReader reader = new RedisV2AdminPreparationReader(redisTemplate);

        V2PreparationSource result = reader.read(List.of(request(10L)), SNAPSHOT).get(10L);

        assertThat(result).isEqualTo(
                new V2PreparationSource(true, true, SourceStatus.VALID, SNAPSHOT));
        verify(redisTemplate, never()).opsForHash();
    }

    /** 반복 테스트가 사용하는 정상 예약 회차 비교값을 생성합니다. */
    private static V2AdminPreparationReader.Request request(long couponId) {
        return new V2AdminPreparationReader.Request(
                couponId, CouponRoundStatus.SCHEDULED, OPENS_AT, CLOSES_AT, 3, 100L, 100L);
    }
}
