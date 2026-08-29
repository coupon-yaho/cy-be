package com.kafkick.api.coupon.query;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.kafkick.core.support.TimeProvider;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CouponDefinitionCacheWarmupTest {

    private static final Instant NOW = Instant.parse("2026-08-28T00:00:00Z");

    @Test
    void fillsTheCacheBeforeTheFirstRequestArrives() {
        // 없으면 첫 로드가 예산을 넘는 동안 되돌려 줄 stale 이 없어 목록이 통째로 503 이다.
        V2IssuableCouponRoundQuery query = mock(V2IssuableCouponRoundQuery.class);
        when(query.findOpenDefinitions(any())).thenReturn(List.of());

        new CouponDefinitionCacheWarmup(query, new TimeProvider(Clock.fixed(NOW, ZoneOffset.UTC)))
                .warmUp();

        verify(query).findOpenDefinitions(NOW);
    }

    @Test
    void keepsBootingWhenTheWarmupItselfFails() {
        // DB 가 아직 안 떠 있는 배포 순서에서 앱이 죽으면, 준비가 끝난 뒤에도 아무도 서비스하지 않는다.
        V2IssuableCouponRoundQuery query = mock(V2IssuableCouponRoundQuery.class);
        when(query.findOpenDefinitions(any())).thenThrow(new IllegalStateException("db down"));

        assertThatCode(() -> new CouponDefinitionCacheWarmup(
                query, new TimeProvider(Clock.fixed(NOW, ZoneOffset.UTC))).warmUp())
                .doesNotThrowAnyException();
    }
}
