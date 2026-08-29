package com.kafkick.api.coupon.query;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicReference;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.github.benmanes.caffeine.cache.Ticker;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import com.kafkick.core.observation.Dependency;
import com.kafkick.core.support.exception.BusinessException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CouponDefinitionL1CacheTest {

    /**
     * 스레드가 실제로 깨어났는지만 보는 <b>생존성</b> 상한이다. <b>검증 대상이 아니다</b> —
     * 이 테스트가 고정하는 계약은 {@code load-timeout} 쪽 값(밀리초)이다.
     *
     * <p>넉넉한 이유는 모듈 넷을 동시에 돌릴 때 단일 로더 스레드가 스케줄되기까지 초 단위로
     * 밀린 적이 있어서다(실측). 짧게 두면 그 부하에서만 빨갛게 되고, 그 빨강은 코드가 아니라
     * 그날의 CPU 를 가리킨다. 길게 둬도 진짜 멈춘 로드는 여전히 잡는다 — 늦게 잡을 뿐이다.
     */
    private static final long LIVENESS_SECONDS = 30L;

    @Test
    void hasOneSharedKeyForTheMainCouponDefinitions() {
        assertThat(CouponDefinitionCacheKey.values()).containsExactly(CouponDefinitionCacheKey.ALL);
    }

    @Test
    void returnsStaleValueWhenReloadFailsAfterFreshEntryExpires() {
        AtomicInteger nanos = new AtomicInteger();
        Instant now = Instant.parse("2026-08-28T00:00:00Z");
        CouponDefinitionL1Cache<String> cache = new CouponDefinitionL1Cache<>(
                Clock.fixed(now, ZoneOffset.UTC), nanos::get,
                new CouponDefinitionL1CacheProperties(
                        java.time.Duration.ofNanos(1), java.time.Duration.ofSeconds(1),
                        java.time.Duration.ofSeconds(60), 10L),
                Runnable::run, new SimpleMeterRegistry());
        CouponDefinitionCacheKey key = CouponDefinitionCacheKey.ALL;

        assertThat(cache.get(key, () -> new CouponDefinitionL1Cache.LoadedValue<>(
                "old", now.plusSeconds(30)))).isEqualTo("old");
        nanos.set(2);

        assertThat(cache.get(key, () -> { throw new IllegalStateException("db down"); }))
                .isEqualTo("old");
    }

    @Test
    void doesNotServeStaleValueAfterItsMaximumAge() {
        AtomicInteger nanos = new AtomicInteger();
        Instant now = Instant.parse("2026-08-28T00:00:00Z");
        CouponDefinitionL1Cache<String> cache = new CouponDefinitionL1Cache<>(
                Clock.fixed(now, ZoneOffset.UTC), nanos::get,
                new CouponDefinitionL1CacheProperties(
                        java.time.Duration.ofNanos(1), java.time.Duration.ofSeconds(1),
                        java.time.Duration.ofNanos(5), 10L), Runnable::run, new SimpleMeterRegistry());
        assertThat(cache.get(CouponDefinitionCacheKey.ALL,
                () -> new CouponDefinitionL1Cache.LoadedValue<>("old", now.plusSeconds(30))))
                .isEqualTo("old");
        nanos.set(6);

        assertThatThrownBy(() -> cache.get(CouponDefinitionCacheKey.ALL,
                () -> { throw new IllegalStateException("db down"); }))
                .isInstanceOfSatisfying(BusinessException.class,
                        failure -> assertThat(failure.getErrorCode().dependency())
                                .isEqualTo(Dependency.MYSQL));
    }

    @Test
    void doesNotServeStaleValueAcrossAnOpenOrCloseBoundary() {
        AtomicInteger nanos = new AtomicInteger();
        AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-08-28T00:00:00Z"));
        Clock clock = new Clock() {
            @Override public ZoneId getZone() { return ZoneOffset.UTC; }
            @Override public Clock withZone(ZoneId zone) { return this; }
            @Override public Instant instant() { return now.get(); }
        };
        CouponDefinitionL1Cache<String> cache = new CouponDefinitionL1Cache<>(clock, nanos::get,
                new CouponDefinitionL1CacheProperties(
                        java.time.Duration.ofNanos(1), java.time.Duration.ofSeconds(1),
                        java.time.Duration.ofSeconds(60), 10L), Runnable::run, new SimpleMeterRegistry());
        assertThat(cache.get(CouponDefinitionCacheKey.ALL,
                () -> new CouponDefinitionL1Cache.LoadedValue<>("old", now.get().plusSeconds(5))))
                .isEqualTo("old");
        nanos.set(2);
        now.set(now.get().plusSeconds(5));

        assertThatThrownBy(() -> cache.get(CouponDefinitionCacheKey.ALL,
                () -> { throw new IllegalStateException("db down"); }))
                .isInstanceOf(BusinessException.class);
    }

    @Test
    void clampsPastBoundaryToImmediateExpiryInsteadOfFailingTheRequest() {
        Instant now = Instant.parse("2026-08-28T00:00:00Z");
        CouponDefinitionL1Cache<String> cache = new CouponDefinitionL1Cache<>(
                Clock.fixed(now, ZoneOffset.UTC), Ticker.systemTicker(),
                new CouponDefinitionL1CacheProperties(
                        java.time.Duration.ofSeconds(10), java.time.Duration.ofSeconds(1),
                        java.time.Duration.ofSeconds(60), 10L),
                Runnable::run, new SimpleMeterRegistry());

        assertThat(cache.get(CouponDefinitionCacheKey.ALL,
                () -> new CouponDefinitionL1Cache.LoadedValue<>("at-boundary", now)))
                .isEqualTo("at-boundary");
    }

    @Test
    void returnsStaleValueWhenReloadExceedsBudget() throws Exception {
        AtomicInteger nanos = new AtomicInteger();
        Instant now = Instant.parse("2026-08-28T00:00:00Z");
        ExecutorService loaderExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch reloadStarted = new CountDownLatch(1);
        CountDownLatch releaseReload = new CountDownLatch(1);
        try {
            CouponDefinitionL1Cache<String> cache = new CouponDefinitionL1Cache<>(
                    Clock.fixed(now, ZoneOffset.UTC), nanos::get,
                    new CouponDefinitionL1CacheProperties(
                            java.time.Duration.ofNanos(1), java.time.Duration.ofMillis(10),
                            java.time.Duration.ofSeconds(60), 10L),
                    loaderExecutor, new SimpleMeterRegistry());
            CouponDefinitionCacheKey key = CouponDefinitionCacheKey.ALL;
            assertThat(cache.get(key, () -> new CouponDefinitionL1Cache.LoadedValue<>(
                    "old", now.plusSeconds(30)))).isEqualTo("old");
            nanos.set(2);

            assertThat(cache.get(key, () -> {
                reloadStarted.countDown();
                try {
                    if (!releaseReload.await(LIVENESS_SECONDS, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("test loader was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
                return new CouponDefinitionL1Cache.LoadedValue<>("new", now.plusSeconds(30));
            })).isEqualTo("old");
            assertThat(reloadStarted.await(LIVENESS_SECONDS, TimeUnit.SECONDS)).isTrue();
        } finally {
            releaseReload.countDown();
            loaderExecutor.shutdownNow();
        }
    }

    @Test
    void loadsTheSameKeyOnlyOnceDuringACacheMiss() throws Exception {
        CouponDefinitionL1Cache<String> cache = new CouponDefinitionL1Cache<>(
                Clock.fixed(Instant.parse("2026-08-28T00:00:00Z"), ZoneOffset.UTC),
                Ticker.systemTicker(),
                new CouponDefinitionL1CacheProperties(
                        java.time.Duration.ofSeconds(10), java.time.Duration.ofSeconds(1),
                        java.time.Duration.ofSeconds(60), 1_000L),
                Runnable::run, new SimpleMeterRegistry());
        CouponDefinitionCacheKey key = CouponDefinitionCacheKey.ALL;
        AtomicInteger loads = new AtomicInteger();
        CountDownLatch ready = new CountDownLatch(32);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(32);
        try {
            List<Future<String>> results = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                results.add(executor.submit(() -> {
                    ready.countDown();
                    assertThat(start.await(LIVENESS_SECONDS, TimeUnit.SECONDS)).isTrue();
                    return cache.get(key, () -> {
                        loads.incrementAndGet();
                        return new CouponDefinitionL1Cache.LoadedValue<>(
                                "definition", Instant.parse("2026-08-28T00:00:30Z"));
                    });
                }));
            }
            assertThat(ready.await(LIVENESS_SECONDS, TimeUnit.SECONDS)).isTrue();
            start.countDown();

            for (Future<String> result : results) {
                assertThat(result.get(LIVENESS_SECONDS, TimeUnit.SECONDS)).isEqualTo("definition");
            }
        } finally {
            executor.shutdownNow();
        }

        assertThat(loads).hasValue(1);
    }

    @Test
    void attributesItsOwnBudgetOverrunToNoDependencyInsteadOfMysql() throws Exception {
        ExecutorService loaderExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch releaseReload = new CountDownLatch(1);
        Instant now = Instant.parse("2026-08-28T00:00:00Z");
        try {
            CouponDefinitionL1Cache<String> cache = new CouponDefinitionL1Cache<>(
                    Clock.fixed(now, ZoneOffset.UTC), Ticker.systemTicker(),
                    new CouponDefinitionL1CacheProperties(
                            java.time.Duration.ofSeconds(10), java.time.Duration.ofMillis(10),
                            java.time.Duration.ofSeconds(60), 10L),
                    loaderExecutor, new SimpleMeterRegistry());

            assertThatThrownBy(() -> cache.get(CouponDefinitionCacheKey.ALL, () -> {
                try {
                    releaseReload.await(LIVENESS_SECONDS, TimeUnit.SECONDS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
                return new CouponDefinitionL1Cache.LoadedValue<>("new", now.plusSeconds(30));
            })).isInstanceOfSatisfying(BusinessException.class, failure -> {
                // DB 는 아직 답하는 중이다. 우리 예산이 끝났을 뿐이라 MySQL 로 귀속하면
                // MySQL 을 건드리지도 않은 Chaos 구간에서 실패 수가 오른다.
                assertThat(failure.getErrorCode().dependency()).isEqualTo(Dependency.NONE);
                assertThat(failure.getErrorCode().getCode()).isEqualTo("COUPON-503");
            });
        } finally {
            releaseReload.countDown();
            loaderExecutor.shutdownNow();
        }
    }

    @Test
    void doesNotAskForAStackTraceOnMitigatedFailures() {
        // 고QPS 조회 경로의 5xx 다. 요청마다 스택을 찍으면 로그 I/O 가 지연을 밀어 올린다.
        assertThat(CouponDefinitionCacheErrorCode.LOAD_UNAVAILABLE.logStackTrace()).isFalse();
        assertThat(CouponDefinitionCacheErrorCode.LOAD_TIMEOUT.logStackTrace()).isFalse();
        assertThat(CouponDefinitionCacheErrorCode.CONTRACT_BROKEN.logStackTrace()).isTrue();
    }

    @Test
    void countsWhyTheFreshValueWasMissing() {
        MeterRegistry registry = new SimpleMeterRegistry();
        AtomicInteger nanos = new AtomicInteger();
        Instant now = Instant.parse("2026-08-28T00:00:00Z");
        CouponDefinitionL1Cache<String> cache = new CouponDefinitionL1Cache<>(
                Clock.fixed(now, ZoneOffset.UTC), nanos::get,
                new CouponDefinitionL1CacheProperties(
                        java.time.Duration.ofNanos(1), java.time.Duration.ofSeconds(1),
                        java.time.Duration.ofSeconds(60), 10L),
                Runnable::run, registry);
        CouponDefinitionCacheKey key = CouponDefinitionCacheKey.ALL;
        cache.get(key, () -> new CouponDefinitionL1Cache.LoadedValue<>("old", now.plusSeconds(30)));
        nanos.set(2);
        cache.get(key, () -> { throw new IllegalStateException("db down"); });

        assertThat(registry.get(CouponDefinitionL1Cache.FALLBACK_METER)
                .tag("reason", "stale").counter().count()).isEqualTo(1d);
    }

    @Test
    void exposesHitStatisticsSoTheCacheCanBeProvenToWork() {
        MeterRegistry registry = new SimpleMeterRegistry();
        Instant now = Instant.parse("2026-08-28T00:00:00Z");
        CouponDefinitionL1Cache<String> cache = new CouponDefinitionL1Cache<>(
                Clock.fixed(now, ZoneOffset.UTC), Ticker.systemTicker(),
                new CouponDefinitionL1CacheProperties(
                        java.time.Duration.ofSeconds(10), java.time.Duration.ofSeconds(1),
                        java.time.Duration.ofSeconds(60), 10L),
                Runnable::run, registry);
        cache.get(CouponDefinitionCacheKey.ALL,
                () -> new CouponDefinitionL1Cache.LoadedValue<>("v", now.plusSeconds(30)));
        cache.get(CouponDefinitionCacheKey.ALL,
                () -> new CouponDefinitionL1Cache.LoadedValue<>("v", now.plusSeconds(30)));

        assertThat(cache.freshStatsView().stats().hitCount()).isEqualTo(1L);
    }
}
