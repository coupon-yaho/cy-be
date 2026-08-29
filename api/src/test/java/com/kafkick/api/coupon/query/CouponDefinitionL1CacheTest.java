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
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
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
     * <p>이 값을 5초에서 올린 적이 있는데, <b>그때의 진단은 틀렸다.</b> 부하로 스레드가 밀린
     * 것이 아니라 리로드가 아예 제출되지 않고 있었다(30초로 올려도 같은 자리에서 죽었다).
     * 원인과 처방은 {@code returnsStaleValueWhenReloadExceedsBudget} 안에 적었다.
     * 여기 숫자는 그 뒤로도 넉넉히 두지만, 넉넉함이 무엇을 가려 주지는 않는다.
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
        // 첫 로드만 호출 스레드에서 돌린다. Caffeine 은 로드가 '완료될 때' 의 ticker 로 만료
        // 시각을 등록하는데, 그 등록이 다른 스레드에서 일어나면 아래 nanos.set(2) 와 순서가
        // 뒤집힐 수 있다. 뒤집히면 만료 시각도 2 이후로 밀려 두 번째 get 이 만료를 못 보고,
        // 리로드가 아예 제출되지 않는다 — 이 테스트가 보려는 것과 무관한 경합이다.
        AtomicBoolean reloadOnItsOwnThread = new AtomicBoolean();
        Executor stagedExecutor = task -> {
            if (reloadOnItsOwnThread.get()) {
                loaderExecutor.execute(task);
            } else {
                task.run();
            }
        };
        try {
            CouponDefinitionL1Cache<String> cache = new CouponDefinitionL1Cache<>(
                    Clock.fixed(now, ZoneOffset.UTC), nanos::get,
                    new CouponDefinitionL1CacheProperties(
                            java.time.Duration.ofNanos(1), java.time.Duration.ofMillis(10),
                            java.time.Duration.ofSeconds(60), 10L),
                    stagedExecutor, new SimpleMeterRegistry());
            CouponDefinitionCacheKey key = CouponDefinitionCacheKey.ALL;
            assertThat(cache.get(key, () -> new CouponDefinitionL1Cache.LoadedValue<>(
                    "old", now.plusSeconds(30)))).isEqualTo("old");
            reloadOnItsOwnThread.set(true);
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
