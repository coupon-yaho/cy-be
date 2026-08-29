package com.kafkick.api.coupon.query;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Ticker;
import com.kafkick.core.support.exception.BusinessException;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/** API 인스턴스 안의 쿠폰 정의 L1. 같은 miss는 하나의 비동기 로더로 합친다. */
public final class CouponDefinitionL1Cache<T> {

    private static final double JITTER_RATIO = 0.10d;
    static final String FALLBACK_METER = "coupon.definition.l1.fallback";

    private final Clock clock;
    private final CouponDefinitionL1CacheProperties properties;
    private final Executor loaderExecutor;
    private final MeterRegistry meterRegistry;
    private final AsyncCache<CouponDefinitionCacheKey, FreshValue<T>> fresh;
    private final AsyncCache<CouponDefinitionCacheKey, FreshValue<T>> stale;

    CouponDefinitionL1Cache(
            Clock clock,
            Ticker ticker,
            CouponDefinitionL1CacheProperties properties,
            Executor loaderExecutor,
            MeterRegistry meterRegistry
    ) {
        this.clock = Objects.requireNonNull(clock);
        this.properties = Objects.requireNonNull(properties);
        this.loaderExecutor = Objects.requireNonNull(loaderExecutor);
        this.meterRegistry = Objects.requireNonNull(meterRegistry);
        this.fresh = Caffeine.<CouponDefinitionCacheKey, FreshValue<T>>newBuilder()
                .maximumSize(properties.maximumSize())
                // 히트율은 "L1 을 넣어서 DB 부하가 줄었다"는 주장의 유일한 증거다.
                .recordStats()
                .expireAfter(new Expiry<CouponDefinitionCacheKey, FreshValue<T>>() {
                    @Override
                    public long expireAfterCreate(
                            CouponDefinitionCacheKey key, FreshValue<T> value, long currentTime) {
                        return value.ttlNanos();
                    }

                    @Override
                    public long expireAfterUpdate(
                            CouponDefinitionCacheKey key, FreshValue<T> value,
                            long currentTime, long currentDuration) {
                        return value.ttlNanos();
                    }

                    @Override
                    public long expireAfterRead(
                            CouponDefinitionCacheKey key, FreshValue<T> value,
                            long currentTime, long currentDuration) {
                        return currentDuration;
                    }
                })
                .ticker(ticker)
                .buildAsync();
        this.stale = Caffeine.<CouponDefinitionCacheKey, FreshValue<T>>newBuilder()
                .maximumSize(properties.maximumSize())
                .recordStats()
                .expireAfterWrite(properties.staleMaxAge())
                .ticker(ticker)
                .buildAsync();
    }

    public T get(CouponDefinitionCacheKey key, Supplier<LoadedValue<T>> loader) {
        try {
            CompletableFuture<FreshValue<T>> future = fresh.get(key, (ignored, executor) ->
                    CompletableFuture.supplyAsync(() -> load(key, loader), loaderExecutor));
            FreshValue<T> loaded = future.get(properties.loadTimeout().toNanos(), TimeUnit.NANOSECONDS);
            return loaded.value();
        } catch (TimeoutException | ExecutionException failure) {
            Throwable cause = failure instanceof ExecutionException && failure.getCause() != null
                    ? failure.getCause() : failure;
            if (cause instanceof CouponDefinitionLoadFailure) {
                return staleOrThrow(key, cause, CouponDefinitionCacheErrorCode.LOAD_UNAVAILABLE);
            }
            if (cause instanceof TimeoutException) {
                // DB 가 아니라 우리 예산이 끝난 것이다. MySQL 로 귀속하면 Chaos 판정이 오염된다.
                return staleOrThrow(key, cause, CouponDefinitionCacheErrorCode.LOAD_TIMEOUT);
            }
            throw new BusinessException(CouponDefinitionCacheErrorCode.CONTRACT_BROKEN,
                    "쿠폰 정의 L1 로더 계약 위반", cause);
        } catch (RejectedExecutionException rejection) {
            return staleOrThrow(key, rejection, CouponDefinitionCacheErrorCode.LOAD_TIMEOUT);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            count("interrupted");
            throw new BusinessException(CouponDefinitionCacheErrorCode.LOAD_TIMEOUT,
                    "쿠폰 정의 L1 로드 인터럽트", interrupted);
        }
    }

    private T staleOrThrow(
            CouponDefinitionCacheKey key, Throwable cause, CouponDefinitionCacheErrorCode errorCode) {
        CompletableFuture<FreshValue<T>> staleFuture = stale.getIfPresent(key);
        FreshValue<T> old = staleFuture == null ? null : staleFuture.getNow(null);
        // stale-max-age는 장애 완충 상한이고, 회차 open/close 경계는 목록 정확성 상한이다.
        // 경계를 넘긴 stale은 새 회차를 누락하거나 이미 닫힌 회차를 보일 수 있어 서빙하지 않는다.
        if (old != null && clock.instant().isBefore(old.nextBoundary())) {
            count("stale");
            return old.value();
        }
        count(errorCode == CouponDefinitionCacheErrorCode.LOAD_TIMEOUT ? "timeout" : "unavailable");
        throw new BusinessException(errorCode, "쿠폰 정의 L1 로드 실패", cause);
    }

    private void count(String reason) {
        Counter.builder(FALLBACK_METER)
                .description("L1 이 신선한 값을 못 준 횟수. reason 이 귀속처다")
                .tag("reason", reason)
                .register(meterRegistry)
                .increment();
    }

    private FreshValue<T> load(CouponDefinitionCacheKey key, Supplier<LoadedValue<T>> loader) {
        LoadedValue<T> value;
        try {
            value = Objects.requireNonNull(loader.get(), "loader result");
        } catch (RuntimeException infrastructureFailure) {
            throw new CouponDefinitionLoadFailure(infrastructureFailure);
        }
        Instant now = clock.instant();
        long boundaryNanos = Math.max(1L, durationNanos(now, value.nextBoundary()));
        long cappedNanos = Math.min(properties.ttl().toNanos(), boundaryNanos);
        long jitteredNanos = Math.max(1L, Math.round(cappedNanos * (1d
                + ThreadLocalRandom.current().nextDouble(-JITTER_RATIO, JITTER_RATIO))));
        FreshValue<T> freshValue = new FreshValue<>(value.value(), value.nextBoundary(),
                Math.min(boundaryNanos, jitteredNanos));
        stale.put(key, CompletableFuture.completedFuture(freshValue));
        return freshValue;
    }

    /** Caffeine 통계를 Micrometer 에 붙이기 위한 동기 뷰. 값 조작에는 쓰지 않는다. */
    Cache<CouponDefinitionCacheKey, FreshValue<T>> freshStatsView() {
        return fresh.synchronous();
    }

    private static long durationNanos(Instant start, Instant end) {
        try {
            return java.time.Duration.between(start, end).toNanos();
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    public record LoadedValue<T>(T value, Instant nextBoundary) {
        public LoadedValue {
            Objects.requireNonNull(value);
            Objects.requireNonNull(nextBoundary);
        }
    }

    private record FreshValue<T>(T value, Instant nextBoundary, long ttlNanos) {
    }
}
