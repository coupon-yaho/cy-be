package com.kafkick.core.coupon.v2;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.coupon.exception.CouponIssueErrorCode;
import com.kafkick.core.coupon.v2.port.CouponRoundIssuanceDefinitionRepository;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.support.exception.BusinessException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CouponRoundIssuanceDefinitionCacheTest {

    @Test
    @DisplayName("같은 회차를 두 번째로 읽을 때는 저장소를 다시 부르지 않는다")
    void readsTheRepositoryOnlyOnceForACachedRound() {
        CountingRepository repository = new CountingRepository(EngineVersion.V2, 0L);
        CouponRoundIssuanceDefinitionCache cache =
                new CouponRoundIssuanceDefinitionCache(repository);

        CouponRoundIssuanceDefinition first = cache.get(10L);
        CouponRoundIssuanceDefinition second = cache.get(10L);

        assertThat(second).isSameAs(first);
        assertThat(repository.calls()).hasValue(1);
    }

    @Test
    @DisplayName("같은 회차로 몰린 스레드가 저장소를 한 번만 부르고 서로 같은 정의를 받는다")
    void concurrentGetsForOneRoundLoadExactlyOnce() throws Exception {
        int threads = 32;
        // 로드가 느려도 나머지 스레드가 각자 DB 왕복을 만들면 안 된다. 그 왕복은
        // coupons 한 행의 X-락을 두고 커넥션을 점유한 채 줄을 선다.
        CountingRepository repository = new CountingRepository(EngineVersion.V2, 200L);
        CouponRoundIssuanceDefinitionCache cache =
                new CouponRoundIssuanceDefinitionCache(repository);

        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        List<CouponRoundIssuanceDefinition> results = new CopyOnWriteArrayList<>();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int i = 0; i < threads; i++) {
                pool.execute(() -> {
                    try {
                        start.await();
                        results.add(cache.get(10L));
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertThat(done.await(20, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }

        assertThat(repository.calls()).hasValue(1);
        assertThat(results).hasSize(threads);
        CouponRoundIssuanceDefinition settled = cache.get(10L);
        assertThat(results).allSatisfy(definition ->
                assertThat(definition).isSameAs(settled));
    }

    @Test
    @DisplayName("한 회차를 로드하는 동안 다른 회차 조회가 막히지 않는다")
    void loadingOneRoundDoesNotBlockAnotherRound() throws Exception {
        CountDownLatch releaseSlowLoad = new CountDownLatch(1);
        BlockingRepository repository = new BlockingRepository(10L, releaseSlowLoad);
        CouponRoundIssuanceDefinitionCache cache =
                new CouponRoundIssuanceDefinitionCache(repository);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            pool.execute(() -> cache.get(10L));
            assertThat(repository.enteredSlowLoad().await(5, TimeUnit.SECONDS)).isTrue();

            Future<CouponRoundIssuanceDefinition> other =
                    pool.submit(() -> cache.get(11L));
            assertThat(other.get(5, TimeUnit.SECONDS).couponRoundId()).isEqualTo(11L);
        } finally {
            releaseSlowLoad.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    @DisplayName("로더가 실패해도 같은 회차를 기다리던 스레드가 묶이지 않는다")
    void aFailedLoadDoesNotStrandWaitersOnTheRound() {
        CountDownLatch enteredLoad = new CountDownLatch(1);
        CountDownLatch releaseLoad = new CountDownLatch(1);
        FailingRepository repository = new FailingRepository(enteredLoad, releaseLoad);
        CouponRoundIssuanceDefinitionCache cache =
                new CouponRoundIssuanceDefinitionCache(repository);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<?> loader = pool.submit(() -> cache.get(10L));
            assertThat(enteredLoad.await(5, TimeUnit.SECONDS)).isTrue();

            // 로더가 실패하는 도중에 들어온 스레드. 예외를 받든 스스로 재로드하든
            // 정해진 시간 안에 끝나야 한다 — join 에 갇히면 이 단언이 깨진다.
            Future<?> waiter = pool.submit(() -> cache.get(10L));
            releaseLoad.countDown();

            assertThatThrownBy(loader::get).hasRootCauseInstanceOf(IllegalStateException.class);
            assertThatCode(() -> {
                try {
                    waiter.get(5, TimeUnit.SECONDS);
                } catch (ExecutionException expected) {
                    assertThat(expected).hasRootCauseInstanceOf(IllegalStateException.class);
                }
            }).doesNotThrowAnyException();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        } finally {
            releaseLoad.countDown();
            pool.shutdownNow();
        }

        // 실패는 캐시에 남지 않는다 — 다음 요청이 다시 로드한다.
        assertThat(repository.calls()).hasValueGreaterThanOrEqualTo(1);
    }

    @Test
    @DisplayName("없는 회차는 캐시에 남기지 않고 매번 저장소를 다시 본다")
    void doesNotCacheAMissingRound() {
        CountingRepository repository = new CountingRepository(null, 0L);
        CouponRoundIssuanceDefinitionCache cache =
                new CouponRoundIssuanceDefinitionCache(repository);

        assertThatThrownBy(() -> cache.get(10L))
                .isInstanceOfSatisfying(BusinessException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(CouponIssueErrorCode.COUPON_ROUND_NOT_FOUND));
        assertThatThrownBy(() -> cache.get(10L))
                .isInstanceOf(BusinessException.class);
        assertThat(repository.calls()).hasValue(2);
    }

    /** 지정한 회차의 로드만 붙잡아 둔다. */
    private static final class BlockingRepository
            implements CouponRoundIssuanceDefinitionRepository {

        private final long slowRoundId;
        private final CountDownLatch release;
        private final CountDownLatch enteredSlowLoad = new CountDownLatch(1);

        private BlockingRepository(long slowRoundId, CountDownLatch release) {
            this.slowRoundId = slowRoundId;
            this.release = release;
        }

        @Override
        public Optional<CouponRoundIssuanceDefinition> findById(long couponRoundId) {
            return Optional.empty();
        }

        @Override
        public Optional<CouponRoundIssuanceDefinition> lockAndFindById(long couponRoundId) {
            if (couponRoundId == slowRoundId) {
                enteredSlowLoad.countDown();
                try {
                    release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            return Optional.of(new CouponRoundIssuanceDefinition(
                    couponRoundId, 7, EngineVersion.V2));
        }

        @Override
        public boolean updateEngineVersionWhenNotOpen(
                long couponRoundId,
                EngineVersion engineVersion
        ) {
            throw new UnsupportedOperationException();
        }

        private CountDownLatch enteredSlowLoad() {
            return enteredSlowLoad;
        }
    }

    /** 로드가 반드시 실패한다. 실패 타이밍을 래치로 잡는다. */
    private static final class FailingRepository
            implements CouponRoundIssuanceDefinitionRepository {

        private final CountDownLatch enteredLoad;
        private final CountDownLatch releaseLoad;
        private final AtomicInteger calls = new AtomicInteger();

        private FailingRepository(CountDownLatch enteredLoad, CountDownLatch releaseLoad) {
            this.enteredLoad = enteredLoad;
            this.releaseLoad = releaseLoad;
        }

        @Override
        public Optional<CouponRoundIssuanceDefinition> findById(long couponRoundId) {
            return Optional.empty();
        }

        @Override
        public Optional<CouponRoundIssuanceDefinition> lockAndFindById(long couponRoundId) {
            calls.incrementAndGet();
            enteredLoad.countDown();
            try {
                releaseLoad.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("db down");
        }

        @Override
        public boolean updateEngineVersionWhenNotOpen(
                long couponRoundId,
                EngineVersion engineVersion
        ) {
            throw new UnsupportedOperationException();
        }

        private AtomicInteger calls() {
            return calls;
        }
    }

    private static final class CountingRepository
            implements CouponRoundIssuanceDefinitionRepository {

        private final EngineVersion nullableEngineVersion;
        private final long loadDelayMillis;
        private final AtomicInteger calls = new AtomicInteger();

        private CountingRepository(EngineVersion nullableEngineVersion, long loadDelayMillis) {
            this.nullableEngineVersion = nullableEngineVersion;
            this.loadDelayMillis = loadDelayMillis;
        }

        @Override
        public Optional<CouponRoundIssuanceDefinition> findById(long couponRoundId) {
            return Optional.empty();
        }

        @Override
        public Optional<CouponRoundIssuanceDefinition> lockAndFindById(long couponRoundId) {
            calls.incrementAndGet();
            if (loadDelayMillis > 0) {
                try {
                    Thread.sleep(loadDelayMillis);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            return nullableEngineVersion == null
                    ? Optional.empty()
                    : Optional.of(new CouponRoundIssuanceDefinition(
                            couponRoundId, 7, nullableEngineVersion));
        }

        @Override
        public boolean updateEngineVersionWhenNotOpen(
                long couponRoundId,
                EngineVersion engineVersion
        ) {
            throw new UnsupportedOperationException();
        }

        private AtomicInteger calls() {
            return calls;
        }
    }
}
