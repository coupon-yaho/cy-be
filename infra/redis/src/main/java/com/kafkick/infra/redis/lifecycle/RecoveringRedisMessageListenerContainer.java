package com.kafkick.infra.redis.lifecycle;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.RedisListenerExecutionFailedException;

final class RecoveringRedisMessageListenerContainer
        extends RedisMessageListenerContainer {

    private static final long DEFAULT_RETRY_INTERVAL_MILLIS = 5_000L;

    private static final Logger log = LoggerFactory.getLogger(
            RecoveringRedisMessageListenerContainer.class
    );

    private final long retryIntervalMillis;
    private final ScheduledExecutorService retryExecutor;
    private final AtomicBoolean startupFailureLogged = new AtomicBoolean();
    private ScheduledFuture<?> retryTask;
    private boolean shutdownRequested;

    RecoveringRedisMessageListenerContainer() {
        this(DEFAULT_RETRY_INTERVAL_MILLIS);
    }

    RecoveringRedisMessageListenerContainer(long retryIntervalMillis) {
        if (retryIntervalMillis <= 0) {
            throw new IllegalArgumentException(
                    "Redis 구독 재시도 간격은 양수여야 합니다."
            );
        }
        this.retryIntervalMillis = retryIntervalMillis;
        this.retryExecutor = Executors.newSingleThreadScheduledExecutor(
                runnable -> {
                    Thread thread = new Thread(
                            runnable,
                            "coupon-round-lifecycle-redis-recovery"
                    );
                    thread.setDaemon(true);
                    return thread;
                }
        );
    }

    @Override
    public synchronized void start() {
        if (shutdownRequested) {
            return;
        }
        try {
            super.start();
            startupFailureLogged.set(false);
        } catch (RedisListenerExecutionFailedException exception) {
            if (!hasCause(exception, RedisConnectionFailureException.class)) {
                throw exception;
            }
            recoverLater(exception);
        } catch (IllegalStateException exception) {
            if (!hasCause(exception, TimeoutException.class)) {
                throw exception;
            }
            recoverLater(exception);
        }
    }

    @Override
    public synchronized void stop() {
        cancelRetry();
        super.stop();
    }

    @Override
    public synchronized void destroy() throws Exception {
        shutdownRequested = true;
        cancelRetry();
        retryExecutor.shutdownNow();
        super.destroy();
    }

    private void recoverLater(RuntimeException exception) {
        super.stop();
        if (startupFailureLogged.compareAndSet(false, true)) {
            log.warn(
                    "쿠폰 회차 종료 Redis 구독 초기 연결에 실패했습니다. "
                            + "API 기동은 계속하고 재연결을 시도합니다. cause={}",
                    exception.toString()
            );
        }
        if (retryTask == null || retryTask.isDone()) {
            retryTask = retryExecutor.schedule(
                    this::retryStart,
                    retryIntervalMillis,
                    TimeUnit.MILLISECONDS
            );
        }
    }

    private void retryStart() {
        synchronized (this) {
            retryTask = null;
        }
        start();
    }

    private void cancelRetry() {
        if (retryTask != null) {
            retryTask.cancel(true);
            retryTask = null;
        }
    }

    private static boolean hasCause(
            Throwable exception,
            Class<? extends Throwable> expected
    ) {
        Throwable current = exception;
        while (current != null) {
            if (expected.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

}
