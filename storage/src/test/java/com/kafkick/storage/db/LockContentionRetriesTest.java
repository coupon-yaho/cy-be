package com.kafkick.storage.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.PessimisticLockingFailureException;

/**
 * 검사용 재시도 도구 자체를 검사한다. <b>여기가 멈추면 저장소 검사가 통째로 안 끝난다.</b>
 * 컨테이너가 필요 없어 단위 검사로 둔다.
 */
class LockContentionRetriesTest {

    /**
     * <b>노드가 둘 이상인 순환에서 안 멈추는 것을 막는다.</b> 자기참조만 끊던 판은
     * A → B → A 에서 영영 돌았다. 그러면 상한에 닿지도 못하고 검사가 타임아웃으로만
     * 끝나, 원인이 술어에 있다는 것이 안 보인다.
     */
    @Test
    @DisplayName("A → B → A 순환 원인에서도 판별이 끝난다")
    void terminatesOnMultiNodeCauseCycle() {
        RuntimeException a = new RuntimeException("A");
        RuntimeException b = new RuntimeException("B", a);
        a.initCause(b);

        Assertions.assertThatCode(() -> LockContentionRetries.isLockContention(a))
                .as("안 끝나면 이 검사가 타임아웃으로 죽는다")
                .doesNotThrowAnyException();
        assertThat(LockContentionRetries.isLockContention(a)).isFalse();
    }

    /**
     * 술어는 운영과 같은 넓이여야 한다. {@code CannotAcquireLockException} 만 보던 판은
     * 스프링이 같은 실패를 상위 타입으로 옮기는 경로에서 운영은 물러서는데 검사만 실패했다.
     */
    @Test
    @DisplayName("감싸인 상위 타입 락 경합도 잡는다")
    void detectsWrappedSupertypeLockContention() {
        RuntimeException wrapped = new IllegalStateException(
                "어댑터가 감쌌다", new PessimisticLockingFailureException("lock wait timeout"));

        assertThat(LockContentionRetries.isLockContention(wrapped)).isTrue();
    }

    @Test
    @DisplayName("락 경합이 아니면 다시 하지 않고 그대로 올린다")
    void doesNotRetryUnrelatedFailure() {
        AtomicInteger calls = new AtomicInteger();
        IllegalArgumentException failure = new IllegalArgumentException("규칙 위반");

        assertThatThrownBy(() -> LockContentionRetries.withRetry(() -> {
            calls.incrementAndGet();
            throw failure;
        })).isSameAs(failure);

        assertThat(calls.get()).isEqualTo(1);
    }

    /** <b>계속되는 락 장애까지 성공으로 숨기지 않는다.</b> 상한에 닿으면 그대로 던진다. */
    @Test
    @DisplayName("상한까지 가면 마지막 예외를 그대로 올린다")
    void rethrowsAfterExhaustingAttempts() {
        AtomicInteger calls = new AtomicInteger();

        assertThatThrownBy(() -> LockContentionRetries.withRetry(() -> {
            calls.incrementAndGet();
            throw new CannotAcquireLockException("deadlock");
        })).isInstanceOf(CannotAcquireLockException.class);

        assertThat(calls.get()).isEqualTo(LockContentionRetries.MAX_ATTEMPTS);
    }

    @Test
    @DisplayName("물러섰다가 성공하면 그 값을 돌려준다")
    void returnsValueAfterBackingOff() {
        AtomicInteger calls = new AtomicInteger();

        String result = LockContentionRetries.withRetry(() -> {
            if (calls.incrementAndGet() == 1) {
                throw new CannotAcquireLockException("deadlock");
            }
            return "ok";
        });

        assertThat(result).isEqualTo("ok");
        assertThat(calls.get()).isEqualTo(2);
    }

    /** 물러설 때 실제로 잔다. {@code Thread.yield} 로는 진 쪽들이 같은 순간에 되돌아온다. */
    @Test
    @DisplayName("물러설 때 지터만큼 실제로 기다린다")
    void actuallyBacksOff() {
        AtomicInteger calls = new AtomicInteger();
        long startedAt = System.nanoTime();

        LockContentionRetries.withRetry(() -> {
            if (calls.incrementAndGet() == 1) {
                throw new CannotAcquireLockException("deadlock");
            }
            return "ok";
        });

        assertThat(Duration.ofNanos(System.nanoTime() - startedAt))
                .isGreaterThanOrEqualTo(Duration.ofNanos(1_000_000L));
    }
}
