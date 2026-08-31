package com.kafkick.storage.db;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import org.springframework.dao.PessimisticLockingFailureException;

/**
 * 저장소 검사에서 락 경합에 물러섰다 다시 하는 도구.
 *
 * <h2>왜 검사에도 두는가</h2>
 *
 * <p>열 스레드가 한 행을 동시에 치면 MySQL 이 데드락 피해자를 고른다. 그건 결함이 아니라
 * 정상 동작이고, 운영에서는 {@code LockContentionRetry} 가 받아 다시 한다. 검사에만 그
 * 처치가 없으면 <b>운영에서 안 나는 실패가 CI 에서만 난다</b> — 실제로 그렇게 깨졌다.
 *
 * <h2>왜 한 곳인가</h2>
 *
 * <p>발급과 사용 두 검사가 같은 30줄을 각자 들고 있었다. 그 상태로 두면 한쪽만 고쳐 두는
 * 자리가 된다 — 운영 쪽을 한 곳으로 모으면서 검사 쪽을 둘로 늘리는 것은 앞뒤가 안 맞는다.
 *
 * <h2>술어를 운영과 맞춘다</h2>
 *
 * <p>전에는 {@code CannotAcquireLockException} 만 봤다. 그건 운영이 보는
 * {@link PessimisticLockingFailureException} 의 <b>하위 타입</b>이라 더 좁다 — 스프링이
 * 같은 실패를 상위 타입으로 옮기는 경로에서는 <b>운영은 물러서는데 검사만 실패한다.</b>
 * 넓은 쪽으로 맞춘다.
 *
 * <h2>물러서는 방식도 맞춘다</h2>
 *
 * <p>{@code Thread.yield()} 를 쓰던 것을 운영과 같은 1~5ms 지터 park 로 바꾼다.
 * {@code yield} 는 열 스레드 경합에서 사실상 안 물러서서, 진 쪽들이 같은 순간에 되돌아와
 * 세 번 다 부딪힐 수 있다.
 */
public final class LockContentionRetries {

    /** 물러서는 상한. 계속되는 락 장애까지 성공으로 숨기지 않으려고 낮게 둔다. */
    public static final int MAX_ATTEMPTS = 3;

    /**
     * 사슬을 훑는 깊이 상한. <b>자기참조만 끊으면 부족하다</b> — A → B → A 처럼 노드가
     * 둘 이상인 순환에서는 그 검사가 안 걸려 검사 스레드가 영영 안 끝난다. 운영 쪽
     * {@code LockContentionRetry} 는 이 상한을 갖고 있었는데, 여기로 옮기면서 빠뜨렸다.
     */
    private static final int CAUSE_CHAIN_LIMIT = 16;

    private static final long BACKOFF_MIN_NANOS = 1_000_000L;
    private static final long BACKOFF_MAX_NANOS = 5_000_000L;

    private LockContentionRetries() {
    }

    /** 락 경합이면 물러섰다 다시 한다. 상한에 닿으면 마지막 예외를 그대로 던진다. */
    public static <T> T withRetry(Supplier<T> action) {
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                return action.get();
            } catch (RuntimeException exception) {
                if (!isLockContention(exception) || attempt == MAX_ATTEMPTS) {
                    throw exception;
                }
                LockSupport.parkNanos(ThreadLocalRandom.current()
                        .nextLong(BACKOFF_MIN_NANOS, BACKOFF_MAX_NANOS));
            }
        }
        throw new IllegalStateException("도달할 수 없는 데드락 재시도 상태입니다.");
    }

    /**
     * <b>원인 사슬을 훑는다.</b> 저장소 어댑터가 {@code DataAccessException} 을
     * {@code CouponPersistenceException}·{@code IdempotencyPersistenceException} 으로
     * 감싸므로 바깥 타입만 보면 못 잡는다. CI 가 남긴 사슬이 정확히 그 모양이었다 —
     * {@code IdempotencyPersistenceException → CannotAcquireLockException →
     * MySQLTransactionRollbackException}.
     *
     * <p>깊이를 {@link #CAUSE_CHAIN_LIMIT} 로 끊는다. 순환 사슬에서 안 멈추게 하려는 것이다.
     */
    public static boolean isLockContention(Throwable failure) {
        Throwable cause = failure;
        for (int depth = 0; cause != null && depth < CAUSE_CHAIN_LIMIT; depth++) {
            if (cause instanceof PessimisticLockingFailureException) {
                return true;
            }
            if (cause.getCause() == cause) {
                return false;
            }
            cause = cause.getCause();
        }
        return false;
    }
}
