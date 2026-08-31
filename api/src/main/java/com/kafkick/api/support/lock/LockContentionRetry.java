package com.kafkick.api.support.lock;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.stereotype.Component;

/**
 * 락 경합으로 물러섰다가 다시 해 본다. <b>발급·사용·사용취소·발급취소가 같이 쓴다.</b>
 *
 * <h2>왜 한 군데로 모았나</h2>
 *
 * <p>처음에는 발급 경로에만 두었다. 그런데 사용 경로가 CI 에서 같은 증상으로 깨졌다 —
 * 같은 멱등키로 열 번 동시에 누르면 {@code MySQLTransactionRollbackException} 이 올라온다.
 * 네 경로가 결국 <b>같은 재고 행과 같은 멱등 행</b>을 치므로, 판별 기준과 물러서는 방식이
 * 갈리면 한쪽만 고쳐 두고 나머지는 못 고친 채로 남는다.
 *
 * <h2>여기 두는 이유 — 트랜잭션 바깥이어야 한다</h2>
 *
 * <p><b>롤백된 트랜잭션 안에서는 재시도해 봐야 소용이 없다.</b> MySQL 이 데드락 피해자를
 * 고르면 그 트랜잭션은 이미 통째로 되감겼다. 그래서 이 재시도는 <b>트랜잭션이 열리기 전</b>,
 * 즉 컨트롤러·코디네이터 층에서만 감싼다.
 *
 * <p>사용·취소 경로가 특히 그렇다. 그쪽 멱등 실행기는
 * {@code @Transactional(propagation = NEVER)} 이고 선점만 {@code REQUIRES_NEW} 로 따로
 * 커밋하는데, <b>실패하면 그 선점을 스스로 풀어 준다</b>
 * ({@code IdempotencyExecutionService#releaseFailedClaim}). 그래서 다음 시도가 자기가
 * 남긴 선점에 막히지 않는다 — 이걸 재기 전에는 반대로 알고 있었다.
 *
 * <h2>이 클래스가 보장하지 않는 것</h2>
 *
 * <p><b>예산은 응답 상한이 아니다.</b> 진행 중인 DB 시도를 중단시키지 못하므로, 첫 시도가
 * 락 대기로 50초를 쓰면 응답은 이미 예산을 크게 넘긴다. 예산이 보장하는 것은 하나 —
 * <b>예산이 끝난 뒤에는 새 시도를 시작하지 않는다.</b>
 */
@Component
public class LockContentionRetry {

    private static final Logger log = LoggerFactory.getLogger(LockContentionRetry.class);

    /** 순환 참조를 물고 있는 원인 사슬에서 안 멈추게 하는 상한. */
    private static final int CAUSE_CHAIN_LIMIT = 16;

    private static final long BACKOFF_MIN_NANOS = 1_000_000L;
    private static final long BACKOFF_MAX_NANOS = 5_000_000L;

    private final LockRetryMeters meters;
    private final LockRetryProperties properties;

    public LockContentionRetry(LockRetryMeters meters, LockRetryProperties properties) {
        this.meters = Objects.requireNonNull(meters, "meters");
        this.properties = Objects.requireNonNull(properties, "properties");
    }

    /**
     * {@code action} 을 부르고, 락 경합으로 실패하면 한도 안에서 다시 부른다.
     *
     * @param operation 지표 태그로 나갈 경로 이름. 요청에서 온 값을 넣지 않는다
     * @param contextForLog 물러서거나 포기할 때만 부른다. 정상 응답에서는 안 부른다.
     *                      <b>회원 이름·연락처를 넣지 않는다</b> — 식별자까지만
     * @param action 트랜잭션을 여는 호출. <b>이미 열린 트랜잭션 안에서 부르면 안 된다</b>
     */
    public <T> T call(String operation, Supplier<String> contextForLog, Supplier<T> action) {
        int maxAttempts = properties.maxAttempts();
        long deadline = System.nanoTime() + properties.budget().toNanos();
        // 요청 단위로 센다. 물러섬마다 올리면 두 번 물러선 요청이 둘로 세어지고,
        // 끝내 실패한 요청이 recovered 와 exhausted 에 동시에 들어간다.
        boolean retried = false;
        for (int attempt = 1; ; attempt++) {
            try {
                T result = action.get();
                if (retried) {
                    meters.recovered(operation);
                }
                return result;
            } catch (RuntimeException failure) {
                if (!causedByLockContention(failure)) {
                    throw failure;
                }
                long remaining = deadline - System.nanoTime();
                if (attempt >= maxAttempts || remaining <= 0L) {
                    throw giveUp(operation, failure, attempt, contextForLog);
                }
                retried = true;
                // 부하 구간에 요청마다 warn 을 찍으면 로그 I/O 가 측정값을 오염시킨다.
                log.debug("{}이(가) 락 경합으로 물러섭니다. attempt={}/{} {}",
                        operation, attempt, maxAttempts, contextForLog.get());
                // 남은 예산 안에서만 기다린다. 예산을 넘겨 자면 깨어난 뒤에 시도가 하나 더
                // 시작되는데, 그것이 락 대기 타임아웃만큼 더 걸릴 수 있다.
                LockSupport.parkNanos(Math.min(remaining, backoffNanos()));
                if (System.nanoTime() >= deadline) {
                    throw giveUp(operation, failure, attempt, contextForLog);
                }
            }
        }
    }

    /** 물러서는 시간에 지터를 준다. 진 쪽들이 같은 순간에 되돌아와 다시 부딪히지 않게 한다. */
    private static long backoffNanos() {
        return ThreadLocalRandom.current().nextLong(BACKOFF_MIN_NANOS, BACKOFF_MAX_NANOS);
    }

    /**
     * 상한이나 시간 예산에 닿았다. <b>사실만 남긴다</b> — 원인이 반복 데드락인지 지속
     * 병목인지는 이 코드가 구분하지 못한다. 그 판정은 로그와 지표를 본 사람이 한다.
     *
     * <p>바깥 예외를 그대로 돌려준다. 어댑터가 붙인 맥락을 벗기지 않는다.
     */
    private RuntimeException giveUp(
            String operation,
            RuntimeException failure,
            int attempt,
            Supplier<String> contextForLog
    ) {
        meters.exhausted(operation);
        log.warn("{}이(가) 락 경합으로 {}회 만에 포기했습니다. {}",
                operation, attempt, contextForLog.get());
        return failure;
    }

    /**
     * <b>원인 사슬을 훑는다.</b> 저장소 어댑터가 {@code DataAccessException} 을
     * {@code CouponPersistenceException}·{@code IdempotencyPersistenceException} 으로 감싸므로,
     * <b>운영에서 오는 락 경합은 대개 그 안에 들어 있다.</b> 처음에는 원본 타입만 잡았는데
     * 그러면 감싸인 쪽이 안 걸려서 재시도가 사실상 안 돌았다(리뷰가 잡았다).
     *
     * <p>감싸이지 않은 경우도 있다 — JPA 가 INSERT 를 커밋 시점으로 미루면 어댑터의
     * {@code catch} 밖에서 터진다. 그래서 <b>양쪽을 다 본다.</b>
     *
     * <p>{@code PessimisticLockingFailureException} 하나로 데드락(1213)과 락 대기
     * 타임아웃(1205)을 함께 잡는다. 스프링이 둘 다 {@code CannotAcquireLockException} 으로
     * 옮기는 것을 재서 확인했다. 둘 다 물러섰다 다시 하면 풀릴 수 있는 실패다.
     *
     * <p>락 경합이 아닌 실패는 여기서 {@code false} 라 곧바로 원형 그대로 다시 던져진다.
     * 넓게 잡아 아무 실패나 재시도하면 진짜 결함을 세 번 반복하고 응답만 느려진다.
     */
    private static boolean causedByLockContention(Throwable failure) {
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
