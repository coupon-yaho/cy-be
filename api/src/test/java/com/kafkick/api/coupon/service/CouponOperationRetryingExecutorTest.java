package com.kafkick.api.coupon.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.CannotAcquireLockException;

import com.kafkick.api.support.lock.LockContentionRetry;
import com.kafkick.api.support.lock.LockRetryMeters;
import com.kafkick.api.support.lock.LockRetryProperties;
import com.kafkick.core.coupon.exception.IdempotencyPersistenceException;
import com.kafkick.core.coupon.service.CouponOperationExecutionService;
import com.kafkick.core.coupon.service.result.CouponCancelUseResult;
import com.kafkick.core.coupon.service.result.CouponUseResult;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * 사용·취소가 락 경합에서 물러섰다 다시 하는지 본다.
 *
 * <p>발급만 고쳐 두었을 때 이쪽이 그대로 남아 있었다. 세 경로가 같은 재고 행을 치므로
 * 발급에만 처치를 두면 부하 구간에 사용·취소가 데드락을 그대로 사용자에게 돌려준다.
 */
class CouponOperationRetryingExecutorTest {

    private static final Long ISSUANCE_ID = 10L;
    private static final Long MEMBER_ID = 20L;
    private static final String IDEMPOTENCY_KEY = "idem-key";

    private final CouponOperationExecutionService delegate =
            mock(CouponOperationExecutionService.class);
    private final MeterRegistry registry = new SimpleMeterRegistry();

    private CouponOperationRetryingExecutor executor() {
        return new CouponOperationRetryingExecutor(
                delegate,
                new LockContentionRetry(
                        new LockRetryMeters(registry),
                        new LockRetryProperties(3, Duration.ofSeconds(5))
                )
        );
    }

    /**
     * <b>어댑터가 감싼 모양 그대로 넣는다.</b> 운영에서 오는 것이 그 모양이다 — CI 가 남긴
     * 사슬도 {@code IdempotencyPersistenceException → CannotAcquireLockException} 이었다.
     * 감싸이지 않은 원본만 잡던 시절에는 재시도가 사실상 안 돌았다.
     */
    private static RuntimeException wrappedDeadlock() {
        return new IdempotencyPersistenceException(
                "멱등 기록 저장에 실패했습니다.",
                new CannotAcquireLockException("Deadlock found when trying to get lock"));
    }

    @Test
    @DisplayName("사용이 감싸인 데드락에 물러섰다가 다시 해서 성공한다")
    void retriesUseAfterWrappedDeadlock() {
        CouponUseResult expected = mock(CouponUseResult.class);
        when(delegate.use(ISSUANCE_ID, MEMBER_ID, 1000, IDEMPOTENCY_KEY))
                .thenThrow(wrappedDeadlock())
                .thenReturn(expected);

        CouponUseResult actual =
                executor().use(ISSUANCE_ID, MEMBER_ID, 1000, IDEMPOTENCY_KEY);

        assertThat(actual).isSameAs(expected);
        verify(delegate, times(2)).use(ISSUANCE_ID, MEMBER_ID, 1000, IDEMPOTENCY_KEY);
        assertThat(counter("use", "recovered")).isEqualTo(1.0);
        assertThat(counter("use", "exhausted")).isEqualTo(0.0);
    }

    /**
     * <b>락 경합이 아니면 손대지 않는다.</b> 넓게 잡아 아무 실패나 다시 하면 진짜 결함을
     * 세 번 반복하고 응답만 느려진다.
     */
    @Test
    @DisplayName("락 경합이 아닌 실패는 다시 하지 않고 그대로 올린다")
    void doesNotRetryUnrelatedFailure() {
        IllegalStateException failure = new IllegalStateException("도메인 규칙 위반");
        when(delegate.cancelUse(ISSUANCE_ID, MEMBER_ID, IDEMPOTENCY_KEY))
                .thenThrow(failure);

        assertThatThrownBy(() ->
                executor().cancelUse(ISSUANCE_ID, MEMBER_ID, IDEMPOTENCY_KEY))
                .isSameAs(failure);

        verify(delegate, times(1)).cancelUse(ISSUANCE_ID, MEMBER_ID, IDEMPOTENCY_KEY);
        assertThat(counter("cancel-use", "recovered")).isEqualTo(0.0);
        assertThat(counter("cancel-use", "exhausted")).isEqualTo(0.0);
    }

    /**
     * <b>끝내 안 되면 원형 그대로 던진다.</b> 어댑터가 붙인 맥락을 벗기면 무엇이 실패했는지
     * 로그에서 사라진다.
     */
    @Test
    @DisplayName("상한까지 가면 마지막 예외를 그대로 올리고 exhausted 로만 센다")
    void rethrowsAfterExhaustingAttempts() {
        when(delegate.cancel(ISSUANCE_ID, MEMBER_ID, IDEMPOTENCY_KEY))
                .thenThrow(wrappedDeadlock());

        assertThatThrownBy(() ->
                executor().cancel(ISSUANCE_ID, MEMBER_ID, IDEMPOTENCY_KEY))
                .isInstanceOf(IdempotencyPersistenceException.class)
                .hasRootCauseInstanceOf(CannotAcquireLockException.class);

        verify(delegate, times(3)).cancel(ISSUANCE_ID, MEMBER_ID, IDEMPOTENCY_KEY);
        assertThat(counter("cancel", "exhausted")).isEqualTo(1.0);
        assertThat(counter("cancel", "recovered")).isEqualTo(0.0);
    }

    /**
     * 경로마다 태그가 갈려야 어느 쪽이 부딪히는지 보인다. 넷이 한 시계열로 합쳐지면
     * 지표를 봐도 손댈 곳을 못 고른다.
     */
    @Test
    @DisplayName("취소는 사용과 다른 태그로 센다")
    void tagsEachOperationSeparately() {
        CouponCancelUseResult result = mock(CouponCancelUseResult.class);
        when(delegate.cancelUse(ISSUANCE_ID, MEMBER_ID, IDEMPOTENCY_KEY))
                .thenThrow(wrappedDeadlock())
                .thenReturn(result);

        executor().cancelUse(ISSUANCE_ID, MEMBER_ID, IDEMPOTENCY_KEY);

        assertThat(counter("cancel-use", "recovered")).isEqualTo(1.0);
        assertThat(counter("use", "recovered")).isEqualTo(0.0);
        assertThat(counter("cancel", "recovered")).isEqualTo(0.0);
        assertThat(counter("issue", "recovered")).isEqualTo(0.0);
    }

    /**
     * <b>한 번도 안 부딪힌 경로도 0 으로 보여야 한다.</b> 첫 증가 때 만들면 대시보드에
     * "데이터 없음" 이 뜨는데, 그건 "안 부딪혔다" 와 아주 다른 말이다.
     */
    @Test
    @DisplayName("네 경로의 카운터가 아무 일 없이도 미리 서 있다")
    void registersEveryOperationUpFront() {
        executor();

        for (String operation : List.of("issue", "use", "cancel-use", "cancel")) {
            assertThat(counter(operation, "recovered")).isEqualTo(0.0);
            assertThat(counter(operation, "exhausted")).isEqualTo(0.0);
        }
    }

    private double counter(String operation, String outcome) {
        return registry.get("coupon.lock.retry")
                .tag("operation", operation)
                .tag("outcome", outcome)
                .counter().count();
    }
}
