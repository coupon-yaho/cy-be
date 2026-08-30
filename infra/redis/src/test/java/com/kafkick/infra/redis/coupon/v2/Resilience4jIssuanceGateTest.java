package com.kafkick.infra.redis.coupon.v2;

import java.time.Duration;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;

import com.kafkick.core.coupon.v2.port.ClaimCommand;
import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.core.coupon.v2.IssuanceGateCircuitOpenException;
import com.kafkick.core.coupon.v2.port.CompensateOutcome;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class Resilience4jIssuanceGateTest {

    @Test
    void opensAfterFiveConsecutiveRedisFailuresWithoutRetryingClaim() {
        IssuanceGatePort delegate = mock(IssuanceGatePort.class);
        ClaimCommand command = new ClaimCommand(1, 2, 1, "key", "token");
        when(delegate.claim(command)).thenThrow(new RedisConnectionFailureException("down"));
        Resilience4jIssuanceGate gate = new Resilience4jIssuanceGate(delegate, breaker());

        for (int attempt = 0; attempt < 5; attempt++) {
            assertThatThrownBy(() -> gate.claim(command))
                    .isInstanceOf(RedisConnectionFailureException.class);
        }

        assertThatThrownBy(() -> gate.claim(command)).isInstanceOf(IssuanceGateCircuitOpenException.class);
        verify(delegate, times(5)).claim(command);
    }

    @Test
    void skipsCompensationWhenTheCircuitIsOpenWithoutPretendingItReverted() {
        IssuanceGatePort delegate = mock(IssuanceGatePort.class);
        CircuitBreaker breaker = breaker();
        breaker.transitionToOpenState();
        Resilience4jIssuanceGate gate = new Resilience4jIssuanceGate(delegate, breaker);

        CompensateOutcome outcome = gate.compensate(1L, 2L, "token");

        assertThat(outcome).isEqualTo(CompensateOutcome.NOT_ATTEMPTED_CIRCUIT_OPEN);
        verify(delegate, times(0)).compensate(1L, 2L, "token");
    }

    /**
     * <b>보상이 half-open 의 시험 호출 자리를 먹지 않는다.</b> 허가를 얻어 놓고 성패를
     * 기록하지 않으면 차단기가 복구를 학습하지 못하고, 그 사이 선점은 전부 즉시 503 이 된다.
     * 보상은 상태만 읽고 지나가야 한다 — 시험 호출 자리는 선점의 몫이다.
     */
    @Test
    void compensatingInHalfOpenLeavesTheProbeBudgetForClaim() {
        IssuanceGatePort delegate = mock(IssuanceGatePort.class);
        CircuitBreaker breaker = breaker();
        breaker.transitionToOpenState();
        breaker.transitionToHalfOpenState();
        Resilience4jIssuanceGate gate = new Resilience4jIssuanceGate(delegate, breaker);

        gate.compensate(1L, 2L, "token");

        verify(delegate, times(1)).compensate(1L, 2L, "token");
        assertThat(breaker.tryAcquirePermission())
                .as("선점이 쓸 시험 호출 자리가 그대로 남아 있어야 한다")
                .isTrue();
    }

    private static CircuitBreaker breaker() {
        return CircuitBreaker.of("test", CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(5)
                .minimumNumberOfCalls(5)
                .failureRateThreshold(100)
                .waitDurationInOpenState(Duration.ofSeconds(5))
                .permittedNumberOfCallsInHalfOpenState(1)
                .recordException(failure -> failure instanceof RedisConnectionFailureException)
                .build());
    }
}
