package com.kafkick.infra.redis.coupon.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.context.annotation.ImportCandidates;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import com.kafkick.core.admin.preparation.V2AdminPreparationReader;
import com.kafkick.core.admin.stock.V2AdminStockReader;
import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.core.coupon.v2.port.ClaimCommand;
import com.kafkick.core.coupon.v2.port.ClaimResult;
import com.kafkick.core.coupon.v2.IssuanceGateCircuitOpenException;
import com.kafkick.core.coupon.v2.port.RestorationHaltStore;
import com.kafkick.core.coupon.v2.port.RestoreOutcome;
import com.kafkick.core.coupon.v2.port.V2RestorationMeters;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;

import org.springframework.data.redis.RedisConnectionFailureException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * 조립은 <b>기동에서만</b> 증명된다. 통합 테스트는 어댑터를 손으로 {@code new} 하므로
 * imports 에서 줄이 빠지거나 조건이 뒤집혀도 전부 통과한다 — 그 상태는 예외도 로그도 없이
 * 첫 발급 요청에서 "게이트 빈 없음" 으로만 드러난다.
 *
 * <p>조건을 양쪽으로 본다: Redis 통로가 <b>있을 때 등록</b>되고 <b>없을 때 물러나는지</b>.
 * 한쪽만 보면 조건을 통째로 지워도 초록이다.
 */
class IssuanceGateRedisAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(IssuanceGateRedisAutoConfiguration.class));

    @Test
    @DisplayName("자동설정으로 등록돼 있어야 한다 — imports 에서 빠지면 게이트 빈이 통째로 사라진다")
    void isRegisteredAsAutoConfiguration() {
        assertThat(ImportCandidates.load(AutoConfiguration.class, getClass().getClassLoader()))
                .as("아래 테스트들은 이 클래스를 직접 등록하므로 imports 에서 빠져도 통과한다")
                .contains(IssuanceGateRedisAutoConfiguration.class.getName());
    }

    @Test
    @DisplayName("Redis 자동설정과 함께면 게이트가 조립된다 — 순서가 어긋나면 조건이 먼저 평가돼 물러난다")
    void registersGateWithRedisAutoConfiguration() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        DataRedisAutoConfiguration.class, IssuanceGateRedisAutoConfiguration.class))
                .run(context -> {
                    assertThat(context).hasSingleBean(IssuanceScriptRunner.class);
                    assertThat(context).hasSingleBean(IssuanceGatePort.class);
                    assertThat(context).hasSingleBean(V2AdminPreparationReader.class);
                    assertThat(context).hasSingleBean(V2AdminStockReader.class);
                    assertThat(context.getBean(IssuanceGatePort.class))
                            .isInstanceOf(Resilience4jIssuanceGate.class);
                    assertThat(context.getBean(V2AdminStockReader.class))
                            .isInstanceOf(RedisV2AdminStockReader.class);
                    assertThat(context.getBean(V2AdminPreparationReader.class))
                            .isInstanceOf(RedisV2AdminPreparationReader.class);
                });
    }

    @Test
    void bindsRedisCircuitBreakerThresholdsFromConfiguration() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        DataRedisAutoConfiguration.class, IssuanceGateRedisAutoConfiguration.class))
                .withPropertyValues(
                        "coupon.v2.redis-circuit-breaker.sliding-window-size=30",
                        "coupon.v2.redis-circuit-breaker.minimum-number-of-calls=10",
                        "coupon.v2.redis-circuit-breaker.failure-rate-threshold=40",
                        "coupon.v2.redis-circuit-breaker.wait-duration-in-open-state=7s")
                .run(context -> {
                    CircuitBreaker breaker = context.getBean(CircuitBreaker.class);
                    assertThat(breaker.getName()).isEqualTo("redisCB");
                    assertThat(breaker.getCircuitBreakerConfig().getSlidingWindowSize()).isEqualTo(30);
                    assertThat(breaker.getCircuitBreakerConfig().getMinimumNumberOfCalls()).isEqualTo(10);
                    assertThat(breaker.getCircuitBreakerConfig().getFailureRateThreshold()).isEqualTo(40f);
                    assertThat(breaker.getCircuitBreakerConfig().getWaitIntervalFunctionInOpenState()
                            .apply(1)).isEqualTo(7_000L);
                });
    }

    @Test
    void defaultCircuitBreakerOnlyOpensForRedisCommunicationFailures() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        DataRedisAutoConfiguration.class, IssuanceGateRedisAutoConfiguration.class))
                .run(context -> {
                    CircuitBreaker breaker = context.getBean(CircuitBreaker.class);

                    assertThat(breaker.getCircuitBreakerConfig().getSlidingWindowSize()).isEqualTo(5);
                    assertThat(breaker.getCircuitBreakerConfig().getMinimumNumberOfCalls()).isEqualTo(5);
                    assertThat(breaker.getCircuitBreakerConfig().getFailureRateThreshold()).isEqualTo(100f);
                });
    }

    @Test
    void actualDefaultCircuitBreakerStopsCallingRedisAfterFiveConsecutiveFailures() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        DataRedisAutoConfiguration.class, IssuanceGateRedisAutoConfiguration.class))
                .run(context -> {
                    IssuanceGatePort delegate = mock(IssuanceGatePort.class);
                    AtomicInteger calls = new AtomicInteger();
                    when(delegate.claim(any())).thenAnswer(invocation -> {
                        if (calls.incrementAndGet() <= 10) {
                            throw new RedisConnectionFailureException("down");
                        }
                        return ClaimResult.claimed(1L);
                    });
                    Resilience4jIssuanceGate gate = new Resilience4jIssuanceGate(
                            delegate, context.getBean(CircuitBreaker.class));
                    ClaimCommand command = new ClaimCommand(1L, 2L, 1, "key", "token");

                    for (int attempt = 0; attempt < 5; attempt++) {
                        assertThatThrownBy(() -> gate.claim(command))
                                .isInstanceOf(RedisConnectionFailureException.class);
                    }

                    assertThatThrownBy(() -> gate.claim(command))
                            .isInstanceOf(IssuanceGateCircuitOpenException.class);
                    assertThat(calls).hasValue(5);
                });
    }

    @Test
    @DisplayName("Redis 통로가 없으면 게이트도 없다 — 없는 통로를 빈으로 세우면 첫 요청에서야 드러난다")
    void backsOffWithoutRedisTemplate() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(IssuanceGatePort.class);
            assertThat(context).doesNotHaveBean(IssuanceScriptRunner.class);
            assertThat(context).doesNotHaveBean(V2AdminPreparationReader.class);
            assertThat(context).doesNotHaveBean(V2AdminStockReader.class);
        });
    }

    @Test
    @DisplayName("이미 등록된 게이트가 있으면 물러난다 — 테스트 대역이 어댑터에 밀리지 않는다")
    void userBeanWins() {
        IssuanceGatePort userGate = mock(IssuanceGatePort.class);
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        DataRedisAutoConfiguration.class, IssuanceGateRedisAutoConfiguration.class))
                .withBean(IssuanceGatePort.class, () -> userGate)
                .run(context -> assertThat(context.getBean(IssuanceGatePort.class)).isSameAs(userGate));
    }

    @Test
    @DisplayName("MeterRegistry 가 있으면 복원 카운터가 실물로 올라온다")
    void registersRestorationMetersWhenRegistryPresent() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        DataRedisAutoConfiguration.class, IssuanceGateRedisAutoConfiguration.class))
                .withBean(SimpleMeterRegistry.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(RestorationHaltStore.class);
                    assertThat(context.getBean(V2RestorationMeters.class))
                            .isInstanceOf(MicrometerV2RestorationMeters.class);
                    // 결과별 시계열이 기동 시점에 전부 서 있어야 "한 번도 안 났다" 와
                    // "계측이 안 붙었다" 가 구별된다.
                    SimpleMeterRegistry registry = context.getBean(SimpleMeterRegistry.class);
                    assertThat(registry.find(MicrometerV2RestorationMeters.NAME).counters())
                            .extracting(counter -> counter.getId().getTag("outcome"))
                            // 결과 5종 + 호출 실패 + 표식 쓰기 실패.
                            .contains(MicrometerV2RestorationMeters.FAILURE_OUTCOME,
                                    MicrometerV2RestorationMeters.HALT_WRITE_FAILURE_OUTCOME)
                            .hasSize(RestoreOutcome.values().length + 2);
                });
    }

    @Test
    void bindsRedisCircuitBreakerStateMetricsWhenMeterRegistryIsPresent() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        DataRedisAutoConfiguration.class, IssuanceGateRedisAutoConfiguration.class))
                .withBean(SimpleMeterRegistry.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(TaggedCircuitBreakerMetrics.class);
                    SimpleMeterRegistry registry = context.getBean(SimpleMeterRegistry.class);
                    assertThat(registry.getMeters())
                            .extracting(meter -> meter.getId().getName())
                            .contains("resilience4j.circuitbreaker.state");
                    assertThat(registry.find("resilience4j.circuitbreaker.state")
                            .tag("name", "redisCB").gauges()).isNotEmpty();
                });
    }

    /**
     * 레지스트리가 없는 조립에서도 <b>기동은 살아야 한다.</b> 계측이 없다고 취소·만료의 재고
     * 복원이 멎으면 그것이 더 큰 사고다.
     */
    @Test
    @DisplayName("MeterRegistry 가 없으면 계측만 꺼지고 기동은 산다")
    void fallsBackToNoOpMetersWithoutRegistry() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        DataRedisAutoConfiguration.class, IssuanceGateRedisAutoConfiguration.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(V2RestorationMeters.class))
                            .isSameAs(V2RestorationMeters.NONE);
                });
    }

    /**
     * 표식 저장소가 게이트와 <b>같은 조립</b>에 있어야 취소(api)와 만료(batch)가 같은 표식을
     * 본다. 여기서 빠지면 복원 등록이 커밋 전에 예외로 죽는다.
     */
    @Test
    @DisplayName("Redis 통로가 없으면 표식 저장소도 물러난다")
    void haltStoreStepsAsideWithoutRedis() {
        runner.run(context -> assertThat(context).doesNotHaveBean(RestorationHaltStore.class));
    }

    /** 사용자 준비 Reader가 자동설정 구현으로 덮이지 않는지 검증합니다. */
    @Test
    @DisplayName("사용자 V2 준비 Reader가 있으면 자동설정 Reader는 물러난다")
    void userPreparationReaderWins() {
        V2AdminPreparationReader userReader = mock(V2AdminPreparationReader.class);

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        DataRedisAutoConfiguration.class, IssuanceGateRedisAutoConfiguration.class))
                .withBean(V2AdminPreparationReader.class, () -> userReader)
                .run(context -> assertThat(context.getBean(V2AdminPreparationReader.class))
                        .isSameAs(userReader));
    }
}
