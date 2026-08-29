package com.kafkick.batch.coupon.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.time.Clock;

import java.util.List;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import com.kafkick.batch.coupon.v2.CouponRoundRebuildRunner;
import com.kafkick.batch.coupon.v2.CouponRoundWarmupRunner;
import com.kafkick.batch.coupon.v2.RoundGateWriteGuard;
import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.core.coupon.v2.port.IssuanceWarmupPort;
import com.kafkick.core.support.TimeProvider;

class CouponRoundWarmupConfigurationTest {

    private final ApplicationContextRunner base = new ApplicationContextRunner()
            .withBean(JdbcTemplate.class, () -> mock(JdbcTemplate.class))
            .withBean(PlatformTransactionManager.class, () -> mock(PlatformTransactionManager.class))
            .withBean(TimeProvider.class, () -> new TimeProvider(Clock.systemUTC()))
            .withPropertyValues("coupon.rebuild.drain=2s")
            .withUserConfiguration(CouponRoundWarmupConfiguration.class);

    /**
     * 설정이 요구하는 <b>두 포트를 하나씩</b> 빼며 전부 확인한다. 하나만 골라 보면 나머지가
     * 조건에서 빠지는 회귀를 못 잡는다 — 목록과 테스트가 같은 자리에서 갈리기 때문이다.
     */
    @ParameterizedTest(name = "{0} 이 없으면 기동이 실패한다")
    @MethodSource("requiredPortTypes")
    void failsStartupWhenARequiredPortIsMissing(Class<?> missing) {
        ApplicationContextRunner runner = base;
        for (Class<?> required : requiredPorts()) {
            if (!required.equals(missing)) {
                runner = register(runner, required);
            }
        }
        runner.run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure())
                    .hasMessageContaining(missing.getSimpleName());
        });
    }

    static List<Class<?>> requiredPorts() {
        return List.of(IssuanceGatePort.class, IssuanceWarmupPort.class);
    }

    static List<Named<Class<?>>> requiredPortTypes() {
        return requiredPorts().stream()
                .map(type -> Named.<Class<?>>of(type.getSimpleName(), type))
                .toList();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static ApplicationContextRunner register(
            ApplicationContextRunner runner, Class<?> type) {
        return runner.withBean((Class) type, () -> mock(type));
    }

    /**
     * drain 대기는 <b>기본값을 코드에 두지 않는다.</b> 설정과 코드 두 곳에 값이 있으면 한쪽만
     * 바뀐 상태가 조용히 남고, 그 사실은 재구성 창이 다시 열릴 때에야 드러난다.
     */
    @Test
    void failsStartupWhenTheRebuildDrainIsMissing() {
        new ApplicationContextRunner()
                .withBean(JdbcTemplate.class, () -> mock(JdbcTemplate.class))
                .withBean(PlatformTransactionManager.class,
                        () -> mock(PlatformTransactionManager.class))
                .withBean(TimeProvider.class, () -> new TimeProvider(Clock.systemUTC()))
                .withBean(IssuanceGatePort.class, () -> mock(IssuanceGatePort.class))
                .withBean(IssuanceWarmupPort.class, () -> mock(IssuanceWarmupPort.class))
                .withUserConfiguration(CouponRoundWarmupConfiguration.class)
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void createsTheWarmupRunnerWhenBothRedisPortsExist() {
        base.withBean(IssuanceGatePort.class, () -> mock(IssuanceGatePort.class))
                .withBean(IssuanceWarmupPort.class, () -> mock(IssuanceWarmupPort.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(CouponRoundWarmupRunner.class);
                    // 두 러너가 **같은** 가드를 받아야 서로의 겹침까지 막는다.
                    assertThat(context).hasSingleBean(CouponRoundRebuildRunner.class);
                    assertThat(context).hasSingleBean(RoundGateWriteGuard.class);
                });
    }
}
