package com.kafkick.infra.redis.coupon.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.context.annotation.ImportCandidates;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.core.admin.stock.V2AdminStockReader;

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
                    assertThat(context).hasSingleBean(V2AdminStockReader.class);
                    assertThat(context.getBean(IssuanceGatePort.class))
                            .isInstanceOf(RedisIssuanceGate.class);
                    assertThat(context.getBean(V2AdminStockReader.class))
                            .isInstanceOf(RedisV2AdminStockReader.class);
                });
    }

    @Test
    @DisplayName("Redis 통로가 없으면 게이트도 없다 — 없는 통로를 빈으로 세우면 첫 요청에서야 드러난다")
    void backsOffWithoutRedisTemplate() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(IssuanceGatePort.class);
            assertThat(context).doesNotHaveBean(IssuanceScriptRunner.class);
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
}
