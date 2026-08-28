package com.kafkick.api.observation;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import com.kafkick.core.coupon.port.IdempotencyRepository;
import com.kafkick.core.coupon.port.IdempotencyResultCodec;
import com.kafkick.core.coupon.port.IssuanceHistoryRepository;
import com.kafkick.core.coupon.port.IssuanceRepository;
import com.kafkick.core.coupon.service.code.CouponCodeGenerator;
import com.kafkick.core.coupon.service.result.CouponIssueResult;
import com.kafkick.core.coupon.v2.V2CouponIssueService;
import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.infra.redis.coupon.v2.IssuanceGateRedisAutoConfiguration;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * v2 발급이 실제로 조립되는지 본다.
 *
 * <p><b>왜 이 테스트가 있어야 하는가</b> — {@code V2CouponIssueService} 는
 * {@code @ConditionalOnBean(IssuanceGatePort.class)} 로 만들어진다. 그 조건은 <b>평가 시점에
 * 이미 등록된 빈</b>만 보므로, 게이트를 만드는 자동설정이 나중에 돌면 조건이 조용히 거짓이 되어
 * 서비스가 아예 안 생긴다.
 *
 * <p>그 상태는 <b>기동 때 아무 소리도 내지 않는다.</b> {@code ObjectProvider.getIfAvailable()}
 * 이 {@code null} 을 돌려주고 첫 발급 요청에서야 500 이 난다(실측 — V2 회차 발급 3건이 전부
 * {@code IllegalStateException: V2 발급 게이트가 활성화되지 않았습니다}).
 * 기동 테스트도 계약 테스트도 그것을 못 잡았다.
 *
 * <p>그래서 여기서 보는 것은 빈 하나의 존재다. 순서를 되돌리면 이 테스트가 빨강이 된다.
 */
class V2IssuanceGateWiringTest {

    @Test
    @DisplayName("StringRedisTemplate 이 있으면 v2 발급 서비스가 조립된다")
    void assemblesV2IssueServiceWhenRedisTemplateExists() {
        runner().run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(IssuanceGatePort.class);
            assertThat(context)
                    .as("게이트가 있는데 발급 서비스가 없으면 첫 요청에서 500 이 난다")
                    .hasSingleBean(V2CouponIssueService.class);
        });
    }

    @Test
    @DisplayName("StringRedisTemplate 이 없으면 게이트도 발급 서비스도 없다")
    void assemblesNeitherWithoutRedisTemplate() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        ApiObservationAutoConfiguration.class,
                        IssuanceGateRedisAutoConfiguration.class))
                .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(IssuanceGatePort.class);
                    assertThat(context).doesNotHaveBean(V2CouponIssueService.class);
                });
    }

    /** 게이트 자동설정을 <b>뒤에</b> 놓는다 — 순서 선언이 없으면 여기서 드러난다. */
    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        ApiObservationAutoConfiguration.class,
                        IssuanceGateRedisAutoConfiguration.class))
                .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
                .withBean(StringRedisTemplate.class,
                        () -> Mockito.mock(StringRedisTemplate.class))
                .withBean(IssuanceRepository.class,
                        () -> Mockito.mock(IssuanceRepository.class))
                .withBean(IssuanceHistoryRepository.class,
                        () -> Mockito.mock(IssuanceHistoryRepository.class))
                .withBean(IdempotencyRepository.class,
                        () -> Mockito.mock(IdempotencyRepository.class))
                .withBean(CouponCodeGenerator.class,
                        () -> Mockito.mock(CouponCodeGenerator.class))
                .withBean("issueCodec", IdempotencyResultCodec.class,
                        () -> Mockito.mock(IdempotencyResultCodec.class))
                .withBean(PlatformTransactionManager.class,
                        () -> Mockito.mock(PlatformTransactionManager.class));
    }
}
