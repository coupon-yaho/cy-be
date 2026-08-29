package com.kafkick.api.observation;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.util.List;
import java.util.stream.Stream;

import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import com.kafkick.core.coupon.port.CouponStockRepository;
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

    /**
     * 조건에 적힌 <b>컴포넌트 스캔 여섯</b>을 하나씩 빼며 전부 확인한다.
     *
     * <p>두어 개만 골라 보면 나머지가 조건에서 빠지는 회귀를 못 잡는다 — 목록과 테스트가
     * 같은 자리에서 갈리기 때문이다. 여기서는 목록 자체를 순회한다.
     *
     * <p>{@link IssuanceGatePort} 는 이 목록에 없다. 그것만 자동설정 빈이라 뺄 방법이
     * {@code StringRedisTemplate} 을 없애는 것뿐이고, 그 경우는 아래 별도 테스트가 본다.
     */
    @ParameterizedTest(name = "{0} 이 없으면 조립하지 않는다")
    @MethodSource("scannedConditionTypes")
    void skipsWhenAConditionBeanIsMissing(Class<?> missing) {
        runnerWithout(missing).run(context -> {
            assertThat(context)
                    .as("조건이 그 타입을 검사하면 컨텍스트를 떨어뜨리지 않고 빠져야 한다")
                    .hasNotFailed();
            assertThat(context).hasSingleBean(IssuanceGatePort.class);
            assertThat(context).doesNotHaveBean(V2CouponIssueService.class);
        });
    }

    static List<Named<Class<?>>> scannedConditionTypes() {
        return Stream.<Class<?>>of(
                        IssuanceRepository.class,
                        IssuanceHistoryRepository.class,
                        IdempotencyRepository.class,
                        CouponStockRepository.class,
                        CouponCodeGenerator.class,
                        PlatformTransactionManager.class)
                .map(type -> Named.<Class<?>>of(type.getSimpleName(), type))
                .toList();
    }

    /**
     * codec 은 조건에 없다 — 그래서 <b>조용히 빠지지 않고 기동이 실패한다.</b>
     *
     * <p>제네릭을 어노테이션에 못 써서 생긴 의도적 공백이고, 그 대가가 무엇인지를 여기에
     * 고정한다. 조건에 raw 타입을 넣는 "고침" 이 들어오면 이 테스트가 빨강이 되어
     * 오탐(네 codec 중 아무거나로 참이 되는 것)을 막는다.
     */
    @Test
    @DisplayName("codec 이 없으면 조용히 빠지지 않고 기동이 실패한다 — 의도된 공백이다")
    void failsFastWithoutResultCodec() {
        runnerWithout(IdempotencyResultCodec.class).run(context ->
                assertThat(context).hasFailed());
    }

    /** 게이트 자동설정을 <b>뒤에</b> 놓는다 — 순서 선언이 없으면 여기서 드러난다. */
    private ApplicationContextRunner runner() {
        return runnerWithout(null);
    }

    /**
     * {@code omitted} 타입 하나만 빼고 조립한다.
     *
     * <p>전부 등록해 두고 성공만 보면 <b>의존성이 빠졌을 때의 경로를 아무도 안 본다</b> —
     * 조건이 그 타입을 검사하지 않으면 조건은 통과하고 빈 생성에서 컨텍스트가 죽는데,
     * 그 사실이 이 테스트에 드러나지 않는다.
     */
    private ApplicationContextRunner runnerWithout(Class<?> omitted) {
        ApplicationContextRunner runner = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        ApiObservationAutoConfiguration.class,
                        IssuanceGateRedisAutoConfiguration.class))
                .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
                .withBean(StringRedisTemplate.class,
                        () -> Mockito.mock(StringRedisTemplate.class));

        for (Class<?> required : List.of(
                IssuanceRepository.class,
                IssuanceHistoryRepository.class,
                IdempotencyRepository.class,
                CouponStockRepository.class,
                CouponCodeGenerator.class,
                PlatformTransactionManager.class,
                IdempotencyResultCodec.class)) {
            if (required.equals(omitted)) {
                continue;
            }
            runner = register(runner, required);
        }
        return runner;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static ApplicationContextRunner register(ApplicationContextRunner runner, Class<?> type) {
        return runner.withBean((Class) type, () -> Mockito.mock(type));
    }
}
