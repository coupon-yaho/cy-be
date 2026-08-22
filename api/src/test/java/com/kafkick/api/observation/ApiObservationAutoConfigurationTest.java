package com.kafkick.api.observation;

import com.kafkick.api.observation.issuance.IssuanceObservationService;
import com.kafkick.api.observation.resource.ResourceProvider;
import com.kafkick.core.consistency.ConsistencyCalculator;
import com.kafkick.core.consistency.ConsistencySeverityPolicy;
import com.kafkick.core.consistency.DefaultConsistencyCalculator;
import com.kafkick.core.observation.EventIdGenerator;
import com.kafkick.core.observation.EventRecorder;
import com.kafkick.core.observation.IssuanceFlowEventFactory;
import com.kafkick.core.support.TimeProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import javax.sql.DataSource;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(OutputCaptureExtension.class)
class ApiObservationAutoConfigurationTest {

    private static final TimeProvider TIME_PROVIDER = new TimeProvider(Clock.fixed(
            Instant.parse("2026-08-19T01:00:05Z"),
            ZoneOffset.UTC
    ));

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ApiObservationAutoConfiguration.class));

    @Test
    void registersDefaultBeansWhenImplementationsAreMissing() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(EventRecorder.class);
            assertThat(context).hasSingleBean(ConsistencyCalculator.class);
            assertThat(context).hasSingleBean(ConsistencySeverityPolicy.class);
            assertThat(context).hasSingleBean(EventIdGenerator.class);
            assertThat(context).hasSingleBean(IssuanceFlowEventFactory.class);
            assertThat(context).hasSingleBean(IssuanceObservationService.class);
            assertThat(context).hasSingleBean(TimeProvider.class);
            assertThat(context.getBean(EventRecorder.class)).isInstanceOf(NoOpEventRecorder.class);
            assertThat(context.getBean(ConsistencyCalculator.class))
                    .isInstanceOf(DefaultConsistencyCalculator.class);
            assertThat(context.getBean(ConsistencySeverityPolicy.class).warnThreshold()).isEqualTo(10);
            assertThat(context.getBean(ConsistencySeverityPolicy.class).criticalThreshold()).isEqualTo(100);
        });
    }

    @Test
    void createsTimeProviderFromApplicationClock() {
        Instant fixedInstant = Instant.parse("2026-08-19T02:00:00Z");
        Clock applicationClock = Clock.fixed(fixedInstant, ZoneOffset.UTC);

        contextRunner
                .withBean(Clock.class, () -> applicationClock)
                .run(context -> assertThat(context.getBean(TimeProvider.class).instant())
                        .isEqualTo(fixedInstant));
    }

    @Test
    void usesSystemUtcWhenClockAndTimeProviderAreMissing(CapturedOutput output) {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(TimeProvider.class);
            assertThat(output).contains("Clock 빈이 없어 시스템 UTC 시계를 사용합니다.");
        });
    }

    @Test
    void backsOffWhenTimeProviderExists() {
        contextRunner
                .withBean(TimeProvider.class, () -> TIME_PROVIDER)
                .run(context -> assertThat(context.getBean(TimeProvider.class))
                        .isSameAs(TIME_PROVIDER));
    }

    @Test
    // 빈 이름이 mainDataSource 인 것은 우연이 아니다. 자동설정이 @Qualifier("mainDataSource") 로
    // 운영 풀을 지목하므로, 이름을 바꾸면 주입 대상이 사라져 이 테스트가 먼저 깨진다.
    void resourceProviderRequiresBothDataSourceAndMeterRegistry() {
        contextRunner.run(context -> assertThat(context).doesNotHaveBean(ResourceProvider.class));

        contextRunner
                .withBean("mainDataSource", DataSource.class, ApiObservationAutoConfigurationTest::dataSource)
                .run(context -> assertThat(context).doesNotHaveBean(ResourceProvider.class));

        contextRunner
                .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
                .run(context -> assertThat(context).doesNotHaveBean(ResourceProvider.class));

        contextRunner
                .withBean("mainDataSource", DataSource.class, ApiObservationAutoConfigurationTest::dataSource)
                .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
                .run(context -> assertThat(context).hasSingleBean(ResourceProvider.class));
    }

    /**
     * 조건과 주입이 어긋나면 컨텍스트가 통째로 죽는다. 조건을 타입으로 두었을 때 실제로 그랬다 —
     * Boot 기본 {@code dataSource} 빈이 타입 조건을 통과시키고 이름 주입만 실패했다.
     *
     * <p>여기서 고정하는 것은 "빈이 없다" 가 아니라 "<b>기동은 살아 있다</b>" 쪽이다.
     */
    @Test
    void resourceProviderIsSkippedInsteadOfFailingWhenTheMainPoolIsAbsent() {
        contextRunner
                .withBean("dataSource", DataSource.class, ApiObservationAutoConfigurationTest::dataSource)
                .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
                .run(context -> assertThat(context)
                        .hasNotFailed()
                        .doesNotHaveBean(ResourceProvider.class));
    }

    /** 나머지 빈과 같은 오버라이드 규약을 따른다 — 사용자가 등록하면 자동설정이 물러난다. */
    @Test
    void userDefinedResourceProviderWins() {
        ResourceProvider stub = new ResourceProvider(
                dataSource(), new SimpleMeterRegistry(), TIME_PROVIDER);

        contextRunner
                .withBean("mainDataSource", DataSource.class, ApiObservationAutoConfigurationTest::dataSource)
                .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
                .withBean("myResourceProvider", ResourceProvider.class, () -> stub)
                .run(context -> assertThat(context)
                        .hasSingleBean(ResourceProvider.class)
                        .getBean(ResourceProvider.class).isSameAs(stub));
    }

    @Test
    void bindsConsistencySeverityThresholdsFromConfiguration() {
        contextRunner
                .withPropertyValues(
                        "observation.consistency.severity.warn-threshold=20",
                        "observation.consistency.severity.critical-threshold=200"
                )
                .run(context -> {
                    ConsistencySeverityPolicy policy = context.getBean(ConsistencySeverityPolicy.class);

                    assertThat(policy.warnThreshold()).isEqualTo(20);
                    assertThat(policy.criticalThreshold()).isEqualTo(200);
                    assertThat(context.getBean(ConsistencyCalculator.class))
                            .isInstanceOf(DefaultConsistencyCalculator.class);
                });
    }

    @Test
    void failsStartupWhenConsistencySeverityThresholdsAreInvalid() {
        contextRunner
                .withPropertyValues(
                        "observation.consistency.severity.warn-threshold=100",
                        "observation.consistency.severity.critical-threshold=10"
                )
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasRootCauseInstanceOf(IllegalArgumentException.class);
                });
    }

    @Test
    void backsOffWhenImplementationsExist() {
        EventRecorder eventRecorder = event -> { };
        IssuanceFlowEventFactory eventFactory = new IssuanceFlowEventFactory(
                () -> java.util.UUID.randomUUID()
        );
        IssuanceObservationService observationService =
                new IssuanceObservationService(eventFactory, eventRecorder, TIME_PROVIDER);
        ConsistencyCalculator calculator = (snapshot, phase, engineVersion) -> null;
        ConsistencySeverityPolicy policy = new ConsistencySeverityPolicy(30, 300);

        contextRunner
                .withBean(TimeProvider.class, () -> TIME_PROVIDER)
                .withBean(EventRecorder.class, () -> eventRecorder)
                .withBean(IssuanceObservationService.class, () -> observationService)
                .withBean(ConsistencyCalculator.class, () -> calculator)
                .withBean(ConsistencySeverityPolicy.class, () -> policy)
                .run(context -> {
                    assertThat(context).hasSingleBean(EventRecorder.class);
                    assertThat(context).hasSingleBean(ConsistencyCalculator.class);
                    assertThat(context.getBean(EventRecorder.class)).isSameAs(eventRecorder);
                    assertThat(context.getBean(IssuanceObservationService.class))
                            .isSameAs(observationService);
                    assertThat(context.getBean(ConsistencyCalculator.class)).isSameAs(calculator);
                    assertThat(context.getBean(ConsistencySeverityPolicy.class)).isSameAs(policy);
                });
    }

    private static DataSource dataSource() {
        return org.mockito.Mockito.mock(DataSource.class);
    }
}
