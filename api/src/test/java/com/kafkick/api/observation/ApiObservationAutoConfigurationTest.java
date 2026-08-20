package com.kafkick.api.observation;

import com.kafkick.api.observation.issuance.IssuanceObservationService;
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
}
