package com.kafkick.core.observation.autoconfigure;

import com.kafkick.core.consistency.ConsistencyCalculator;
import com.kafkick.core.consistency.NoOpConsistencyCalculator;
import com.kafkick.core.observation.EventRecorder;
import com.kafkick.core.observation.EventIdGenerator;
import com.kafkick.core.observation.IssuanceFlowEventFactory;
import com.kafkick.core.observation.NoOpEventRecorder;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class CoreObservationAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(CoreObservationAutoConfiguration.class));

    @Test
    void registersNoOpBeansWhenImplementationsAreMissing() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(EventRecorder.class);
            assertThat(context).hasSingleBean(ConsistencyCalculator.class);
            assertThat(context).hasSingleBean(EventIdGenerator.class);
            assertThat(context).hasSingleBean(IssuanceFlowEventFactory.class);
            assertThat(context.getBean(EventRecorder.class)).isInstanceOf(NoOpEventRecorder.class);
            assertThat(context.getBean(ConsistencyCalculator.class))
                    .isInstanceOf(NoOpConsistencyCalculator.class);
        });
    }

    @Test
    void backsOffWhenImplementationsExist() {
        EventRecorder eventRecorder = event -> { };
        ConsistencyCalculator calculator = (snapshot, phase, engineVersion) -> null;

        contextRunner
                .withBean(EventRecorder.class, () -> eventRecorder)
                .withBean(ConsistencyCalculator.class, () -> calculator)
                .run(context -> {
                    assertThat(context).hasSingleBean(EventRecorder.class);
                    assertThat(context).hasSingleBean(ConsistencyCalculator.class);
                    assertThat(context.getBean(EventRecorder.class)).isSameAs(eventRecorder);
                    assertThat(context.getBean(ConsistencyCalculator.class)).isSameAs(calculator);
                });
    }
}
