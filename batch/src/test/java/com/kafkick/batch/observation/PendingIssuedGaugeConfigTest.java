package com.kafkick.batch.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.task.TaskSchedulingAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import com.kafkick.core.support.TimeProvider;

class PendingIssuedGaugeConfigTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(TaskSchedulingAutoConfiguration.class))
        .withBean("obs", JdbcTemplate.class, () -> org.mockito.Mockito.mock(JdbcTemplate.class))
        .withBean(StringRedisTemplate.class, () -> org.mockito.Mockito.mock(StringRedisTemplate.class))
        .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
        .withBean(TimeProvider.class, () -> new TimeProvider(Clock.systemUTC()))
        .withUserConfiguration(PendingIssuedGaugeConfig.class);

    @Test
    void enabledBindsEverySchedulingPropertyAndCreatesCollector() {
        runner.withPropertyValues(
            "observation.datasource.enabled=true",
            "observation.pending-issued-gauge.enabled=true",
            "observation.pending-issued-gauge.interval=45s",
            "observation.pending-issued-gauge.stale-after=9m",
            "observation.pending-issued-gauge.scan-count=123")
            .run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(context).hasSingleBean(PendingIssuedGaugeCollector.class);
                PendingIssuedGaugeProperties properties = context.getBean(PendingIssuedGaugeProperties.class);
                assertThat(properties.enabled()).isTrue();
                assertThat(properties.interval()).isEqualTo(java.time.Duration.ofSeconds(45));
                assertThat(properties.scanCount()).isEqualTo(123);
            });
    }

    @Test
    void disabledStartsWithoutCollectorOrRedisBean() {
        new ApplicationContextRunner()
            .withBean("obs", JdbcTemplate.class, () -> org.mockito.Mockito.mock(JdbcTemplate.class))
            .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
            .withBean(TimeProvider.class, () -> new TimeProvider(Clock.systemUTC()))
            .withUserConfiguration(PendingIssuedGaugeConfig.class)
            .withPropertyValues(
                "observation.datasource.enabled=true",
                "observation.pending-issued-gauge.enabled=false")
            .run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(context).doesNotHaveBean(PendingIssuedGaugeCollector.class);
            });
    }

    @Test
    void enabledStartsWithoutRedisBeanSoV1DeploymentDoesNotRequireTheChannel() {
        new ApplicationContextRunner()
            .withBean("obs", JdbcTemplate.class, () -> org.mockito.Mockito.mock(JdbcTemplate.class))
            .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
            .withBean(TimeProvider.class, () -> new TimeProvider(Clock.systemUTC()))
            .withUserConfiguration(PendingIssuedGaugeConfig.class)
            .withPropertyValues(
                "observation.datasource.enabled=true",
                "observation.pending-issued-gauge.enabled=true")
            .run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(context).hasSingleBean(PendingIssuedGaugeCollector.class);
            });
    }

    @Test
    void invalidBoundIntervalFailsAtStartup() {
        runner.withPropertyValues(
            "observation.datasource.enabled=true",
            "observation.pending-issued-gauge.enabled=true",
            "observation.pending-issued-gauge.interval=0s")
            .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void schedulerUsesTheValidatedBoundInterval() {
        PendingIssuedGaugeCollector collector = org.mockito.Mockito.mock(PendingIssuedGaugeCollector.class);
        PendingIssuedGaugeProperties properties = new PendingIssuedGaugeProperties(
            true, java.time.Duration.ofSeconds(45), java.time.Duration.ofMinutes(5), 200, null);
        ScheduledTaskRegistrar registrar = org.mockito.Mockito.mock(ScheduledTaskRegistrar.class);

        new PendingIssuedGaugeScheduler(collector, properties).configureTasks(registrar);

        org.mockito.Mockito.verify(registrar)
            .addFixedDelayTask(org.mockito.ArgumentMatchers.any(Runnable.class),
                org.mockito.ArgumentMatchers.eq(java.time.Duration.ofSeconds(45)));
    }
}
