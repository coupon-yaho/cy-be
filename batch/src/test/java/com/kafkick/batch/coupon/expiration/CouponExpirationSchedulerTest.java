package com.kafkick.batch.coupon.expiration;

import org.junit.jupiter.api.Test;

import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class CouponExpirationSchedulerTest {

    private final ApplicationContextRunner contextRunner =
            new ApplicationContextRunner()
                    .withBean(
                            CouponExpirationRunner.class,
                            () -> mock(CouponExpirationRunner.class)
                    )
                    .withUserConfiguration(CouponExpirationScheduler.class);

    @Test
    void doesNotRegisterSchedulerWhenBatchSchedulingIsDisabled() {
        contextRunner
                .withPropertyValues("batch.scheduling.enabled=false")
                .run(context -> assertThat(context)
                        .doesNotHaveBean(CouponExpirationScheduler.class));
    }

    @Test
    void registersSchedulerWhenBatchSchedulingIsEnabled() {
        contextRunner
                .withPropertyValues("batch.scheduling.enabled=true")
                .run(context -> assertThat(context)
                        .hasSingleBean(CouponExpirationScheduler.class));
    }
}
