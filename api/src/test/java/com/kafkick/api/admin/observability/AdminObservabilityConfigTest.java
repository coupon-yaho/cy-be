package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.test.util.ReflectionTestUtils;

import com.kafkick.core.admin.overview.AdminOverviewService;
import com.kafkick.core.admin.overview.calculator.CampaignOverviewCalculator;
import com.kafkick.core.admin.overview.calculator.CampaignQueueCalculator;
import com.kafkick.core.admin.overview.calculator.ConsistencyActionCalculator;
import com.kafkick.core.admin.overview.calculator.CustomerOutcomeCalculator;
import com.kafkick.core.admin.overview.calculator.IssuanceActionCalculator;
import com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator;
import com.kafkick.core.admin.overview.calculator.OperationActionCalculator;
import com.kafkick.core.admin.overview.calculator.OverviewStatusCalculator;
import com.kafkick.core.admin.overview.calculator.StockRiskCalculator;
import com.kafkick.core.admin.overview.mock.AdminOverviewMockDataFactory;
import com.kafkick.core.admin.overview.observation.OverviewObservationSource;
import com.kafkick.core.support.TimeProvider;

/** Admin Prometheus instant/range/Overview adapter 배선을 검증합니다. */
class AdminObservabilityConfigTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(
                    AdminObservabilityConfig.class,
                    AdminOverviewMockDataFactory.class,
                    IssuanceFlowCalculator.class,
                    IssuanceActionCalculator.class,
                    CampaignQueueCalculator.class,
                    CustomerOutcomeCalculator.class,
                    StockRiskCalculator.class,
                    CampaignOverviewCalculator.class,
                    ConsistencyActionCalculator.class,
                    OperationActionCalculator.class,
                    OverviewStatusCalculator.class)
            .withPropertyValues(
                    "observation.prometheus.base-url=http://prometheus:9090",
                    "observation.prometheus.connect-timeout=100ms",
                    "observation.prometheus.read-timeout=300ms",
                    "observation.prometheus.stale-after=2m",
                    "observation.prometheus.total-budget=500ms",
                    "observation.prometheus.overview.current-window=2m",
                    "observation.prometheus.overview.comparison-offset=4m",
                    "observation.prometheus.overview.trend-window=20m",
                    "observation.prometheus.overview.trend-step=2m",
                    "observation.prometheus.overview.outcome-window=15m",
                    "observation.prometheus.overview.latency-window=30s",
                    "observation.prometheus.overview.max-range=2h",
                    "observation.prometheus.overview.max-points=2000")
            .withBean(TimeProvider.class, () -> new TimeProvider(Clock.systemUTC()));

    /** 실제 Spring context가 기존 instant client와 range client 및 기술 중립 원천을 함께 배선합니다. */
    @Test
    @DisplayName("ApplicationContext가 Prom 원천을 AdminOverviewService까지 단일 경로로 배선한다")
    void wiresPromObservationSourceIntoOverviewServiceInRealSpringContext() {
        contextRunner.run(context -> {
            assertThat(context).hasBean("promQueryClient");
            assertThat(context).hasBean("archivePromQueryClient");
            assertThat(context).hasSingleBean(PromRangeQueryClient.class);
            assertThat(context).hasSingleBean(OverviewObservationSource.class);
            assertThat(context.getBean("promQueryClient", PromTimeQuery.class))
                    .isSameAs(context.getBean("promQueryClient", PromQueryClient.class));
            assertThat(context.getBean(PromRangeQuery.class)).isSameAs(context.getBean(PromRangeQueryClient.class));
            assertThat(context.getBean(OverviewObservationSource.class))
                    .isInstanceOf(PromOverviewObservationSource.class);
            OverviewPrometheusProperties overviewProperties =
                    context.getBean(OverviewPrometheusProperties.class);
            assertThat(overviewProperties.currentWindow()).isEqualTo(java.time.Duration.ofMinutes(2));
            assertThat(overviewProperties.expectedTrendBuckets()).isEqualTo(10);
            assertThat(ReflectionTestUtils.getField(
                    context.getBean(OverviewObservationSource.class), "overviewProperties"))
                    .isSameAs(overviewProperties);
            assertThat(ReflectionTestUtils.getField(
                    context.getBean(PromRangeQueryClient.class), "maxRange"))
                    .isEqualTo(java.time.Duration.ofHours(2));
            assertThat(ReflectionTestUtils.getField(
                    context.getBean(PromRangeQueryClient.class), "maxPoints"))
                    .isEqualTo(2_000);
            assertThat(context).hasSingleBean(AdminOverviewService.class);
            assertThat(ReflectionTestUtils.getField(
                    context.getBean(AdminOverviewService.class), "observationSource"))
                    .isSameAs(context.getBean(OverviewObservationSource.class));
        });
    }
}
