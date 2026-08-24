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
                    "observation.prometheus.overview.max-points=2000",
                    "observation.prometheus.series.connect-timeout=400ms",
                    "observation.prometheus.series.read-timeout=1200ms",
                    "observation.prometheus.series.total-budget=2s",
                    "observation.prometheus.series.step=10s",
                    "observation.prometheus.series.max-points=200")
            .withBean(TimeProvider.class, () -> new TimeProvider(Clock.systemUTC()));

    /**
     * range client 에 실제로 물린 read 타임아웃(ms)을 읽습니다.
     *
     * <p>{@code /metrics/series} 가 1초 폴링 타임아웃을 물었는지는 이 값으로만 드러납니다.
     * 인스턴스 비교로는 못 봅니다 — 두 빈이 각자 {@code RestClient} 를 새로 만들므로, 같은
     * 타임아웃을 물려도 서로 다른 인스턴스입니다. 확인해야 하는 것은 동일성이 아니라 값입니다.</p>
     */
    private static int readTimeoutMillisOf(PromRangeQueryClient client) {
        Object restClient = ReflectionTestUtils.getField(client, "restClient");
        Object factory = ReflectionTestUtils.getField(restClient, "clientRequestFactory");
        return (int) ReflectionTestUtils.getField(factory, "readTimeout");
    }

    /** 실제 Spring context가 기존 instant client와 range client 및 기술 중립 원천을 함께 배선합니다. */
    @Test
    @DisplayName("ApplicationContext가 Prom 원천을 AdminOverviewService까지 단일 경로로 배선한다")
    void wiresPromObservationSourceIntoOverviewServiceInRealSpringContext() {
        contextRunner.run(context -> {
            assertThat(context).hasBean("promQueryClient");
            assertThat(context).hasBean("archivePromQueryClient");
            assertThat(context).hasBean(AdminObservabilityConfig.OVERVIEW_RANGE_CLIENT);
            assertThat(context).hasBean(AdminObservabilityConfig.SERIES_RANGE_CLIENT);
            assertThat(context).hasSingleBean(OverviewObservationSource.class);
            assertThat(context.getBean("promQueryClient", PromTimeQuery.class))
                    .isSameAs(context.getBean("promQueryClient", PromQueryClient.class));
            // range client 이 둘이 되었으므로 타입만으로는 못 고른다. Overview 는 자기 것을 잡아야 한다.
            assertThat(ReflectionTestUtils.getField(
                    context.getBean(OverviewObservationSource.class), "rangeQuery"))
                    .isSameAs(context.getBean(AdminObservabilityConfig.OVERVIEW_RANGE_CLIENT, PromRangeQueryClient.class));
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
                    context.getBean(AdminObservabilityConfig.OVERVIEW_RANGE_CLIENT, PromRangeQueryClient.class), "maxRange"))
                    .isEqualTo(java.time.Duration.ofHours(2));
            assertThat(ReflectionTestUtils.getField(
                    context.getBean(AdminObservabilityConfig.OVERVIEW_RANGE_CLIENT, PromRangeQueryClient.class), "maxPoints"))
                    .isEqualTo(2_000);
            // series 경로는 자기 상한과 자기 RestClient 를 쓴다. 하나라도 overview 것을 잡으면
            // 이 경로가 1초 폴링 타임아웃을 물게 된다.
            assertThat(ReflectionTestUtils.getField(
                    context.getBean(AdminObservabilityConfig.SERIES_RANGE_CLIENT, PromRangeQueryClient.class), "maxPoints"))
                    .isEqualTo(200);
            // 인스턴스 동일성으로는 못 본다 — 배선이 호출마다 새 RestClient 를 만들기 때문에
            // 공유하지 않아도 서로 다른 인스턴스다. 실제로 물린 타임아웃을 봐야 한다.
            assertThat(readTimeoutMillisOf(context.getBean(
                    "seriesPromRangeQueryClient", PromRangeQueryClient.class)))
                    .isEqualTo(1_200);
            assertThat(readTimeoutMillisOf(context.getBean(
                    "promRangeQueryClient", PromRangeQueryClient.class)))
                    .isEqualTo(300);
            assertThat(ReflectionTestUtils.getField(
                    context.getBean(PromSeriesAssembler.class), "rangeQuery"))
                    .isSameAs(context.getBean(AdminObservabilityConfig.SERIES_RANGE_CLIENT, PromRangeQueryClient.class));

            assertThat(context).hasSingleBean(AdminOverviewService.class);
            assertThat(ReflectionTestUtils.getField(
                    context.getBean(AdminOverviewService.class), "observationSource"))
                    .isSameAs(context.getBean(OverviewObservationSource.class));
        });
    }
}
