package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.convert.ApplicationConversionService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.PlatformTransactionManager;

import com.kafkick.api.admin.support.AdminApiErrorCode;
import com.kafkick.core.admin.campaignsource.AdminCampaignCatalog;
import com.kafkick.core.admin.campaignsource.AdminCampaignDataReader;
import com.kafkick.core.admin.campaignsource.AdminCampaignDetailData;
import com.kafkick.core.admin.couponmetrics.AdminCouponMetricsService;
import com.kafkick.core.admin.couponmetrics.CouponIssuanceRateReader;
import com.kafkick.core.admin.couponmetrics.CouponMetricsCalculator;
import com.kafkick.core.benchmark.BenchmarkRunRepository;
import com.kafkick.core.benchmark.RunTimeseriesArchiver.ArchiveStore;
import com.kafkick.core.admin.overview.AdminOverviewService;
import com.kafkick.core.admin.overview.calculator.CampaignOverviewCalculator;
import com.kafkick.core.admin.overview.calculator.CampaignQueueCalculator;
import com.kafkick.core.admin.overview.calculator.CustomerOutcomeCalculator;
import com.kafkick.core.admin.overview.calculator.IssuanceActionCalculator;
import com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator;
import com.kafkick.core.admin.overview.calculator.OperationActionCalculator;
import com.kafkick.core.admin.overview.calculator.OverviewStatusCalculator;
import com.kafkick.core.admin.overview.calculator.StockRiskCalculator;
import com.kafkick.core.admin.overview.observation.OverviewObservationSource;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.runtimeconfig.ReadOnlyRuntimeConfigStore;
import com.kafkick.core.runtimeconfig.RuntimeConfigSnapshot;
import com.kafkick.core.runtimeconfig.RuntimeConfigStore;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.storage.db.admin.JdbcAdminCampaignDataReader;

class AdminObservabilityConfigTest {

    private static final Instant NOW = Instant.parse("2026-08-24T00:00:00Z");

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withInitializer(context -> context.getBeanFactory()
                    .setConversionService(ApplicationConversionService.getSharedInstance()))
            .withUserConfiguration(
                    AdminObservabilityConfig.class,
                    StorageReaderScanConfiguration.class,
                    CouponMetricsCalculator.class,
                    IssuanceFlowCalculator.class,
                    IssuanceActionCalculator.class,
                    CampaignQueueCalculator.class,
                    CustomerOutcomeCalculator.class,
                    StockRiskCalculator.class,
                    CampaignOverviewCalculator.class,
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
                    "observation.prometheus.series.max-points=200",
                    "observation.datasource.enabled=false")
            .withBean(TimeProvider.class, () -> new TimeProvider(Clock.systemUTC()))
            .withBean(RuntimeConfigStore.class, AdminObservabilityConfigTest::runtimeConfigStore)
            .withBean(BenchmarkRunRepository.class,
                    () -> org.mockito.Mockito.mock(BenchmarkRunRepository.class))
            .withBean(ArchiveStore.class, () -> org.mockito.Mockito.mock(ArchiveStore.class));

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

    @Test
    void usesTheAvailableCoreReaderWithoutCreatingThePendingFallback() {
        contextRunner.withPropertyValues("observation.datasource.enabled=true")
                .run(context -> {
                    assertThat(context).hasSingleBean(AdminCampaignDataReader.class);
                    assertThat(context.getBean(AdminCampaignDataReader.class))
                            .isInstanceOf(JdbcAdminCampaignDataReader.class);
                    assertThat(context).doesNotHaveBean(PendingAdminCampaignDataReader.class);
                    assertThat(context).hasSingleBean(AdminOverviewService.class);
                    assertThat(ReflectionTestUtils.getField(
                            context.getBean(AdminOverviewService.class), "campaignDataReader"))
                            .isSameAs(context.getBean(AdminCampaignDataReader.class));
                    assertThat(context).hasBean(AdminObservabilityConfig.OVERVIEW_RANGE_CLIENT);
                    assertThat(context).hasBean(AdminObservabilityConfig.SERIES_RANGE_CLIENT);
                    assertThat(context).hasSingleBean(OverviewObservationSource.class);
                    assertThat(context.getBean(OverviewObservationSource.class))
                            .isInstanceOf(PromOverviewObservationSource.class);
                    AdminOverviewPolicyProperties policy =
                            context.getBean(AdminOverviewPolicyProperties.class);
                    assertThat(policy.toCorePolicy().issuanceDecreaseRatio()).isEqualTo(0.50);
                    assertThat(policy.toCorePolicy().issuanceStoppedAfter())
                            .isEqualTo(java.time.Duration.ofMinutes(2));
                    assertThat(policy.toCorePolicy().queueGuidanceThreshold())
                            .isEqualTo(java.time.Duration.ofMinutes(10));
                    assertThat(policy.toCorePolicy().queueAdmissionStoppedAfter())
                            .isEqualTo(java.time.Duration.ofMinutes(2));
                    assertThat(policy.toCorePolicy().stockDepletionThreshold())
                            .isEqualTo(java.time.Duration.ofMinutes(10));
                });
    }

    @Test
    void bindsAllPolicyOverridesIntoTheOverviewService() {
        contextRunner.withPropertyValues(
                        "admin.overview.policy.issuance-decrease-ratio=0.25",
                        "admin.overview.policy.issuance-stopped-after=3m",
                        "admin.overview.policy.queue-guidance-threshold=11m",
                        "admin.overview.policy.queue-admission-stopped-after=4m",
                        "admin.overview.policy.stock-depletion-threshold=12m")
                .run(context -> {
                    Object value = ReflectionTestUtils.getField(
                            context.getBean(AdminOverviewService.class), "policy");
                    assertThat(value).isEqualTo(new com.kafkick.core.admin.overview.OverviewCalculationPolicy(
                            0.25, java.time.Duration.ofMinutes(3), java.time.Duration.ofMinutes(11),
                            java.time.Duration.ofMinutes(4), java.time.Duration.ofMinutes(12)));
                });
    }

    /** 최신 series 배선과 CY-455의 DB Reader fallback이 같은 context에서 공존함을 검증합니다. */
    @Test
    void createsExactlyOnePendingReaderWhenObservationReaderIsAbsent() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(AdminCampaignDataReader.class);
            assertThat(context).hasSingleBean(PendingAdminCampaignDataReader.class);
            AdminCampaignDataReader reader = context.getBean(AdminCampaignDataReader.class);
            assertThat(reader.loadCatalog(NOW)).isEqualTo(
                    new AdminCampaignCatalog(SourceStatus.PENDING, null, java.util.List.of()));
            org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                            reader.findDetail(1L, NOW.minusSeconds(60), NOW, NOW))
                    .isInstanceOfSatisfying(BusinessException.class,
                            exception -> assertThat(exception.getErrorCode())
                                    .isEqualTo(AdminApiErrorCode.OBSERVATION_DISABLED));
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
            assertThat(context).hasSingleBean(CouponIssuanceRateReader.class);
            assertThat(context.getBean(CouponIssuanceRateReader.class))
                    .isInstanceOf(PromCouponIssuanceRateReader.class);
            assertThat(ReflectionTestUtils.getField(
                    context.getBean(CouponIssuanceRateReader.class), "rangeQuery"))
                    .isSameAs(context.getBean(AdminObservabilityConfig.SERIES_RANGE_CLIENT, PromRangeQueryClient.class));
            assertThat(ReflectionTestUtils.getField(
                    context.getBean(CouponIssuanceRateReader.class), "timeQuery"))
                    .isSameAs(context.getBean(AdminObservabilityConfig.INSTANT_CLIENT, PromTimeQuery.class));

            assertThat(context).hasSingleBean(AdminOverviewService.class);
            assertThat(ReflectionTestUtils.getField(
                    context.getBean(AdminOverviewService.class), "observationSource"))
                    .isSameAs(context.getBean(OverviewObservationSource.class));
            assertThat(ReflectionTestUtils.getField(
                    context.getBean(AdminCouponMetricsService.class), "issuanceRateReader"))
                    .isSameAs(context.getBean(CouponIssuanceRateReader.class));
        });
    }

    private static RuntimeConfigStore runtimeConfigStore() {
        return new ReadOnlyRuntimeConfigStore(new RuntimeConfigSnapshot(
                EngineVersion.V1, ReleaseStage.V1, QueueMode.OFF, 1L,
                NOW, "test", SourceStatus.VALID));
    }

    @Configuration(proxyBeanMethods = false)
    @ComponentScan(
            basePackageClasses = JdbcAdminCampaignDataReader.class,
            useDefaultFilters = false,
            includeFilters = @ComponentScan.Filter(
                    type = FilterType.ASSIGNABLE_TYPE,
                    classes = JdbcAdminCampaignDataReader.class))
    static class StorageReaderScanConfiguration {

        @Bean
        @Qualifier("obs")
        NamedParameterJdbcTemplate observationNamedParameterJdbcTemplate() {
            return org.mockito.Mockito.mock(NamedParameterJdbcTemplate.class);
        }

        @Bean("observationTransactionManager")
        PlatformTransactionManager observationTransactionManager() {
            return org.mockito.Mockito.mock(PlatformTransactionManager.class);
        }
    }
}
