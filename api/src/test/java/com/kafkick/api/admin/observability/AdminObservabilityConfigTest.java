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
                    "observation.datasource.enabled=false")
            .withBean(TimeProvider.class, () -> new TimeProvider(Clock.systemUTC()))
            .withBean(RuntimeConfigStore.class, AdminObservabilityConfigTest::runtimeConfigStore)
            .withBean(BenchmarkRunRepository.class,
                    () -> org.mockito.Mockito.mock(BenchmarkRunRepository.class))
            .withBean(ArchiveStore.class, () -> org.mockito.Mockito.mock(ArchiveStore.class));

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
                    assertThat(context).hasSingleBean(PromRangeQueryClient.class);
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
