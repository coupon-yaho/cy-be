package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;

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
import com.kafkick.core.admin.CouponPolicyType;
import com.kafkick.core.admin.couponroundsource.AdminCouponRoundCatalog;
import com.kafkick.core.admin.couponroundsource.AdminCouponRoundDataReader;
import com.kafkick.core.admin.couponroundsource.AdminCouponRoundDetailData;
import com.kafkick.core.admin.couponroundsource.PreparationSource;
import com.kafkick.core.admin.couponmetrics.AdminCouponMetricsService;
import com.kafkick.core.admin.couponmetrics.CouponIssuanceRateReader;
import com.kafkick.core.admin.couponmetrics.CouponMetricsCalculator;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSource;
import com.kafkick.core.benchmark.BenchmarkRunRepository;
import com.kafkick.core.benchmark.RunTimeseriesArchiver.ArchiveStore;
import com.kafkick.core.admin.overview.AdminOverviewService;
import com.kafkick.core.admin.overview.calculator.CouponRoundOverviewCalculator;
import com.kafkick.core.admin.overview.calculator.CouponRoundPreparationCalculator;
import com.kafkick.core.admin.overview.calculator.CouponRoundQueueCalculator;
import com.kafkick.core.admin.overview.calculator.ConsistencyActionCalculator;
import com.kafkick.core.admin.overview.calculator.CustomerOutcomeCalculator;
import com.kafkick.core.admin.overview.calculator.IssuanceActionCalculator;
import com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator;
import com.kafkick.core.admin.overview.calculator.OperationActionCalculator;
import com.kafkick.core.admin.overview.calculator.OverviewStatusCalculator;
import com.kafkick.core.admin.overview.calculator.StockRiskCalculator;
import com.kafkick.core.admin.overview.observation.OverviewObservationSource;
import com.kafkick.core.admin.preparation.AdminPreparationResolver;
import com.kafkick.core.admin.preparation.V2AdminPreparationReader;
import com.kafkick.core.admin.preparation.V2PreparationSource;
import com.kafkick.core.admin.queue.AdminQueueObservationSource;
import com.kafkick.core.admin.queue.PendingAdminQueueObservationSource;
import com.kafkick.core.admin.queue.mock.MockAdminQueueObservationSource;
import com.kafkick.core.consistency.ConsistencyFinalObservation;
import com.kafkick.core.consistency.ConsistencyFinalReader;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.runtimeconfig.ReadOnlyRuntimeConfigStore;
import com.kafkick.core.runtimeconfig.RuntimeConfigSnapshot;
import com.kafkick.core.runtimeconfig.RuntimeConfigStore;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.storage.db.admin.JdbcAdminCouponRoundDataReader;

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
                    CouponRoundQueueCalculator.class,
                    CustomerOutcomeCalculator.class,
                    StockRiskCalculator.class,
                    CouponRoundOverviewCalculator.class,
                    CouponRoundPreparationCalculator.class,
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
                    assertThat(context).hasSingleBean(AdminCouponRoundDataReader.class);
                    assertThat(context.getBean(AdminCouponRoundDataReader.class))
                            .isInstanceOf(JdbcAdminCouponRoundDataReader.class);
                    assertThat(context).doesNotHaveBean(PendingAdminCouponRoundDataReader.class);
                    assertThat(context).hasSingleBean(AdminOverviewService.class);
                    assertThat(ReflectionTestUtils.getField(
                            context.getBean(AdminOverviewService.class), "couponRoundDataReader"))
                            .isSameAs(context.getBean(AdminCouponRoundDataReader.class));
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

    /** 기본 운영 설정은 가짜값 대신 PENDING 대기열 원천을 두 Service에 같은 인스턴스로 주입합니다. */
    @Test
    void usesOnePendingQueueSourceByDefault() {
        contextRunner.run(context -> {
            AdminQueueObservationSource queueSource = context.getBean(AdminQueueObservationSource.class);

            assertThat(queueSource).isInstanceOf(PendingAdminQueueObservationSource.class);
            assertThat(ReflectionTestUtils.getField(
                    context.getBean(AdminOverviewService.class), "queueObservationSource"))
                    .isSameAs(queueSource);
            assertThat(ReflectionTestUtils.getField(
                    context.getBean(AdminCouponMetricsService.class), "queueObservationSource"))
                    .isSameAs(queueSource);
        });
    }

    /** 프론트 연동 환경이 Mock을 명시적으로 켤 때만 결정적 대기열 원천을 선택합니다. */
    @Test
    void usesMockQueueSourceOnlyWhenExplicitlyEnabled() {
        contextRunner.withPropertyValues("admin.queue.mock-enabled=true")
                .run(context -> assertThat(context.getBean(AdminQueueObservationSource.class))
                        .isInstanceOf(MockAdminQueueObservationSource.class));
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
    void injectsTheAvailableFinalReaderAndConsistencyCalculatorIntoOverviewService() {
        ConsistencyFinalReader finalReader = couponIds -> Map.of(
                couponIds.getFirst(),
                new ConsistencyFinalObservation(SourceStatus.N_A, null));

        contextRunner.withBean(ConsistencyFinalReader.class, () -> finalReader)
                .run(context -> {
                    AdminOverviewService service = context.getBean(AdminOverviewService.class);
                    assertThat(ReflectionTestUtils.getField(service, "consistencyFinalReader"))
                            .isSameAs(finalReader);
                    assertThat(ReflectionTestUtils.getField(service, "consistencyActionCalculator"))
                            .isSameAs(context.getBean(ConsistencyActionCalculator.class));
                });
    }

    @Test
    void usesPendingFinalReaderWhenObservationStorageIsAbsent() {
        contextRunner.run(context -> {
            ConsistencyFinalReader reader = (ConsistencyFinalReader) ReflectionTestUtils.getField(
                    context.getBean(AdminOverviewService.class), "consistencyFinalReader");

            assertThat(reader).isInstanceOf(PendingConsistencyFinalReader.class);
            assertThat(reader.findLatestByCouponIds(List.of(11L, 12L)))
                    .containsOnly(
                            org.assertj.core.api.Assertions.entry(11L,
                                    new ConsistencyFinalObservation(SourceStatus.PENDING, null)),
                            org.assertj.core.api.Assertions.entry(12L,
                                    new ConsistencyFinalObservation(SourceStatus.PENDING, null)));
            assertThat(reader.findLatestByCouponIds(List.of())).isEmpty();
            Map<Long, ConsistencyFinalObservation> normalized =
                    reader.findLatestByCouponIds(List.of(12L, 11L, 12L));
            assertThat(normalized.keySet()).containsExactly(12L, 11L);
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> normalized.put(
                            13L, new ConsistencyFinalObservation(SourceStatus.PENDING, null)))
                    .isInstanceOf(UnsupportedOperationException.class);
            org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                            reader.findLatestByCouponIds(List.of(0L)))
                    .isInstanceOf(IllegalArgumentException.class);
            org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                            reader.findLatestByCouponIds(java.util.Arrays.asList(11L, null)))
                    .isInstanceOf(IllegalArgumentException.class);
        });
    }

    /** Redis 준비 Reader가 있으면 Overview 준비 Resolver가 같은 인스턴스로 V2 회차를 판정합니다. */
    @Test
    void injectsTheAvailableV2PreparationReaderIntoOverviewService() {
        V2AdminPreparationReader reader = (requests, observedAt) -> {
            assertThat(requests).singleElement().satisfies(request -> {
                assertThat(request.couponId()).isEqualTo(10L);
                assertThat(request.expectedGradeMask()).isEqualTo(3);
                assertThat(request.expectedTotalQuantity()).isEqualTo(100L);
                assertThat(request.expectedRemainingQuantity()).isEqualTo(100L);
            });
            assertThat(observedAt).isEqualTo(NOW);
            return Map.of(10L, new V2PreparationSource(true, true, SourceStatus.VALID, NOW));
        };

        contextRunner.withBean(V2AdminPreparationReader.class, () -> reader)
                .run(context -> {
                    AdminPreparationResolver resolver = preparationResolver(context.getBean(
                            AdminOverviewService.class));

                    assertThat(ReflectionTestUtils.getField(resolver, "v2Reader")).isSameAs(reader);
                    assertThat(resolver.resolve(v2ReadyCatalog(), NOW)).containsEntry(
                            10L, new V2PreparationSource(true, true, SourceStatus.VALID, NOW));
                });
    }

    /** Redis 준비 Reader Bean이 없으면 조회 대상 V2 회차만 UNAVAILABLE로 반환합니다. */
    @Test
    void usesUnavailableV2PreparationReaderWhenRedisReaderIsAbsent() {
        contextRunner.run(context -> {
            AdminPreparationResolver resolver = preparationResolver(context.getBean(
                    AdminOverviewService.class));

            assertThat(resolver.resolve(v2ReadyCatalog(), NOW))
                    .containsEntry(10L, V2PreparationSource.unavailable());
        });
    }

    /** 최신 series 배선과 CY-455의 DB Reader fallback이 같은 context에서 공존함을 검증합니다. */
    @Test
    void createsExactlyOnePendingReaderWhenObservationReaderIsAbsent() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(AdminCouponRoundDataReader.class);
            assertThat(context).hasSingleBean(PendingAdminCouponRoundDataReader.class);
            AdminCouponRoundDataReader reader = context.getBean(AdminCouponRoundDataReader.class);
            assertThat(reader.loadCatalog(NOW)).isEqualTo(
                    new AdminCouponRoundCatalog(SourceStatus.PENDING, null, java.util.List.of()));
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

    /** Overview Service에 주입된 V2 준비 Resolver를 배선 검증용으로 읽습니다. */
    private static AdminPreparationResolver preparationResolver(AdminOverviewService service) {
        return (AdminPreparationResolver) ReflectionTestUtils.getField(service, "preparationResolver");
    }

    /** Redis 준비 조회 계약을 모두 충족하는 V2 예약 회차 카탈로그를 만듭니다. */
    private static AdminCouponRoundCatalog v2ReadyCatalog() {
        return new AdminCouponRoundCatalog(SourceStatus.VALID, NOW, List.of(
                new AdminCouponRoundCatalog.CouponRoundData(
                        10L,
                        "V2 couponRound",
                        "brand",
                        EngineVersion.V2,
                        CouponRoundStatus.SCHEDULED,
                        NOW.plusSeconds(600),
                        NOW.plusSeconds(3_600),
                        new CouponMetricsSource.Observation<>(
                                new CouponMetricsSource.StockCounts(100L, 0L),
                                SourceStatus.VALID,
                                NOW),
                        new PreparationSource(
                                true,
                                true,
                                CouponPolicyType.FIXED_AMOUNT,
                                3,
                                SourceStatus.VALID,
                                NOW))));
    }

    @Configuration(proxyBeanMethods = false)
    @ComponentScan(
            basePackageClasses = JdbcAdminCouponRoundDataReader.class,
            useDefaultFilters = false,
            includeFilters = @ComponentScan.Filter(
                    type = FilterType.ASSIGNABLE_TYPE,
                    classes = JdbcAdminCouponRoundDataReader.class))
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
