package com.kafkick.api.admin.support;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import com.fasterxml.jackson.annotation.JsonInclude;

import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.StandaloneMockMvcBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import org.springframework.validation.beanvalidation.SpringConstraintValidatorFactory;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.http.converter.json.JacksonJsonHttpMessageConverter;

import tools.jackson.databind.json.JsonMapper;

import com.kafkick.api.admin.support.config.AdminAnalyticsProperties;
import com.kafkick.api.support.GlobalExceptionHandler;
import com.kafkick.api.support.RequestIdFilter;
import com.kafkick.api.caller.CallerArgumentResolver;
import com.kafkick.api.caller.CallerFilter;
import com.kafkick.api.caller.HeaderCallerResolver;
import com.kafkick.core.admin.couponmetrics.AdminCouponMetricsService;
import com.kafkick.core.admin.couponmetrics.CouponIssuanceRateReader;
import com.kafkick.core.admin.couponmetrics.CouponMetricsCalculator;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSource;
import com.kafkick.api.admin.support.fixture.AdminCouponMetricsTestFixture;
import com.kafkick.api.admin.support.fixture.AdminOverviewTestDataset;
import com.kafkick.api.admin.support.fixture.AdminOverviewTestFixture;
import com.kafkick.core.admin.campaignsource.AdminCampaignCatalog;
import com.kafkick.core.admin.campaignsource.AdminCampaignDataReader;
import com.kafkick.core.admin.campaignsource.AdminCampaignDetailData;
import com.kafkick.core.admin.campaignsource.DetailAvailability;
import com.kafkick.core.admin.campaignsource.PreparationObservation;
import com.kafkick.core.admin.analytics.AdminAnalyticsCalculator;
import com.kafkick.core.admin.analytics.AdminAnalyticsFreshnessPolicy;
import com.kafkick.core.admin.analytics.AdminAnalyticsService;
import com.kafkick.core.admin.analytics.mock.AdminAnalyticsMockDataFactory;
import com.kafkick.core.admin.analytics.mock.AdminAnalyticsMockSource;
import com.kafkick.core.admin.overview.AdminOverviewService;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.admin.overview.calculator.CampaignOverviewCalculator;
import com.kafkick.core.admin.overview.calculator.CampaignQueueCalculator;
import com.kafkick.core.admin.overview.calculator.CustomerOutcomeCalculator;
import com.kafkick.core.admin.overview.calculator.IssuanceActionCalculator;
import com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator;
import com.kafkick.core.admin.overview.calculator.OperationActionCalculator;
import com.kafkick.core.admin.overview.calculator.OverviewStatusCalculator;
import com.kafkick.core.admin.overview.calculator.StockRiskCalculator;
import com.kafkick.core.admin.overview.observation.OverviewObservationData;
import com.kafkick.core.admin.overview.observation.OverviewObservationSource;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.runtimeconfig.ReadOnlyRuntimeConfigStore;
import com.kafkick.core.runtimeconfig.RuntimeConfigSnapshot;
import com.kafkick.core.support.TimeProvider;

/**
 * 관리자 Controller 계약 테스트에 공통 MockMvc, Validation, 오류 봉투, 헤더 경계를 구성합니다.
 *
 * <p>일반 Controller 테스트에는 유효한 관리자 헤더를 기본 제공하고, 인증 실패 테스트만 명시적으로
 * 헤더 없는 구성을 사용합니다. 따라서 각 테스트는 검증하려는 API 계약에만 집중할 수 있습니다.</p>
 */
public final class AdminControllerContractTestSupport {

    private AdminControllerContractTestSupport() {
    }

    public static MockMvc mockMvc(Object controller) {
        return build(controller, true, false);
    }

    /** 운영 NON_NULL 직렬화를 직접 검증할 Controller에 실제 JSON 생략 정책을 적용합니다. */
    public static MockMvc mockMvcWithNonNullJson(Object controller) {
        return build(controller, true, true);
    }

    /** 지정한 Clock으로 관리자 Overview의 실제 Service 조립을 재사용합니다. */
    public static AdminOverviewService overviewService(Clock clock) {
        AdminOverviewTestFixture fixture = new AdminOverviewTestFixture();
        return overviewService(clock, campaignReader(fixture), fixture);
    }

    /** 오류 분기 HTTP 계약 테스트가 지정한 Core Reader를 Overview에도 그대로 사용합니다. */
    public static AdminOverviewService overviewService(
            Clock clock,
            AdminCampaignDataReader reader
    ) {
        return overviewService(clock, reader, new AdminOverviewTestFixture());
    }

    private static AdminOverviewService overviewService(
            Clock clock,
            AdminCampaignDataReader reader,
            AdminOverviewTestFixture fixture
    ) {
        Instant snapshotAt = clock.instant();
        return new AdminOverviewService(
                new TimeProvider(clock),
                reader,
                new ReadOnlyRuntimeConfigStore(new RuntimeConfigSnapshot(
                        EngineVersion.V1, ReleaseStage.V1, QueueMode.OFF,
                        1L, snapshotAt, "test", SourceStatus.VALID)),
                fixture.create(snapshotAt).policy(),
                overviewObservationSource(fixture),
                new IssuanceFlowCalculator(),
                new IssuanceActionCalculator(),
                new CampaignQueueCalculator(),
                new CustomerOutcomeCalculator(),
                new StockRiskCalculator(),
                new CampaignOverviewCalculator(),
                new OperationActionCalculator(),
                new OverviewStatusCalculator());
    }

    /** Controller 계약 테스트의 고정 Dataset을 기술 중립 관측 묶음으로 제공합니다. */
    private static OverviewObservationSource overviewObservationSource(AdminOverviewTestFixture fixture) {
        return request -> {
            AdminOverviewTestDataset dataset = fixture.create(request.snapshotAt());
            AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.AggregateIssuanceRate> aggregatePending =
                    new AdminOverviewSnapshot.Observation<>(null, SourceStatus.PENDING, null);
            AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.LatencySummary> sourceObservation =
                    dataset.latencySummary();
            AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.LatencySummary> observedLatency;
            if (!sourceObservation.status().carriesValue()) {
                // 값 없는 상태는 null 값·시각 계약까지 포함하므로 새 Observation으로 다시 만들지 않습니다.
                observedLatency = sourceObservation;
            } else {
                AdminOverviewSnapshot.LatencySummary sourceLatency = sourceObservation.value();
                // 실 Prom 경계와 같이 성공 p99만 값으로 싣고 미관측 실패 p99는 null로 유지합니다.
                observedLatency = new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.LatencySummary(
                                sourceLatency.successfulP99(), null,
                                sourceLatency.windowStart(), sourceLatency.windowEnd()),
                        sourceObservation.status(), sourceObservation.observedAt());
            }
            return new OverviewObservationData(
                    request,
                    dataset.issuanceFlowInputs().stream()
                            .filter(input -> request.campaignTargets().stream()
                                    .anyMatch(target -> target.couponId().equals(input.couponId())))
                            .toList(),
                    dataset.outcomeInput(),
                    aggregatePending,
                    observedLatency);
        };
    }

    /** 지정한 Clock과 Overview 모집단으로 캠페인 상세 지표 실제 Service를 구성합니다. */
    public static AdminCouponMetricsService couponMetricsService(Clock clock) {
        AdminOverviewTestFixture overviewFixture = new AdminOverviewTestFixture();
        return new AdminCouponMetricsService(
                new TimeProvider(clock),
                campaignReader(overviewFixture),
                pendingCouponIssuanceRateReader(),
                new CouponMetricsCalculator());
    }

    /** 오류 분기 HTTP 계약 테스트가 지정한 Core Reader를 그대로 사용합니다. */
    public static AdminCouponMetricsService couponMetricsService(
            Clock clock,
            AdminCampaignDataReader reader
    ) {
        return new AdminCouponMetricsService(
                new TimeProvider(clock), reader, pendingCouponIssuanceRateReader(), new CouponMetricsCalculator());
    }

    /** Controller JSON 계약은 외부 Prometheus 대신 값 없는 PENDING 발급률만 고정합니다. */
    private static CouponIssuanceRateReader pendingCouponIssuanceRateReader() {
        return (couponId, window, snapshotAt) -> new CouponMetricsSource.Observation<>(
                null, SourceStatus.PENDING, null);
    }

    /** 기존 Controller JSON fixture를 Core Port 형태로만 제공해 API가 Storage 구현을 참조하지 않게 합니다. */
    private static AdminCampaignDataReader campaignReader(AdminOverviewTestFixture overviewFixture) {
        AdminCouponMetricsTestFixture detailFixture = new AdminCouponMetricsTestFixture(overviewFixture);
        return new AdminCampaignDataReader() {
            @Override
            public AdminCampaignCatalog loadCatalog(Instant snapshotAt) {
                AdminOverviewTestDataset dataset = overviewFixture.create(snapshotAt);
                return new AdminCampaignCatalog(SourceStatus.VALID, snapshotAt,
                        dataset.campaigns().stream()
                                .map(campaign -> new AdminCampaignCatalog.CampaignData(
                                        campaign.couponId(), campaign.campaignName(), campaign.brandName(),
                                        campaign.status(), campaign.opensAt(), campaign.closesAt(),
                                        new CouponMetricsSource.Observation<>(
                                                campaign.stockStatus().carriesValue()
                                                        ? new CouponMetricsSource.StockCounts(
                                                        campaign.totalQuantity(), campaign.activeCount()) : null,
                                                campaign.stockStatus(), campaign.stockObservedAt()),
                                        new PreparationObservation(null, SourceStatus.PENDING, null)))
                                .toList());
            }

            @Override
            public AdminCampaignDetailData findDetail(
                    long couponId,
                    Instant fromInclusive,
                    Instant toExclusive,
                    Instant snapshotAt
            ) {
                return detailFixture.find(snapshotAt, couponId)
                        .map(source -> new AdminCampaignDetailData(
                                DetailAvailability.AVAILABLE,
                                new AdminCampaignDetailData.DetailValue(
                                        source.couponId(), "campaign-" + source.couponId(), "brand",
                                        source.campaign(), source.stock(), source.holdingCounts(),
                                        source.transitions())))
                        .orElseGet(() -> new AdminCampaignDetailData(DetailAvailability.NOT_FOUND, null));
            }
        };
    }

    /** 지정한 Clock으로 관리자 브랜드 분석 Mock Service를 구성합니다. */
    public static AdminAnalyticsService analyticsService(Clock clock) {
        TimeProvider timeProvider = new TimeProvider(clock);
        return new AdminAnalyticsService(
                new AdminAnalyticsMockSource(new AdminAnalyticsMockDataFactory(), clock.instant()),
                timeProvider,
                new AdminAnalyticsCalculator(new AdminAnalyticsFreshnessPolicy(java.time.Duration.ofHours(1))));
    }

    /** 인증 실패 HTTP 상태를 검증할 때 기본 관리자 헤더 없이 MockMvc를 구성합니다. */
    public static MockMvc mockMvcWithoutAdminHeaders(Object controller) {
        return build(controller, false, false);
    }

    /** Controller별 인증 헤더와 선택적 NON_NULL 직렬화를 가진 standalone MockMvc를 만듭니다. */
    private static MockMvc build(
            Object controller,
            boolean defaultAdminHeaders,
            boolean nonNullJson
    ) {
        Clock fixedClock = Clock.fixed(Instant.parse("2026-08-16T00:00:00Z"), ZoneOffset.UTC);
        DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
        beanFactory.registerSingleton("adminAnalyticsProperties", AdminAnalyticsProperties.withMockEnabled(1));
        LocalValidatorFactoryBean validator = new LocalValidatorFactoryBean();
        validator.setConstraintValidatorFactory(new SpringConstraintValidatorFactory(beanFactory));
        validator.afterPropertiesSet();
        DefaultFormattingConversionService conversionService = new DefaultFormattingConversionService();
        conversionService.addConverter(new MetricsWindowConverter());
        StandaloneMockMvcBuilder builder = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler(new TimeProvider(fixedClock)))
                .setValidator(validator)
                .setConversionService(conversionService)
                .addInterceptors(
                    new AdminAuthorizationInterceptor(),
                    new BenchmarkCommandAuthorizationInterceptor(
                        "test-benchmark-secret-at-least-32-bytes"))
                .setCustomArgumentResolvers(new CallerArgumentResolver())
                .addFilters(new CallerFilter(new HeaderCallerResolver()), new RequestIdFilter());
        if (nonNullJson) {
            builder.setMessageConverters(jsonMessageConverter());
        }
        if (defaultAdminHeaders) {
            // 정상 API 계약 테스트가 인증 실패에 가려지지 않도록 검증된 관리자 헤더를 공통 적용합니다.
            builder.defaultRequest(get("/")
                    .header(HeaderCallerResolver.USER_ID_HEADER, "812934")
                    .header(AdminAuthorizationInterceptor.USER_ROLE_HEADER, "ADMIN")
                    .header(BenchmarkCommandAuthorizationInterceptor.SECRET_HEADER,
                        "test-benchmark-secret-at-least-32-bytes"));
        }
        return builder.build();
    }

    /** 실제 API와 같은 NON_NULL JSON 정책을 standalone Controller 계약 테스트에 적용합니다. */
    private static JacksonJsonHttpMessageConverter jsonMessageConverter() {
        JsonMapper jsonMapper = JsonMapper.builder()
                .changeDefaultPropertyInclusion(
                        inclusion -> inclusion.withValueInclusion(JsonInclude.Include.NON_NULL))
                .build();
        return new JacksonJsonHttpMessageConverter(jsonMapper);
    }
}
