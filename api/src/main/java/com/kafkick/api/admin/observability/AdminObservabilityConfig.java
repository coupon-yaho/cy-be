package com.kafkick.api.admin.observability;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import com.kafkick.core.admin.couponroundsource.AdminCouponRoundDataReader;
import com.kafkick.core.admin.couponmetrics.AdminCouponMetricsService;
import com.kafkick.core.admin.couponmetrics.CouponIssuanceRateReader;
import com.kafkick.core.admin.couponmetrics.CouponMetricsCalculator;
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
import com.kafkick.core.admin.queue.AdminQueueObservationSource;
import com.kafkick.core.admin.queue.PendingAdminQueueObservationSource;
import com.kafkick.core.admin.queue.mock.MockAdminQueueObservationSource;
import com.kafkick.core.admin.stock.AdminStockResolver;
import com.kafkick.core.admin.stock.V2AdminStockReader;
import com.kafkick.core.consistency.ConsistencyFinalReader;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.runtimeconfig.RuntimeConfigStore;
import com.kafkick.core.benchmark.BenchmarkRunRepository;
import com.kafkick.core.benchmark.RunTimeseriesArchiver;
import com.kafkick.core.benchmark.RunTimeseriesArchiver.ArchiveStore;

/**
 * 관제 지표 조회가 Prometheus 를 읽는 경로를 배선합니다.
 *
 * <p>전용 {@link RestClient} 를 씁니다. 공용 클라이언트를 쓰면 다른 호출의 타임아웃 설정이
 * 관제 폴링에 그대로 걸립니다 — 화면은 1 초마다 부르므로 여기서 오래 기다리면 안 됩니다.</p>
 */
@Configuration
@EnableConfigurationProperties({
        PrometheusQueryProperties.class,
        PrometheusArchiveProperties.class,
        PrometheusSeriesProperties.class,
        OverviewPrometheusProperties.class,
        AdminOverviewPolicyProperties.class,
        AdminQueueMockProperties.class,
        QueueGatewayPrometheusProperties.class
})
public class AdminObservabilityConfig {

    /**
     * range client 빈 이름입니다. {@link PromRangeQuery} 구현이 둘이라 타입만으로는 고를 수 없고,
     * 잘못 고르면 {@code /metrics/series} 가 1초 폴링 타임아웃을 물거나 Overview 추세가 series
     * 예산을 물게 됩니다. 이름이 곧 계약이므로 문자열을 옮겨 적지 않습니다.
     */
    public static final String OVERVIEW_RANGE_CLIENT = "promRangeQueryClient";
    public static final String SERIES_RANGE_CLIENT = "seriesPromRangeQueryClient";

    /**
     * instant client 빈 이름입니다. 둘 다 {@link PromQueryClient} 라 타입만으로는 고를 수 없고,
     * 잘못 고르면 1초 폴링 경로가 archive 의 read 10초를 물어 화면이 스스로 부하가 됩니다.
     */
    public static final String INSTANT_CLIENT = "promQueryClient";
    public static final String ARCHIVE_INSTANT_CLIENT = "archivePromQueryClient";

    @Bean
    public PromQueryClient promQueryClient(PrometheusQueryProperties properties) {
        return new PromQueryClient(prometheusRestClient(properties));
    }

    /**
     * instant-vector 계약을 약화하지 않는 별도 matrix range client를 등록합니다.
     *
     * @param properties Prometheus 접속·타임아웃 설정
     * @param overviewProperties Overview 집계 구간과 range 조회 안전 상한
     * @return 외부 설정의 조회 범위·평가점 상한을 사용하는 range client
     */
    @Bean
    public PromRangeQueryClient promRangeQueryClient(
            PrometheusQueryProperties properties,
            OverviewPrometheusProperties overviewProperties
    ) {
        return new PromRangeQueryClient(
                prometheusRestClient(properties),
                overviewProperties.maxRange(),
                overviewProperties.maxPoints());
    }

    /**
     * Prometheus 결과를 Core의 기술 중립 Overview 입력으로 바꾸는 원천을 등록합니다.
     *
     * @param instantQuery Overview snapshot 평가 시각을 명시하는 instant-vector 경계
     * @param rangeQuery 별도 matrix range 경계
     * @param properties stale·전체 시작 예산 설정
     * @param overviewProperties Overview 집계 구간과 range 조회 안전 상한
     * @return Core가 의존할 기술 중립 관측 원천
     */
    @Bean
    public OverviewObservationSource promOverviewObservationSource(
            @Qualifier(INSTANT_CLIENT) PromTimeQuery instantQuery,
            @Qualifier(OVERVIEW_RANGE_CLIENT) PromRangeQuery rangeQuery,
            PrometheusQueryProperties properties,
            OverviewPrometheusProperties overviewProperties
    ) {
        return new PromOverviewObservationSource(
                instantQuery, rangeQuery, properties.staleAfter(), properties.totalBudget(),
                overviewProperties);
    }

    /**
     * 운영 기본 PENDING 원천과 프론트 시연용 Mock 원천을 하나의 Core Port로 선택합니다.
     *
     * @param properties 명시적 Mock 활성화 여부; 기본값은 false
     * @return 두 관리자 Service가 공유할 대기열 관측 원천
     */
    @Bean
    public AdminQueueObservationSource adminQueueObservationSource(AdminQueueMockProperties properties) {
        if (properties.isMockEnabled()) {
            // Mock은 명시적으로 켠 로컬·프론트 연동 환경에서만 가짜 관측값을 노출합니다.
            return new MockAdminQueueObservationSource();
        }
        return new PendingAdminQueueObservationSource();
    }

    /** API 관측 원천과 V2 재고·준비 Reader를 기술 중립 Overview Service에 명시적으로 배선합니다. */
    @Bean
    public AdminOverviewService adminOverviewService(
            TimeProvider timeProvider,
            AdminCouponRoundDataReader couponRoundDataReader,
            RuntimeConfigStore runtimeConfigStore,
            AdminOverviewPolicyProperties policyProperties,
            OverviewObservationSource observationSource,
            AdminQueueObservationSource queueObservationSource,
            IssuanceFlowCalculator issuanceFlowCalculator,
            IssuanceActionCalculator issuanceActionCalculator,
            CouponRoundQueueCalculator couponRoundQueueCalculator,
            CustomerOutcomeCalculator customerOutcomeCalculator,
            StockRiskCalculator stockRiskCalculator,
            CouponRoundOverviewCalculator couponRoundOverviewCalculator,
            CouponRoundPreparationCalculator couponRoundPreparationCalculator,
            ObjectProvider<ConsistencyFinalReader> consistencyFinalReaderProvider,
            ConsistencyActionCalculator consistencyActionCalculator,
            OperationActionCalculator operationActionCalculator,
            OverviewStatusCalculator overviewStatusCalculator,
            ObjectProvider<V2AdminStockReader> v2AdminStockReaderProvider,
            ObjectProvider<V2AdminPreparationReader> v2AdminPreparationReaderProvider
    ) {
        ConsistencyFinalReader consistencyFinalReader = consistencyFinalReaderProvider
                .getIfAvailable(PendingConsistencyFinalReader::new);
        return new AdminOverviewService(
                timeProvider, couponRoundDataReader, runtimeConfigStore, policyProperties.toCorePolicy(),
                observationSource, queueObservationSource, issuanceFlowCalculator,
                issuanceActionCalculator, couponRoundQueueCalculator, customerOutcomeCalculator,
                stockRiskCalculator, couponRoundOverviewCalculator, couponRoundPreparationCalculator,
                consistencyFinalReader, consistencyActionCalculator,
                operationActionCalculator, overviewStatusCalculator,
                new AdminStockResolver(v2AdminStockReaderProvider
                        .getIfAvailable(AdminStockResolver::unavailableV2Reader)),
                new AdminPreparationResolver(v2AdminPreparationReaderProvider
                        .getIfAvailable(AdminPreparationResolver::unavailableV2Reader)));
    }

    /** 관측 JDBC Reader가 없을 때만 관측 비활성 오류를 내는 Core Port 구현을 제공합니다. */
    @Bean
    @ConditionalOnMissingBean(AdminCouponRoundDataReader.class)
    public AdminCouponRoundDataReader pendingAdminCouponRoundDataReader() {
        return new PendingAdminCouponRoundDataReader();
    }

    /** 쿠폰 회차 상세 발급률은 series 전용 range와 instant freshness 경계를 함께 사용합니다. */
    @Bean
    public CouponIssuanceRateReader promCouponIssuanceRateReader(
            @Qualifier(SERIES_RANGE_CLIENT) PromRangeQuery rangeQuery,
            @Qualifier(INSTANT_CLIENT) PromTimeQuery timeQuery,
            PrometheusSeriesProperties seriesProperties,
            PrometheusQueryProperties queryProperties
    ) {
        return new PromCouponIssuanceRateReader(
                rangeQuery, timeQuery, seriesProperties, queryProperties.staleAfter());
    }

    /** 상세 화면도 Storage 구현 타입이 아니라 동일한 Core Port만 주입받습니다. */
    @Bean
    public AdminCouponMetricsService adminCouponMetricsService(
            TimeProvider timeProvider,
            AdminCouponRoundDataReader couponRoundDataReader,
            CouponIssuanceRateReader issuanceRateReader,
            AdminQueueObservationSource queueObservationSource,
            CouponMetricsCalculator calculator,
            ObjectProvider<V2AdminStockReader> v2AdminStockReaderProvider
    ) {
        return new AdminCouponMetricsService(
                timeProvider, couponRoundDataReader, issuanceRateReader, queueObservationSource, calculator,
                new AdminStockResolver(v2AdminStockReaderProvider
                        .getIfAvailable(AdminStockResolver::unavailableV2Reader)));
    }

    /** 동일한 연결·읽기 타임아웃의 Prometheus 전용 RestClient를 생성합니다. */
    private static RestClient prometheusRestClient(PrometheusQueryProperties properties) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(properties.connectTimeout());
        requestFactory.setReadTimeout(properties.readTimeout());
        return RestClient.builder()
                .baseUrl(properties.baseUrl())
                .requestFactory(requestFactory)
                .build();
    }

    @Bean
    public PromQueryClient archivePromQueryClient(
            PrometheusQueryProperties queryProperties, PrometheusArchiveProperties archiveProperties) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(archiveProperties.connectTimeout());
        requestFactory.setReadTimeout(archiveProperties.readTimeout());
        return new PromQueryClient(RestClient.builder()
                .baseUrl(queryProperties.baseUrl())
                .requestFactory(requestFactory)
                .build());
    }

    /**
     * {@code /metrics/series} 전용 range client 를 등록합니다.
     *
     * <p><b>{@code promRangeQueryClient} 와 RestClient 를 공유하지 않습니다.</b> 저쪽은 1초 폴링
     * 예산의 connect 100ms · read 300ms 를 물고 있어 range 조회에는 짧습니다. 여기서 전용
     * 타임아웃을 물려야 이 경로가 느려도 {@code /metrics} 가 영향을 받지 않습니다.</p>
     *
     * @param queryProperties Prometheus 주소
     * @param seriesProperties series 전용 타임아웃과 평가점 상한
     * @return series 전용 range client
     */
    @Bean
    public PromRangeQueryClient seriesPromRangeQueryClient(
            PrometheusQueryProperties queryProperties, PrometheusSeriesProperties seriesProperties) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(seriesProperties.connectTimeout());
        requestFactory.setReadTimeout(seriesProperties.readTimeout());
        return new PromRangeQueryClient(
                RestClient.builder()
                        .baseUrl(queryProperties.baseUrl())
                        .requestFactory(requestFactory)
                        .build(),
                seriesProperties.maxRange(),
                seriesProperties.maxPoints());
    }

    @Bean
    public PromSeriesAssembler promSeriesAssembler(
            @Qualifier(SERIES_RANGE_CLIENT) PromRangeQuery rangeQuery,
            TimeProvider timeProvider, PrometheusSeriesProperties seriesProperties) {
        return new PromSeriesAssembler(rangeQuery, timeProvider, seriesProperties);
    }

    @Bean
    public PromMetricsAssembler promMetricsAssembler(
            @Qualifier(INSTANT_CLIENT) PromQuery client,
            TimeProvider timeProvider, PrometheusQueryProperties properties) {
        return new PromMetricsAssembler(
                client, timeProvider, properties.staleAfter(), properties.totalBudget());
    }

    @Bean
    @ConditionalOnProperty(prefix = "observation.datasource", name = "enabled", havingValue = "true")
    public RunTimeseriesArchiver runTimeseriesArchiver(
            BenchmarkRunRepository runs,
            @Qualifier(ARCHIVE_INSTANT_CLIENT) PromQueryClient source,
            ArchiveStore store,
            @org.springframework.beans.factory.annotation.Value(
                "${benchmark.archive.claim-lease:5m}") java.time.Duration claimLease,
            @org.springframework.beans.factory.annotation.Value(
                "${benchmark.archive.max-samples:10000}") int maxSamples,
            @org.springframework.beans.factory.annotation.Value(
                "${benchmark.archive.write-chunk-size:500}") int writeChunkSize) {
        return new RunTimeseriesArchiver(
            runs, source, store, claimLease, maxSamples, writeChunkSize);
    }
}
