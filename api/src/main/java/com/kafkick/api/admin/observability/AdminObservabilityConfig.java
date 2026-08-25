package com.kafkick.api.admin.observability;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

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
        OverviewPrometheusProperties.class
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

    /** API 전용 Prom 관측 원천과 Core 계산기를 기술 중립 Overview Service에 명시적으로 배선합니다. */
    @Bean
    public AdminOverviewService adminOverviewService(
            TimeProvider timeProvider,
            AdminOverviewMockDataFactory mockDataFactory,
            OverviewObservationSource observationSource,
            IssuanceFlowCalculator issuanceFlowCalculator,
            IssuanceActionCalculator issuanceActionCalculator,
            CampaignQueueCalculator campaignQueueCalculator,
            CustomerOutcomeCalculator customerOutcomeCalculator,
            StockRiskCalculator stockRiskCalculator,
            CampaignOverviewCalculator campaignOverviewCalculator,
            ConsistencyActionCalculator consistencyActionCalculator,
            OperationActionCalculator operationActionCalculator,
            OverviewStatusCalculator overviewStatusCalculator
    ) {
        return new AdminOverviewService(
                timeProvider, mockDataFactory, observationSource, issuanceFlowCalculator,
                issuanceActionCalculator, campaignQueueCalculator, customerOutcomeCalculator,
                stockRiskCalculator, campaignOverviewCalculator, consistencyActionCalculator,
                operationActionCalculator, overviewStatusCalculator);
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
