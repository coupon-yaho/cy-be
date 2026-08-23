package com.kafkick.api.admin.observability;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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

/**
 * 관제 지표 조회가 Prometheus 를 읽는 경로를 배선합니다.
 *
 * <p>전용 {@link RestClient} 를 씁니다. 공용 클라이언트를 쓰면 다른 호출의 타임아웃 설정이
 * 관제 폴링에 그대로 걸립니다 — 화면은 1 초마다 부르므로 여기서 오래 기다리면 안 됩니다.</p>
 */
@Configuration
@EnableConfigurationProperties(PrometheusQueryProperties.class)
public class AdminObservabilityConfig {

    @Bean
    public PromQueryClient promQueryClient(PrometheusQueryProperties properties) {
        return new PromQueryClient(prometheusRestClient(properties));
    }

    /**
     * instant-vector 계약을 약화하지 않는 별도 matrix range client를 등록합니다.
     *
     * @param properties Prometheus 접속·타임아웃 설정
     * @return 최대 1시간·1,000점 기본 상한을 가진 range client
     */
    @Bean
    public PromRangeQueryClient promRangeQueryClient(PrometheusQueryProperties properties) {
        return new PromRangeQueryClient(prometheusRestClient(properties));
    }

    /**
     * Prometheus 결과를 Core의 기술 중립 Overview 입력으로 바꾸는 원천을 등록합니다.
     *
     * @param instantQuery Overview snapshot 평가 시각을 명시하는 instant-vector 경계
     * @param rangeQuery 별도 matrix range 경계
     * @param properties stale·전체 시작 예산 설정
     * @return Core가 의존할 기술 중립 관측 원천
     */
    @Bean
    public OverviewObservationSource promOverviewObservationSource(
            PromTimeQuery instantQuery,
            PromRangeQuery rangeQuery,
            PrometheusQueryProperties properties
    ) {
        return new PromOverviewObservationSource(
                instantQuery, rangeQuery, properties.staleAfter(), properties.totalBudget());
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
    public PromMetricsAssembler promMetricsAssembler(
            PromQuery client, TimeProvider timeProvider, PrometheusQueryProperties properties) {
        return new PromMetricsAssembler(
                client, timeProvider, properties.staleAfter(), properties.totalBudget());
    }
}
