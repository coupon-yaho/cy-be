package com.kafkick.api.admin.observability;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import com.kafkick.core.support.TimeProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
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
@EnableConfigurationProperties({PrometheusQueryProperties.class, PrometheusArchiveProperties.class})
public class AdminObservabilityConfig {

    @Bean
    public PromQueryClient promQueryClient(PrometheusQueryProperties properties) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(properties.connectTimeout());
        requestFactory.setReadTimeout(properties.readTimeout());
        return new PromQueryClient(RestClient.builder()
                .baseUrl(properties.baseUrl())
                .requestFactory(requestFactory)
                .build());
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

    @Bean
    public PromMetricsAssembler promMetricsAssembler(
            @Qualifier("promQueryClient") PromQuery client,
            TimeProvider timeProvider, PrometheusQueryProperties properties) {
        return new PromMetricsAssembler(
                client, timeProvider, properties.staleAfter(), properties.totalBudget());
    }

    @Bean
    @ConditionalOnBean({BenchmarkRunRepository.class, ArchiveStore.class})
    public RunTimeseriesArchiver runTimeseriesArchiver(
            BenchmarkRunRepository runs,
            @Qualifier("archivePromQueryClient") PromQueryClient source,
            ArchiveStore store) {
        return new RunTimeseriesArchiver(runs, source, store);
    }
}
