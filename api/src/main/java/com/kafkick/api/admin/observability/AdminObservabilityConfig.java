package com.kafkick.api.admin.observability;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

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
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(properties.connectTimeout());
        requestFactory.setReadTimeout(properties.readTimeout());
        return new PromQueryClient(RestClient.builder()
                .baseUrl(properties.baseUrl())
                .requestFactory(requestFactory)
                .build());
    }

    @Bean
    public PromMetricsAssembler promMetricsAssembler(
            PromQueryClient client, TimeProvider timeProvider, PrometheusQueryProperties properties) {
        return new PromMetricsAssembler(client, timeProvider, properties.staleAfter());
    }
}
