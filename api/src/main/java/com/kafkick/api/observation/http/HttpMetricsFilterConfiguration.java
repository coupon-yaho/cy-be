package com.kafkick.api.observation.http;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class HttpMetricsFilterConfiguration {

    @Bean
    public HttpMetricsFilter httpMetricsFilter(
            ResultClassifier resultClassifier,
            HttpMetrics httpMetrics,
            InFlightRegistry inFlightRegistry
    ) {
        return new HttpMetricsFilter(resultClassifier, httpMetrics, inFlightRegistry);
    }
}
