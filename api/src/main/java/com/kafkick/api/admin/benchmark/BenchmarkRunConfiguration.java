package com.kafkick.api.admin.benchmark;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import com.kafkick.core.benchmark.BenchmarkRunRepository;
import com.kafkick.core.benchmark.BenchmarkRunService;
import com.kafkick.core.support.TimeProvider;

/** 회차 API를 노출하는 api 모듈에서만 core 서비스를 빈으로 올린다. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "observation.datasource.enabled", havingValue = "true")
public class BenchmarkRunConfiguration {

    @Bean
    public BenchmarkRunService benchmarkRunService(
        BenchmarkRunRepository repository,
        TimeProvider timeProvider
    ) {
        return new BenchmarkRunService(repository, timeProvider);
    }
}
