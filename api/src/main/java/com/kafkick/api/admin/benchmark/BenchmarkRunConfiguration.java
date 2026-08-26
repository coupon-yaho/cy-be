package com.kafkick.api.admin.benchmark;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import com.kafkick.core.benchmark.BenchmarkRunRepository;
import com.kafkick.core.benchmark.BenchmarkRunService;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.consistency.ConsistencyFinalStore;
import org.springframework.beans.factory.annotation.Value;
import java.time.Duration;
import org.springframework.beans.factory.ObjectProvider;

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

    @Bean
    public ConsistencyFinalizer consistencyFinalizer(
            BenchmarkRunService runs,
            ObjectProvider<ConsistencyFinalStore> store,
            ObjectProvider<BatchConsistencyFinalClient> batch,
            TimeProvider timeProvider,
            @Value("${benchmark.consistency.claim-lease:5m}") Duration claimLease,
            @Value("${benchmark.consistency.max-observation-lag:15m}") Duration maxObservationLag) {
        return new ConsistencyFinalizer(runs, store.getIfAvailable(), batch.getIfAvailable(),
                timeProvider, claimLease, maxObservationLag);
    }
}
