package com.kafkick.batch.observation;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import io.micrometer.core.instrument.MeterRegistry;

import com.kafkick.core.support.TimeProvider;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = {
    "observation.datasource.enabled",
    "observation.pending-issued-gauge.enabled"
}, havingValue = "true")
@EnableConfigurationProperties(PendingIssuedGaugeProperties.class)
public class PendingIssuedGaugeConfig {

    @Bean
    public PendingIssuedGaugeCollector pendingIssuedGaugeCollector(
        @Qualifier("obs") JdbcTemplate observationJdbcTemplate,
        ObjectProvider<StringRedisTemplate> redisTemplate,
        PendingIssuedGaugeProperties properties,
        MeterRegistry meterRegistry,
        TimeProvider timeProvider
    ) {
        return new PendingIssuedGaugeCollector(
            observationJdbcTemplate, redisTemplate.getIfAvailable(), properties, meterRegistry, timeProvider);
    }

    @Bean
    public PendingIssuedGaugeScheduler pendingIssuedGaugeScheduler(
        PendingIssuedGaugeCollector collector,
        PendingIssuedGaugeProperties properties
    ) {
        return new PendingIssuedGaugeScheduler(collector, properties);
    }
}
