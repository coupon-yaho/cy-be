package com.kafkick.api.observation;

import com.kafkick.core.consistency.ConsistencyCalculator;
import com.kafkick.core.consistency.ConsistencySeverityPolicy;
import com.kafkick.core.consistency.DefaultConsistencyCalculator;
import com.kafkick.core.observation.EventIdGenerator;
import com.kafkick.core.observation.EventRecorder;
import com.kafkick.core.observation.IssuanceFlowEventFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@EnableConfigurationProperties(ConsistencySeverityProperties.class)
public class ApiObservationAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(ApiObservationAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean(EventIdGenerator.class)
    public EventIdGenerator eventIdGenerator() {
        return new UuidEventIdGenerator();
    }

    @Bean
    @ConditionalOnMissingBean(IssuanceFlowEventFactory.class)
    public IssuanceFlowEventFactory issuanceFlowEventFactory(EventIdGenerator eventIdGenerator) {
        return new IssuanceFlowEventFactory(eventIdGenerator);
    }

    @Bean
    @ConditionalOnMissingBean(EventRecorder.class)
    public EventRecorder eventRecorder() {
        log.warn("EventRecorder 실구현이 없어 no-op을 사용합니다.");
        return new NoOpEventRecorder();
    }

    @Bean
    @ConditionalOnMissingBean(ConsistencySeverityPolicy.class)
    public ConsistencySeverityPolicy consistencySeverityPolicy(
            ConsistencySeverityProperties properties
    ) {
        return properties.toPolicy();
    }

    @Bean
    @ConditionalOnMissingBean(ConsistencyCalculator.class)
    public ConsistencyCalculator consistencyCalculator(ConsistencySeverityPolicy severityPolicy) {
        return new DefaultConsistencyCalculator(severityPolicy);
    }
}
