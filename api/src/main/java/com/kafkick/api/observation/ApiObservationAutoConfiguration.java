package com.kafkick.api.observation;

import com.kafkick.api.observation.issuance.IssuanceObservationService;
import com.kafkick.core.consistency.ConsistencyCalculator;
import com.kafkick.core.consistency.ConsistencySeverityPolicy;
import com.kafkick.core.consistency.DefaultConsistencyCalculator;
import com.kafkick.core.observation.EventIdGenerator;
import com.kafkick.core.observation.EventRecorder;
import com.kafkick.core.observation.IssuanceFlowEventFactory;
import com.kafkick.core.support.TimeProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;

import java.time.Clock;

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

    /**
     * 애플리케이션 공통 시계를 사용하는 기본 시간 공급자를 등록합니다.
     *
     * <p>사용자가 {@link TimeProvider}를 직접 등록하면 이 Bean은 생성되지 않습니다. 별도 시간
     * 공급자는 없지만 {@link Clock} Bean이 있으면 해당 시계를 사용해 테스트와 애플리케이션의
     * 시각 기준을 유지합니다. 둘 다 없을 때만 시스템 UTC 시계를 최후 기본값으로 사용합니다.
     *
     * @param clockProvider 애플리케이션이 제공하는 공통 시계 조회 경계
     * @return 발급 관측 결과 시각을 제공할 시간 공급자
     */
    @Bean
    @ConditionalOnMissingBean(TimeProvider.class)
    public TimeProvider observationTimeProvider(ObjectProvider<Clock> clockProvider) {
        Clock clock = clockProvider.getIfAvailable(() -> {
            log.warn("Clock 빈이 없어 시스템 UTC 시계를 사용합니다. 시각 고정 테스트가 무력화됩니다.");
            return Clock.systemUTC();
        });
        return new TimeProvider(clock);
    }

    /**
     * 요청 단위 발급 관측 Session을 생성하는 서비스를 등록합니다.
     *
     * <p>실제 {@link EventRecorder} 구현이 등록되면 조건부 기본 NoOp 대신 해당 구현을 사용합니다.
     *
     * @param eventFactory 발급 관측 이벤트 생성기
     * @param eventRecorder 완성된 이벤트를 전달할 기록 포트
     * @param timeProvider 결과 발생 시각을 제공하는 시간 공급자
     * @return 발급 관측 Session 생성 서비스
     */
    @Bean
    @ConditionalOnMissingBean(IssuanceObservationService.class)
    public IssuanceObservationService issuanceObservationService(
            IssuanceFlowEventFactory eventFactory,
            EventRecorder eventRecorder,
            TimeProvider timeProvider
    ) {
        return new IssuanceObservationService(eventFactory, eventRecorder, timeProvider);
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
