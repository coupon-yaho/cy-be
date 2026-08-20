package com.kafkick.batch.observation;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import io.micrometer.core.instrument.MeterRegistry;

import com.kafkick.core.consistency.ConsistencyCalculator;
import com.kafkick.core.consistency.DefaultConsistencyCalculator;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.support.TimeProvider;

/**
 * 도메인 Gauge 배선. {@code observation.domain-gauge.enabled} 로 옵트인한다.
 *
 * <p>기본값을 켜 두지 않는 이유 — 이 배선은 관측 전용 DataSource 를 전제한다. 그 풀을 켜지 않은
 * JVM 에서 자동으로 켜지면 {@code @Qualifier("obs")} 대상을 못 찾아 기동에서 죽는다.
 * 관측 설정이 관측과 무관한 프로세스를 멈춰 세우는 셈이라 관측 전용 풀을 나눈 이유와 어긋난다.
 *
 * <p><b>두 스위치를 모두 요구한다.</b> 도메인 Gauge 만 켜고 관측 풀을 끄면 기동에서 죽는데,
 * 그 조합은 환경변수로 얼마든지 만들어진다({@code DOMAIN_GAUGE_ENABLED=true} 만 넘기는 배포).
 * 관측이 검증 배치와 verify 트리거까지 멈춰 세우는 건 OBSERVATION_DOWN 을 따로 둔 취지와 정반대라,
 * 짝이 안 맞으면 지표를 포기하고 프로세스는 살린다.
 *
 * <p>⚠️ 반대 방향 실패 — 관측 풀 스위치를 끄면 도메인 Gauge 가 <b>조용히</b> 사라진다. 기동도
 * 로그도 정상인데 {@code app_consistency_gap} 이 없다. 그 상황은 Prometheus 쪽에서
 * {@code absent()} 로 잡아야 한다.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = {
        "observation.datasource.enabled",
        "observation.domain-gauge.enabled"
}, havingValue = "true")
@EnableConfigurationProperties({ DomainGaugeProperties.class, ConsistencySeverityProperties.class })
public class DomainObservationConfig {

    /**
     * 임계치는 api 와 같은 키에서 읽는다. 여기에 숫자를 박으면 운영자가 api 쪽만 조정하고
     * batch 는 기본값으로 도는데, 두 JVM 이 같은 회차를 다른 기준으로 판정하게 된다.
     */
    /*
     * TODO(batch 자동설정 도입 담당): 이 조건은 일반 @Configuration 에 있어 빈 등록 순서에 의존한다.
     *   batch 에 다른 ConsistencyCalculator 빈이 없어 지금은 갈릴 상황이 없지만, 자동설정이 생기거나
     *   실제로 계산기를 재정의하는 코드가 들어오면 그 자동설정으로 옮긴다. 지금 옮기면 관측 배선
     *   하나를 위해 기동 메커니즘을 하나 더 늘리게 된다.
     */
    @Bean
    @ConditionalOnMissingBean(ConsistencyCalculator.class)
    public ConsistencyCalculator consistencyCalculator(ConsistencySeverityProperties severityProperties) {
        return new DefaultConsistencyCalculator(severityProperties.toPolicy());
    }

    /**
     * V1 은 Redis 통로를 넘기지 않는다. V2·V3 에서 통로가 없으면(자동설정을 뺀 배포) 값 대신
     * UNAVAILABLE 이 나간다.
     */
    @Bean
    public ConsistencyRawValueReader consistencyRawValueReader(
        @Qualifier("obs") JdbcTemplate observationJdbcTemplate,
        ObjectProvider<StringRedisTemplate> redisTemplate,
        DomainGaugeProperties properties,
        TimeProvider timeProvider
    ) {
        // V1 에는 Redis 가 없다. 스타터가 클래스패스에 있으면 자동설정이 StringRedisTemplate 을
        // 무조건 등록하므로 빈은 존재한다 — 통로를 안 쓴다는 사실을 여기서 명시적으로 끊는다.
        StringRedisTemplate template = properties.engineVersion() == EngineVersion.V1
            ? null
            : redisTemplate.getIfAvailable();
        return new ConsistencyRawValueReader(
            observationJdbcTemplate, template, properties, timeProvider);
    }

    @Bean
    public DomainGaugeRegistrar domainGaugeRegistrar(
        ConsistencyRawValueReader reader,
        ConsistencyCalculator calculator,
        DomainGaugeProperties properties,
        MeterRegistry meterRegistry,
        TimeProvider timeProvider
    ) {
        return new DomainGaugeRegistrar(reader, calculator, properties, meterRegistry, timeProvider);
    }
}
