package com.kafkick.infra.redis.coupon.v2;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.data.redis.core.StringRedisTemplate;

import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;

import io.micrometer.core.instrument.MeterRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;

import org.springframework.dao.DataAccessException;

import com.kafkick.infra.redis.observation.RedisLatencyAutoConfiguration;

import com.kafkick.core.admin.preparation.V2AdminPreparationReader;
import com.kafkick.core.admin.stock.V2AdminStockReader;
import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.core.coupon.v2.port.IssuanceWarmupPort;
import com.kafkick.core.coupon.v2.port.RestorationHaltStore;
import com.kafkick.core.coupon.v2.port.V2RestorationMeters;

/**
 * v2 게이트 조립. 스크립트 5종은 {@link IssuanceScripts} 의 상수라 빈이 아니고,
 * 빈이 되는 것은 <b>그것들을 부르는 통로</b>뿐이다.
 *
 * <p>{@code StringRedisTemplate} 이 없는 배포(V1 전용)에서는 게이트도 없다 —
 * 없는 통로를 빈으로 세워 두면 첫 발급 요청에서야 그 사실이 드러난다.
 */
@AutoConfiguration(
        after = DataRedisAutoConfiguration.class,
        // 계측 빈은 MeterRegistry 정의가 선 뒤에 해석돼야 한다. 순서를 안 걸면 레지스트리를
        // 못 본 채 NONE 으로 떨어져 카운터 시계열이 예외 없이 사라진다 —
        // RedisLatencyAutoConfiguration 이 같은 함정을 밟고 남긴 상수를 그대로 쓴다.
        afterName = {
                RedisLatencyAutoConfiguration.METRICS_AUTO_CONFIGURATION,
                RedisLatencyAutoConfiguration.COMPOSITE_METER_REGISTRY_AUTO_CONFIGURATION
        })
@ConditionalOnBean(StringRedisTemplate.class)
@EnableConfigurationProperties(RedisCircuitBreakerProperties.class)
public class IssuanceGateRedisAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(IssuanceScriptRunner.class)
    IssuanceScriptRunner issuanceScriptRunner(StringRedisTemplate redisTemplate) {
        return new IssuanceScriptRunner(redisTemplate);
    }

    @Bean
    @ConditionalOnMissingBean(IssuanceGatePort.class)
    IssuanceGatePort issuanceGatePort(
            IssuanceScriptRunner scriptRunner,
            StringRedisTemplate redisTemplate,
            @Qualifier("redisIssuanceCircuitBreaker") CircuitBreaker redisIssuanceCircuitBreaker) {
        return new Resilience4jIssuanceGate(
                new RedisIssuanceGate(scriptRunner, redisTemplate), redisIssuanceCircuitBreaker);
    }

    @Bean("redisIssuanceCircuitBreakerRegistry")
    @ConditionalOnMissingBean(name = "redisIssuanceCircuitBreakerRegistry")
    CircuitBreakerRegistry redisIssuanceCircuitBreakerRegistry(RedisCircuitBreakerProperties properties) {
        // Retry는 붙이지 않는다. timeout 뒤 Lua가 선점을 끝냈을 수 있어, 새 requestToken으로
        // 다시 보내면 완료·보상 CAS가 앞선 PENDING과 갈린다.
        //
        // **아래 조합은 기동을 거절한다.** 표본 창보다 최소 호출 수가 크면 차단기가 영영
        // 열리지 않는데(표본이 임계에 못 미친다), 값 하나만 보면 둘 다 정상이라
        // RedisCircuitBreakerProperties 의 setter 가 잡을 수 없다. 조용히 통과시키면
        // "차단기를 걸었는데 안 열린다"가 장애 중에 드러난다.
        if (properties.getMinimumNumberOfCalls() > properties.getSlidingWindowSize()) {
            throw new IllegalStateException(
                    "redisCB minimum-number-of-calls는 sliding-window-size보다 클 수 없습니다.");
        }
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(properties.getSlidingWindowSize())
                .minimumNumberOfCalls(properties.getMinimumNumberOfCalls())
                .failureRateThreshold(properties.getFailureRateThreshold())
                .waitDurationInOpenState(properties.getWaitDurationInOpenState())
                .permittedNumberOfCallsInHalfOpenState(1)
                .recordException(failure -> failure instanceof DataAccessException)
                .build();
        return CircuitBreakerRegistry.of(config);
    }

    @Bean
    @ConditionalOnMissingBean(name = "redisIssuanceCircuitBreaker")
    CircuitBreaker redisIssuanceCircuitBreaker(
            @Qualifier("redisIssuanceCircuitBreakerRegistry")
            CircuitBreakerRegistry redisIssuanceCircuitBreakerRegistry
    ) {
        return redisIssuanceCircuitBreakerRegistry.circuitBreaker("redisCB");
    }

    @Bean
    @ConditionalOnMissingBean(TaggedCircuitBreakerMetrics.class)
    @ConditionalOnBean(MeterRegistry.class)
    TaggedCircuitBreakerMetrics redisCircuitBreakerMetrics(
            @Qualifier("redisIssuanceCircuitBreakerRegistry")
            CircuitBreakerRegistry redisIssuanceCircuitBreakerRegistry, MeterRegistry meterRegistry
    ) {
        TaggedCircuitBreakerMetrics metrics = TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(
                redisIssuanceCircuitBreakerRegistry);
        metrics.bindTo(meterRegistry);
        return metrics;
    }

    /** 발급 게이트와 같은 Redis 연결에서 관리자용 V2 재고 정본 조회 포트를 제공합니다. */
    @Bean
    @ConditionalOnMissingBean(V2AdminStockReader.class)
    V2AdminStockReader v2AdminStockReader(StringRedisTemplate redisTemplate) {
        return new RedisV2AdminStockReader(redisTemplate);
    }

    /** 발급 게이트와 같은 Redis 연결에서 V2 예약 회차의 관리자 준비 상태를 제공합니다. */
    @Bean
    @ConditionalOnMissingBean(V2AdminPreparationReader.class)
    V2AdminPreparationReader v2AdminPreparationReader(StringRedisTemplate redisTemplate) {
        return new RedisV2AdminPreparationReader(redisTemplate);
    }

    /**
     * 복원 중단 표식. <b>게이트와 같은 조립에 둔다</b> — 취소(api)와 만료(batch)가 같은 표식을
     * 봐야 "그 회차 중단" 이 프로세스 경계를 넘는다.
     */
    @Bean
    @ConditionalOnMissingBean(RestorationHaltStore.class)
    RestorationHaltStore restorationHaltStore(StringRedisTemplate redisTemplate) {
        return new RedisRestorationHaltStore(redisTemplate);
    }

    /**
     * 복원 결과 카운터. <b>{@code @ConditionalOnBean} 을 쓰지 않는다</b> — 조건 평가 시점이
     * 자동설정 순서에 걸리고, 어긋나면 빈이 조용히 사라져 경보 룰이 평가조차 안 된다.
     * {@link ObjectProvider} 는 빈 생성 시점에 해석하므로 순서와 무관하다. 레지스트리가 여럿일
     * 때는 {@code @Primary}(합성 레지스트리)를 고른다 — 없으면 계측만 꺼진다.
     */
    @Bean
    @ConditionalOnMissingBean(V2RestorationMeters.class)
    V2RestorationMeters v2RestorationMeters(ObjectProvider<MeterRegistry> meterRegistry) {
        MeterRegistry registry = meterRegistry.getIfUnique();
        if (registry == null) {
            // 레지스트리가 없을 수도, @Primary 없이 여럿일 수도 있다. 둘 다 계측만 꺼지는데,
            // 조용히 꺼지면 경보 룰이 평가조차 안 되는 것을 아무도 모른다.
            LoggerFactory.getLogger(IssuanceGateRedisAutoConfiguration.class).warn(
                    "MeterRegistry 를 하나로 특정하지 못해 v2 복원 카운터를 끕니다. "
                            + "coupon_v2_stock_restore_total 시계열이 나오지 않습니다.");
            return V2RestorationMeters.NONE;
        }
        return new MicrometerV2RestorationMeters(registry);
    }

    /**
     * 워밍업 시딩. 게이트와 <b>다른 빈</b>인 이유는 수명이 달라서다 — 이쪽은 회차당 한 번,
     * 게이트가 닫힌 창에서만 돈다(설계 §6.2). 한 빈으로 묶으면 발급 유스케이스가 재고를
     * 통째로 쓰는 연산을 주입받게 된다.
     */
    @Bean
    @ConditionalOnMissingBean(IssuanceWarmupPort.class)
    IssuanceWarmupPort issuanceWarmupPort(
            StringRedisTemplate redisTemplate, RestorationHaltStore haltStore) {
        return new RedisIssuanceWarmup(redisTemplate, haltStore);
    }
}
