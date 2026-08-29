package com.kafkick.api.coupon.query;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.github.benmanes.caffeine.cache.Ticker;
import com.kafkick.core.coupon.v2.query.CouponDefinition;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

@Configuration
@EnableConfigurationProperties({
        CouponDefinitionL1CacheProperties.class,
        CouponDefinitionL2CacheProperties.class
})
public class CouponDefinitionL1CacheConfiguration {

    private static final Logger log =
            LoggerFactory.getLogger(CouponDefinitionL1CacheConfiguration.class);

    @Bean(destroyMethod = "shutdown")
    public ExecutorService couponDefinitionCacheLoaderExecutor() {
        // 메인 정의 캐시는 ALL 한 키다. Caffeine single-flight가 같은 miss를 합치므로 로더도
        // 하나면 충분하고, DB 지연 때 발급용 Hikari 연결을 더 잠식하지 않는다.
        return Executors.newSingleThreadExecutor(
                runnable -> {
                    Thread thread = new Thread(runnable, "coupon-definition-cache-loader");
                    thread.setDaemon(true);
                    return thread;
                });
    }

    /**
     * {@code Clock} 은 {@code ObjectProvider} 로 받지 않는다. 빈이 없을 때 조용히 시스템 시계로
     * 넘어가면, 시계를 고정한 회차에서 캐시만 실제 시각을 보는 상태가 예외도 로그도 없이 성립한다.
     * 없으면 기동에서 죽는 편이 낫다.
     */
    @Bean
    public CouponDefinitionL1Cache<List<CouponDefinition>> couponDefinitionL1Cache(
            Clock clock,
            CouponDefinitionL1CacheProperties properties,
            CouponDefinitionL2CacheProperties l2Properties,
            @Qualifier("couponDefinitionCacheLoaderExecutor") Executor loaderExecutor,
            ObjectProvider<MeterRegistry> meterRegistries) {
        requireCoherentBudgets(properties, l2Properties);
        MeterRegistry meterRegistry = meterRegistries.getIfAvailable(() -> {
            log.warn("MeterRegistry 가 없다 — L1 히트율과 완화 응답 수가 노출되지 않는다");
            return new SimpleMeterRegistry();
        });
        CouponDefinitionL1Cache<List<CouponDefinition>> cache = new CouponDefinitionL1Cache<>(
                clock, Ticker.systemTicker(), properties, loaderExecutor, meterRegistry);
        CaffeineCacheMetrics.monitor(meterRegistry, cache.freshStatsView(), "coupon.definition.l1");
        return cache;
    }

    /**
     * L2 대기 예산이 L1 호출자 예산보다 짧아야 로드 권한이 herd 를 실제로 막는다. 어긋난 값은
     * 기동에서 죽인다 — 살려 두면 부하 시험 뒤 DB 질의 수를 셀 때에야 드러난다.
     */
    private static void requireCoherentBudgets(
            CouponDefinitionL1CacheProperties l1, CouponDefinitionL2CacheProperties l2) {
        if (l2.waitTimeout().compareTo(l1.loadTimeout()) >= 0) {
            throw new IllegalStateException(
                    "쿠폰 정의 캐시 설정이 어긋났습니다: l2.wait-timeout(" + l2.waitTimeout()
                            + ") 은 l1.load-timeout(" + l1.loadTimeout() + ") 보다 짧아야 합니다.");
        }
    }
}
