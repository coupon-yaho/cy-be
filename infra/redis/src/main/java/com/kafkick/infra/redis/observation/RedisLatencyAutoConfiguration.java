package com.kafkick.infra.redis.observation;

import io.lettuce.core.metrics.MicrometerCommandLatencyRecorder;
import io.lettuce.core.metrics.MicrometerOptions;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.data.redis.autoconfigure.ClientResourcesBuilderCustomizer;
import org.springframework.context.annotation.Bean;

/**
 * Redis 명령 레이턴시를 Lettuce 의 커맨드 레코더로 계측한다. 등록만 하고 읽지 않는다 —
 * 값은 {@code /actuator/prometheus} 가 레지스트리에서 직접 긁어 간다.
 *
 * <p>백분위와 expiry 는 여기서 정하지 않는다. 부하 중 재기동 없이 조여야 하고, 관측 창이
 * HTTP 미터와 같아야 회차 간 그래프를 겹쳐 읽을 수 있다. 소유는 설정 파일이다 —
 * batch 의 {@code management.yml} 의 {@code management.metrics.distribution} 블록.
 *
 * <p>클래스명 기반 fallback 정렬에 맡기면 {@code MeterRegistry} 빈 정의가 아직 없는 시점에
 * 아래 조건이 평가되어 계측이 예외 없이 사라진다. 이름으로 순서를 건다 — 이 모듈은
 * spring-boot-micrometer-metrics 를 컴파일 타임에 보지 않으므로 클래스 참조는 쓸 수 없다.
 */
@AutoConfiguration(
        afterName = {
                RedisLatencyAutoConfiguration.METRICS_AUTO_CONFIGURATION,
                RedisLatencyAutoConfiguration.COMPOSITE_METER_REGISTRY_AUTO_CONFIGURATION
        },
        // ClientResources 가 만들어지기 전에 커스터마이저가 등록돼 있어야 레코더가 붙는다.
        beforeName = "org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration")
// 클래스패스에 없으면 스스로 물러나는 것이 자동설정의 계약이다. 없으면 계측만 꺼져야지
// ClassNotFoundException 으로 기동이 죽으면 안 된다.
@ConditionalOnClass({ MeterRegistry.class, MicrometerCommandLatencyRecorder.class,
        ClientResourcesBuilderCustomizer.class })
public class RedisLatencyAutoConfiguration {

    /** 순서를 거는 대상. 읽는 쪽이 옮겨 적으면 한쪽만 바뀌어도 아무도 모른다. */
    public static final String METRICS_AUTO_CONFIGURATION =
            "org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration";
    public static final String COMPOSITE_METER_REGISTRY_AUTO_CONFIGURATION =
            "org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration";

    /**
     * Lettuce 가 붙이는 미터 이름의 공통 네임스페이스. 백분위·expiry 를 소유하는 쪽(batch 의
     * {@code management.yml})이 같은 문자열로 키를 만들어야 설정이 이 미터들에 걸린다.
     * 어긋나면 예외 없이 설정만 조용히 미적용된다 — 옮겨 적지 말고 이 상수를 참조할 것.
     */
    public static final String COMMAND_METER_NAMESPACE = "lettuce.command";

    private static final String LETTUCE_COMMAND_METER_PREFIX = COMMAND_METER_NAMESPACE + ".";

    /**
     * Lettuce 가 붙이는 {@code local}(ephemeral 포트) · {@code remote}(Redis host:port) 두 태그를
     * 지운다. 시계열이 접속 조합만큼 갈리고, scrape 응답에 Redis 토폴로지가 실린다.
     *
     * <p>남길 것을 고르지 않고 지울 것을 고른다. allowlist 로 짜면 나중에 누군가
     * {@code commonTags("instance", ...)} 를 걸었을 때 다른 미터에는 붙고 Redis 미터에서만
     * 조용히 빠져, 인스턴스를 2대로 늘리는 순간 어느 쪽이 느린지 못 가린다.
     */
    @Bean
    MeterFilter redisLatencyTags() {
        return new MeterFilter() {
            @Override
            public Meter.Id map(Meter.Id id) {
                if (!id.getName().startsWith(LETTUCE_COMMAND_METER_PREFIX)) {
                    return id;
                }
                return id.replaceTags(id.getTags().stream()
                        .filter(tag -> !tag.getKey().equals("local") && !tag.getKey().equals("remote"))
                        .toList());
            }
        };
    }

    @Bean
    @ConditionalOnBean(MeterRegistry.class)
    // ⚠️ 타입으로 걸면 안 된다. ClientResourcesBuilderCustomizer 는 Boot 가 전부 모아 순서대로
    //    적용하는 가산형 확장점이고, Boot 자신도 LettuceObservationAutoConfiguration 에서 같은
    //    타입 빈(lettuceObservation)을 낸다 — 트레이싱을 붙이는 순간 그쪽이 등록되면서 이 빈이
    //    물러나 계측이 예외도 로그도 없이 통째로 사라진다.
    @ConditionalOnMissingBean(name = "redisLatencyCustomizer")
    ClientResourcesBuilderCustomizer redisLatencyCustomizer(MeterRegistry registry) {
        return builder -> builder.commandLatencyRecorder(
                new MicrometerCommandLatencyRecorder(
                        registry,
                        MicrometerOptions.builder()
                                .histogram(false)
                                .build()));
    }
}
