package com.kafkick.infra.redis.stream;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.kafkick.core.observation.attempt.AttemptLiveReader;
import com.kafkick.core.observation.attempt.AttemptLiveSink;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import tools.jackson.databind.ObjectMapper;

/**
 * live 버퍼 빈. <b>{@code kafka.enabled} 에 걸지 않는다.</b>
 *
 * <p>쓰는 쪽(컨슈머)은 Kafka 스위치를 타지만 읽는 쪽(관제 API)은 아니다. 여기에 그 조건을 걸면
 * Kafka 를 끈 회차에서 조회 엔드포인트가 빈을 못 찾아 <b>기동에서 죽는다</b> — 그 회차의 올바른
 * 동작은 "화면이 비어 있다" 이지 "앱이 안 뜬다" 가 아니다.
 *
 * <p>자동설정 클래스인 이유는 {@code @ConditionalOnMissingBean} 이 <b>여기서만</b> 순서를
 * 보장받기 때문이다. 컴포넌트 스캔되는 {@code @Configuration} 에 붙이면 대상 빈 정의가 등록되기
 * 전에 평가돼 항상 참이 된다(AGENTS.md 의 함정 표 첫 줄).
 */
@AutoConfiguration(after = DataRedisAutoConfiguration.class)
public class AttemptLiveRedisAutoConfiguration {

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(AttemptLiveRedisAutoConfiguration.class);

    /**
     * 하나의 빈이 두 포트를 구현한다. 쓰기와 읽기가 <b>같은 키</b>를 봐야 하기 때문이다 —
     * 빈을 둘로 나누면 키 상수가 두 군데를 지나고, 한쪽만 바뀌어도 예외 없이 화면만 빈다.
     */
    @Bean
    @ConditionalOnMissingBean({AttemptLiveSink.class, AttemptLiveReader.class})
    AttemptLiveStream attemptLiveStream(
            StringRedisTemplate redisTemplate,
            ObjectMapper objectMapper,
            ObjectProvider<MeterRegistry> meterRegistries
    ) {
        return new AttemptLiveStream(redisTemplate, objectMapper,
                meterRegistries.getIfAvailable(() -> {
                    log.warn("MeterRegistry 가 없다 — live 버퍼의 판독 실패 수가 노출되지 않는다");
                    return new SimpleMeterRegistry();
                }));
    }
}
