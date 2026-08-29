package com.kafkick.infra.redis.runtimeconfig;

import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.runtimeconfig.RuntimeConfigSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.util.Objects;

final class RuntimeConfigBootstrap implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(RuntimeConfigBootstrap.class);
    private static final String BOOTSTRAP_ACTOR = "system:bootstrap";

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final Clock clock;
    private final RuntimeConfigBootstrapProperties properties;

    RuntimeConfigBootstrap(
            StringRedisTemplate redis,
            ObjectMapper objectMapper,
            Clock clock,
            RuntimeConfigBootstrapProperties properties
    ) {
        this.redis = Objects.requireNonNull(redis, "redis");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.properties = Objects.requireNonNull(properties, "properties");
    }

    @Override
    public void run(ApplicationArguments args) {
        RuntimeConfigSnapshot initial = new RuntimeConfigSnapshot(
                properties.getEngineVersion(), properties.getReleaseStage(), properties.getQueueMode(),
                0, clock.instant(), BOOTSTRAP_ACTOR, SourceStatus.VALID);
        try {
            String json = objectMapper.writeValueAsString(initial);
            if (Boolean.TRUE.equals(redis.opsForValue().setIfAbsent(RedisRuntimeConfigStore.CONFIG_KEY, json))) {
                log.warn("config:runtime 이 없어 기본값으로 부트스트랩했다. revision=0");
            }
        } catch (DataAccessException exception) {
            log.error("config:runtime 부트스트랩에 실패했다. 앱은 계속 기동한다.", exception);
        } catch (JacksonException exception) {
            log.error("config:runtime 기본값을 직렬화하지 못했다. 앱은 계속 기동한다.", exception);
        }
    }
}
