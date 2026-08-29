package com.kafkick.infra.redis.runtimeconfig;

import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.runtimeconfig.RuntimeConfigSnapshot;
import org.junit.jupiter.api.Test;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.junit.jupiter.api.extension.ExtendWith;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(OutputCaptureExtension.class)
class RuntimeConfigBootstrapTest {

    private static final Instant NOW = Instant.parse("2026-08-29T00:00:00Z");
    private final ObjectMapper objectMapper = new RuntimeConfigJsonMapper().objectMapper();

    @Test
    void emptyRedisIsSeededOnceWithConfiguredDefaultsAndWarns(CapturedOutput output) throws Exception {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        @SuppressWarnings("unchecked")
        ValueOperations<String, String> values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        when(values.setIfAbsent(eq("config:runtime"), org.mockito.ArgumentMatchers.anyString())).thenReturn(true);
        RuntimeConfigBootstrapProperties properties = properties(EngineVersion.V2, ReleaseStage.V2_1, QueueMode.ALWAYS);

        bootstrap(redis, properties).run(new DefaultApplicationArguments());

        var json = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(values).setIfAbsent(eq("config:runtime"), json.capture());
        assertThat(objectMapper.readValue(json.getValue(), RuntimeConfigSnapshot.class)).isEqualTo(
                new RuntimeConfigSnapshot(EngineVersion.V2, ReleaseStage.V2_1, QueueMode.ALWAYS,
                        0, NOW, "system:bootstrap", SourceStatus.VALID));
        assertThat(output).contains("WARN", "config:runtime 이 없어 기본값으로 부트스트랩했다. revision=0");
    }

    @Test
    void existingValueIsNotOverwrittenAndDoesNotWarn(CapturedOutput output) throws Exception {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        @SuppressWarnings("unchecked")
        ValueOperations<String, String> values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        when(values.setIfAbsent(eq("config:runtime"), org.mockito.ArgumentMatchers.anyString())).thenReturn(false);

        bootstrap(redis, properties(EngineVersion.V1, ReleaseStage.V1, QueueMode.OFF))
                .run(new DefaultApplicationArguments());

        assertThat(output).doesNotContain("config:runtime 이 없어 기본값으로 부트스트랩했다");
    }

    @Test
    void unavailableRedisDoesNotPreventStartup() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        when(redis.opsForValue()).thenThrow(new DataAccessResourceFailureException("down"));

        assertDoesNotThrow(() -> bootstrap(redis, properties(EngineVersion.V1, ReleaseStage.V1, QueueMode.OFF))
                .run(new DefaultApplicationArguments()));
    }

    private RuntimeConfigBootstrap bootstrap(StringRedisTemplate redis, RuntimeConfigBootstrapProperties properties) {
        return new RuntimeConfigBootstrap(redis, objectMapper, Clock.fixed(NOW, ZoneOffset.UTC), properties);
    }

    private static RuntimeConfigBootstrapProperties properties(
            EngineVersion engineVersion, ReleaseStage releaseStage, QueueMode queueMode
    ) {
        RuntimeConfigBootstrapProperties properties = new RuntimeConfigBootstrapProperties();
        properties.setEngineVersion(engineVersion);
        properties.setReleaseStage(releaseStage);
        properties.setQueueMode(queueMode);
        return properties;
    }
}
