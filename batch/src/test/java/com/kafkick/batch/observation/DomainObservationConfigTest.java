package com.kafkick.batch.observation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.support.TimeProvider;

/**
 * 배선 조건과 V1 의 Redis 차단을 본다. 둘 다 "설정 문자열" 이 아니라 만들어진 빈으로 확인한다.
 */
class DomainObservationConfigTest {

    private final JdbcTemplate observationJdbcTemplate = mock(JdbcTemplate.class);

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withBean("obs", JdbcTemplate.class, () -> observationJdbcTemplate)
        .withBean(TimeProvider.class, () -> new TimeProvider(Clock.systemUTC()))
        .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
        .withUserConfiguration(DomainObservationConfig.class);

    @Test
    @DisplayName("도메인 Gauge 만 켜고 관측 풀을 끄면 배선이 통째로 빠진다 — 기동은 살린다")
    void bothSwitchesAreRequired() {
        runner.withPropertyValues("observation.domain-gauge.enabled=true")
            .run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(context).doesNotHaveBean(DomainGaugeRegistrar.class);
            });
    }

    @Test
    @DisplayName("두 스위치가 다 켜져야 등록된다")
    void wiringNeedsBothSwitches() {
        runner.withPropertyValues(
                "observation.datasource.enabled=true",
                "observation.domain-gauge.enabled=true")
            .run(context -> assertThat(context).hasSingleBean(DomainGaugeRegistrar.class));
    }

    /**
     * 리더 안에도 {@code engineVersion == V1} 분기가 있어 행위만 보면 이 배선을 지워도 테스트가
     * 통과한다(실제로 그랬다). 그래서 "통로를 <b>요청조차 하지 않는다</b>" 는 배선 자체를 본다.
     */
    @SuppressWarnings("unchecked")
    private void givenCouponRow() throws Exception {
        java.sql.ResultSet resultSet = mock(java.sql.ResultSet.class);
        given(resultSet.getLong(anyString())).willReturn(7L);
        given(resultSet.getObject(anyString(), eq(Long.class))).willReturn(100L);
        given(observationJdbcTemplate.query(anyString(), any(RowMapper.class), any(Object[].class)))
            .willAnswer(invocation -> List.of(
                ((RowMapper<Object>) invocation.getArgument(1)).mapRow(resultSet, 0)));
    }

    @Test
    @DisplayName("V1 배선은 Redis 통로를 요청조차 하지 않는다")
    void v1WiringNeverAsksForTheRedisChannel() {
        AtomicBoolean asked = new AtomicBoolean();
        ObjectProvider<StringRedisTemplate> provider = new ObjectProvider<>() {
            @Override
            public StringRedisTemplate getObject() {
                return getIfAvailable();
            }

            @Override
            public StringRedisTemplate getObject(Object... args) {
                return getIfAvailable();
            }

            @Override
            public StringRedisTemplate getIfAvailable() {
                asked.set(true);
                return mock(StringRedisTemplate.class);
            }

            @Override
            public StringRedisTemplate getIfUnique() {
                return getIfAvailable();
            }
        };

        new DomainObservationConfig().consistencyRawValueReader(
            mock(JdbcTemplate.class),
            provider,
            new DomainGaugeProperties(EngineVersion.V1, null, null, null, null, null, null),
            new TimeProvider(Clock.systemUTC()));

        assertThat(asked).isFalse();
    }

    @Test
    @DisplayName("V1 은 StringRedisTemplate 이 컨텍스트에 있어도 쓰지 않는다")
    void v1NeverUsesRedisEvenWhenTheBeanExists() throws Exception {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        // ⚠️ 회차 행이 없으면 리더가 Redis 분기 전에 반환해, 엔진 버전과 무관하게 통과한다.
        givenCouponRow();

        runner.withBean(StringRedisTemplate.class, () -> redisTemplate)
            .withPropertyValues(
                "observation.datasource.enabled=true",
                "observation.domain-gauge.enabled=true",
                "observation.domain-gauge.engine-version=V1")
            .run(context -> {
                // 스프링이 빈 초기화 중에 Aware 콜백을 부른다. 우리가 부른 것만 남긴다.
                clearInvocations(redisTemplate);
                context.getBean(ConsistencyRawValueReader.class).readStock();
                // 스타터가 클래스패스에 있으면 자동설정이 이 빈을 무조건 만든다(실측). V1 을
                // 지키는 것은 빈의 부재가 아니라 배선에서 통로를 끊는 이 결정이다.
                verifyNoInteractions(redisTemplate);
            });
    }
}
