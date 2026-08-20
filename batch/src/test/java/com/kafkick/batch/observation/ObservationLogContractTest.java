package com.kafkick.batch.observation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.sql.ResultSet;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.data.redis.core.StringRedisTemplate;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import com.kafkick.core.consistency.ConsistencySeverityPolicy;
import com.kafkick.core.consistency.DefaultConsistencyCalculator;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.support.TimeProvider;

/** 로그가 지켜야 할 두 가지 — 경보 임계치는 설정에서 오고, Redis 값 원문은 남기지 않는다. */
class ObservationLogContractTest {

    private final JdbcTemplate observationJdbcTemplate = mock(JdbcTemplate.class);
    private final StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
    private final TimeProvider timeProvider =
        new TimeProvider(Clock.fixed(Instant.parse("2026-08-20T12:00:00Z"), ZoneOffset.UTC));

    private ListAppender<ILoggingEvent> appender;
    private Logger registrarLogger;
    private Logger readerLogger;

    @BeforeEach
    void attachAppender() {
        appender = new ListAppender<>();
        appender.start();
        registrarLogger = (Logger) LoggerFactory.getLogger(DomainGaugeRegistrar.class);
        readerLogger = (Logger) LoggerFactory.getLogger(ConsistencyRawValueReader.class);
        registrarLogger.addAppender(appender);
        readerLogger.addAppender(appender);
    }

    @AfterEach
    void detachAppender() {
        registrarLogger.detachAppender(appender);
        readerLogger.detachAppender(appender);
    }

    @Test
    @DisplayName("연속 실패 경보 임계치는 설정에서 온다 — 코드에 박으면 부하 중에 못 바꾼다")
    void alarmThresholdComesFromConfiguration() {
        given(observationJdbcTemplate.query(anyString(), any(RowMapper.class), any(Object[].class)))
            .willThrow(new QueryTimeoutException("관측 쿼리가 잘렸다"));
        DomainGaugeProperties properties =
            new DomainGaugeProperties(EngineVersion.V1, 7L, 1, null, null, null, null);
        DomainGaugeRegistrar registrar = new DomainGaugeRegistrar(
            new ConsistencyRawValueReader(observationJdbcTemplate, null, properties, timeProvider),
            new DefaultConsistencyCalculator(ConsistencySeverityPolicy.defaults()),
            properties, new SimpleMeterRegistry(), timeProvider);

        registrar.refreshConsistency();

        assertThat(appender.list)
            .as("임계치 1 이면 첫 실패부터 ERROR 여야 한다")
            .anyMatch(event -> event.getLevel() == Level.ERROR);
    }

    @Test
    @DisplayName("Redis 값 원문을 로그에 남기지 않는다 — 키가 설정 가능이라 무엇이 담길지 모른다")
    void redisValueIsNotLogged() throws Exception {
        givenCoupon();
        given(redisTemplate.execute(any(RedisScript.class), anyList()))
            .willReturn(List.of("SECRET-VALUE-9f2a", "40", 40L));
        DomainGaugeProperties properties =
            new DomainGaugeProperties(EngineVersion.V3, 7L, null, null, null, null, null);

        new ConsistencyRawValueReader(observationJdbcTemplate, redisTemplate, properties, timeProvider)
            .read();

        // 부재 단정만 두면 로그 경로가 통째로 사라져도 통과한다. 남는다는 것부터 고정한다.
        assertThat(appender.list)
            .as("숫자가 아니었다는 사실은 로그로 남아야 한다")
            .anyMatch(event -> event.getFormattedMessage().contains("숫자가 아니다"));
        assertThat(appender.list)
            .as("진단에 필요한 것은 '숫자가 아니었다' 이지 그 내용이 아니다")
            .noneMatch(event -> event.getFormattedMessage().contains("SECRET-VALUE-9f2a"));
    }

    @SuppressWarnings("unchecked")
    private void givenCoupon() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        given(resultSet.getLong(anyString())).willReturn(10L);
        given(resultSet.getObject(anyString(), eq(Long.class))).willReturn(10L);
        given(observationJdbcTemplate.query(anyString(), any(RowMapper.class), any(Object[].class)))
            .willAnswer(invocation -> {
                RowMapper<Object> rowMapper = invocation.getArgument(1);
                return List.of(rowMapper.mapRow(resultSet, 0));
            });
    }
}
