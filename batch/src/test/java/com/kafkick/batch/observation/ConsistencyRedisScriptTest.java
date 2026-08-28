package com.kafkick.batch.observation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.sql.ResultSet;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.kafkick.batch.observation.ConsistencyRawValueReader.DomainRawSnapshot;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;

/**
 * 정합성 Lua 스크립트는 <b>실제 Redis 에서만</b> 검증된다. 목으로 세우면 스크립트가 무엇을
 * 돌려주는지는 테스트가 정하는 값이 되어, 스크립트를 어떻게 고쳐도 통과한다.
 */
class ConsistencyRedisScriptTest {

    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7")).withExposedPorts(6379);

    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate redisTemplate;

    private final JdbcTemplate observationJdbcTemplate = mock(JdbcTemplate.class);

    @BeforeAll
    static void startRedis() {
        REDIS.start();
        factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getFirstMappedPort()));
        factory.afterPropertiesSet();
        factory.start();
        redisTemplate = new StringRedisTemplate(factory);
    }

    @AfterAll
    static void stopRedis() {
        factory.destroy();
        REDIS.stop();
    }

    @BeforeEach
    void resetKeys() throws Exception {
        redisTemplate.delete(List.of(
                "coupon:7:stock:remaining", "coupon:7:issued", "coupon:7:members"));
        givenCouponRow();
    }

    @Test
    @DisplayName("정상 값이면 VALID 다 — 문자열 카운터와 집합 크기를 함께 읽는다")
    void readsMixedTypesFromRealRedis() {
        redisTemplate.opsForValue().set("coupon:7:stock:remaining", "960");
        redisTemplate.opsForValue().set("coupon:7:issued", "40");
        redisTemplate.opsForSet().add("coupon:7:members", "m1", "m2", "m3");

        DomainRawSnapshot snapshot = reader().read();

        assertThat(snapshot.consistency().redisObservation().status()).isEqualTo(SourceStatus.VALID);
        assertThat(snapshot.consistency().rawValues().redisRemaining()).isEqualTo(960);
        assertThat(snapshot.consistency().rawValues().redisMemberEverCount()).isEqualTo(3);
    }

    @Test
    @DisplayName("키가 아직 없으면 예열이다")
    void missingKeysArePending() {
        assertThat(reader().read().consistency().redisObservation().status())
                .isEqualTo(SourceStatus.PENDING);
    }

    @Test
    @DisplayName("회원 집합 키가 예상 밖 자료형이면 예열이 아니라 고장이다")
    void unexpectedMemberKeyTypeIsAFailure() {
        redisTemplate.opsForValue().set("coupon:7:stock:remaining", "960");
        redisTemplate.opsForValue().set("coupon:7:issued", "40");
        redisTemplate.opsForHash().put("coupon:7:members", "field", "value");

        assertThat(reader().read().consistency().redisObservation().status())
                .as("키를 잘못 가리킨 설정을 예열로 두면 영영 안 나올 값을 기다리게 된다")
                .isEqualTo(SourceStatus.UNAVAILABLE);
    }

    private ConsistencyRawValueReader reader() {
        return new ConsistencyRawValueReader(
                observationJdbcTemplate, redisTemplate,
                new DomainGaugeProperties(EngineVersion.V3, 7L, null, null, null, null, null),
                new TimeProvider(Clock.systemUTC()));
    }

    @SuppressWarnings("unchecked")
    private void givenCouponRow() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        given(resultSet.getLong(anyString())).willReturn(7L);
        given(resultSet.getObject(anyString(), eq(Long.class))).willReturn(1000L);
        given(resultSet.getObject(anyString(), eq(LocalDateTime.class))).willReturn(null);
        given(observationJdbcTemplate.query(anyString(), any(RowMapper.class), any(Object[].class)))
                .willAnswer(invocation -> List.of(
                        ((RowMapper<Object>) invocation.getArgument(1)).mapRow(resultSet, 0)));
    }
}
