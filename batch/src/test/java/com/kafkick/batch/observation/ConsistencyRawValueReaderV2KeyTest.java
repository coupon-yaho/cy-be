package com.kafkick.batch.observation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.sql.ResultSet;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.kafkick.batch.observation.ConsistencyRawValueReader.DomainRawSnapshot;
import com.kafkick.core.consistency.ConsistencyRawValues;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.infra.redis.coupon.v2.IssuanceKeys;

/**
 * 정합성 리더가 <b>v2 의 실제 키 세 개</b>를 읽는지를 진짜 Redis 로 본다.
 *
 * <p>{@code cy:v2:issued} 는 Hash 다. 자료형 분기에 hash 가 없으면 크기를 못 재
 * {@code UNAVAILABLE} 로 떨어지고, {@code ConsistencyGapType} 은 V2 에 4축을 전부 적용하므로
 * <b>축 하나가 영구 UNAVAILABLE 이면 V2 는 FINAL 을 영원히 통과하지 못한다</b>(11 문서 ③).
 * 그 사실은 부하 시험이 끝날 때에야 드러나므로, 키를 확정하는 이 단위에서 같이 닫는다.
 *
 * <p>목으로 세우면 이 결함이 안 잡힌다 — 목은 Lua 를 실행하지 않는다.
 */
@Testcontainers(disabledWithoutDocker = true)
class ConsistencyRawValueReaderV2KeyTest {

    @Container
    private static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7.4-alpine")).withExposedPorts(6379);

    private static final long COUPON_ID = 7;
    private static final IssuanceKeys KEYS = IssuanceKeys.of(COUPON_ID);
    private static final Instant FIXED_NOW = Instant.parse("2026-08-20T12:00:00Z");

    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate redis;

    private final JdbcTemplate observationJdbcTemplate = mock(JdbcTemplate.class);
    private final TimeProvider timeProvider = new TimeProvider(Clock.fixed(FIXED_NOW, ZoneOffset.UTC));

    @BeforeAll
    static void startRedis() {
        factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getFirstMappedPort()));
        factory.afterPropertiesSet();
        factory.start();
        redis = new StringRedisTemplate(factory);
    }

    @AfterAll
    static void stopRedis() {
        factory.destroy();
    }

    @BeforeEach
    void seedV2Keys() throws Exception {
        redis.delete(List.of(KEYS.stock(), KEYS.issued(), KEYS.issuedEver()));
        redis.opsForValue().set(KEYS.stock(), "960");
        redis.opsForValue().set(KEYS.issuedEver(), "40");
        redis.opsForHash().put(KEYS.issued(), "1", "P|1700000000000|api-1-n1-t1-1|key-1");
        redis.opsForHash().put(KEYS.issued(), "2", "D|1700000000001|api-1-n1-t1-2|key-2");
        givenCoupon(1000, 40);
        givenAggregate(1000, 40, 40, 40);
    }

    @Test
    @DisplayName("v2 의 issued Hash 를 VALID 로 읽는다 — 자료형 분기에 hash 가 있어야 한다")
    void readsIssuedHashAsValid() {
        DomainRawSnapshot snapshot = reader().read();

        assertThat(snapshot.consistency().redisObservation().status()).isEqualTo(SourceStatus.VALID);
        ConsistencyRawValues values = snapshot.consistency().rawValues();
        assertThat(values.redisRemaining()).isEqualTo(960);
        assertThat(values.redisIssuedEverCount()).isEqualTo(40);
        // 파손 field 도 HLEN 에 잡힌다. 그것을 가르는 것은 corruptFieldCount 의 몫이다(05).
        assertThat(values.redisMemberEverCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("키가 아직 없는 것은 예열이다 — 고장으로 올리지 않는다")
    void missingKeysArePending() {
        redis.delete(List.of(KEYS.stock(), KEYS.issued(), KEYS.issuedEver()));

        DomainRawSnapshot snapshot = reader().read();

        assertThat(snapshot.consistency().redisObservation().status()).isEqualTo(SourceStatus.PENDING);
    }

    private ConsistencyRawValueReader reader() {
        return new ConsistencyRawValueReader(
                observationJdbcTemplate, redis,
                new DomainGaugeProperties(
                        EngineVersion.V2, COUPON_ID, null,
                        KEYS.stock(), KEYS.issuedEver(), KEYS.issued(), null),
                timeProvider);
    }

    @SuppressWarnings("unchecked")
    private void givenCoupon(long totalQuantity, long activeCount) throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        given(resultSet.getLong("coupon_id")).willReturn(COUPON_ID);
        given(resultSet.getObject("total_quantity", Long.class)).willReturn(totalQuantity);
        given(resultSet.getObject("active_count", Long.class)).willReturn(activeCount);
        given(observationJdbcTemplate.query(
                argThat(sql -> sql != null && sql.contains("FROM coupons")),
                any(RowMapper.class), any(Object[].class)))
                .willAnswer(invocation -> {
                    RowMapper<Object> rowMapper = invocation.getArgument(1);
                    return List.of(rowMapper.mapRow(resultSet, 0));
                });
    }

    @SuppressWarnings("unchecked")
    private void givenAggregate(
            long totalQuantity, long activeCount, long dbActiveCount, long dbIssuedEverCount
    ) throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        given(resultSet.getLong("total_quantity")).willReturn(totalQuantity);
        given(resultSet.getLong("active_count")).willReturn(activeCount);
        given(resultSet.getLong("db_active_count")).willReturn(dbActiveCount);
        given(resultSet.getLong("db_issued_ever_count")).willReturn(dbIssuedEverCount);
        given(observationJdbcTemplate.query(
                argThat(sql -> sql != null && sql.contains("FROM issuances")),
                any(RowMapper.class), any(Object[].class)))
                .willAnswer(invocation -> {
                    RowMapper<Object> rowMapper = invocation.getArgument(1);
                    return List.of(rowMapper.mapRow(resultSet, 0));
                });
    }
}
