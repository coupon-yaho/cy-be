package com.kafkick.batch.observation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.sql.ResultSet;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.junit.jupiter.api.Test;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.connection.DataType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.kafkick.batch.observation.ConsistencyRawValueReader.DomainRawSnapshot;
import com.kafkick.batch.observation.ConsistencyRawValueReader.StockSnapshot;
import com.kafkick.core.consistency.ConsistencyRawValues;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;

class ConsistencyRawValueReaderTest {

    /** 회차 선택 시각을 고정한다 — 실시간을 보면 이 테스트가 시계에 따라 다른 값을 넣는다. */
    private static final Instant FIXED_NOW = Instant.parse("2026-08-20T12:00:00Z");

    private final TimeProvider timeProvider = new TimeProvider(Clock.fixed(FIXED_NOW, java.time.ZoneOffset.UTC));
    private final JdbcTemplate observationJdbcTemplate = mock(JdbcTemplate.class);
    private final StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);

    @Test
    @DisplayName("정합성 원시값은 재고 카운터와 행 집계를 한 문장에서 읽는다")
    void consistencyValuesComeFromOneStatement() throws Exception {
        givenCoupon(7L, 120, 97);
        givenAggregate(120, 97, 98, 110, LocalDateTime.parse("2026-08-20T00:00:00"));

        DomainRawSnapshot snapshot = reader(EngineVersion.V1).read();

        ConsistencyRawValues values = snapshot.consistency().rawValues();
        assertThat(values.totalQuantity()).isEqualTo(120);
        assertThat(values.storedActiveCount()).isEqualTo(97);
        assertThat(values.dbActiveCount()).isEqualTo(98);
        assertThat(values.dbIssuedEverCount()).isEqualTo(110);
        assertThat(snapshot.consistency().databaseObservation().status()).isEqualTo(SourceStatus.VALID);
        assertThat(snapshot.couponId()).isEqualTo(7L);
    }

    @Test
    @DisplayName("집계 SQL 의 활성 상태 목록은 IssuanceStatus 에서 나온다")
    void activeStatusListFollowsTheEnum() throws Exception {
        givenCoupon(7L, 120, 97);
        givenAggregate(120, 97, 98, 110, null);

        reader(EngineVersion.V1).read();

        verify(observationJdbcTemplate).query(
            argThat(sql -> sql.contains("i.status IN ('ISSUED', 'USED')")),
            any(RowMapper.class), any(Object[].class));
    }

    @Test
    @DisplayName("IssuanceStatus 가 늘면 누적 발급 목록을 다시 판단해야 한다")
    void issuedEverContractCoversEveryKnownStatus() {
        assertThat(IssuanceStatus.values())
            .as("ConsistencyRawValues 계약이 네 상태를 열거한다. 상수가 늘면 그 상태가 '발급 이력' 에"
                + " 포함되는지 사람이 정하고, core 의 계약 javadoc 과 ISSUED_EVER_STATUS_LIST 를"
                + " 함께 고쳐야 한다 — 자동으로 따라가면 발급 전 단계까지 세어 persist gap 이 음수가 된다")
            .containsExactlyInAnyOrder(
                IssuanceStatus.ISSUED, IssuanceStatus.USED,
                IssuanceStatus.CANCELLED, IssuanceStatus.EXPIRED);
    }

    @Test
    @DisplayName("누적 발급도 상태 목록을 enum 에서 뽑는다 — COUNT(*) 는 계약 밖 상태까지 센다")
    void issuedEverCountFollowsTheEnumToo() throws Exception {
        givenCoupon(7L, 120, 97);
        givenAggregate(120, 97, 98, 110, null);

        reader(EngineVersion.V1).read();

        verify(observationJdbcTemplate).query(
            argThat(sql -> sql != null
                && sql.contains("'ISSUED', 'USED', 'CANCELLED', 'EXPIRED'")
                && !sql.contains("COUNT(*)")),
            any(RowMapper.class), any(Object[].class));
    }

    @Test
    @DisplayName("V1 은 Redis 를 아예 조회하지 않고 N_A 로 둔다")
    void v1DoesNotTouchRedis() throws Exception {
        givenCoupon(7L, 120, 97);
        givenAggregate(120, 97, 98, 110, null);

        DomainRawSnapshot snapshot = reader(EngineVersion.V1).read();
        StockSnapshot stock = reader(EngineVersion.V1).readStock();

        verifyNoInteractions(redisTemplate);
        assertThat(snapshot.consistency().redisObservation().status()).isEqualTo(SourceStatus.N_A);
        assertThat(stock.queueStatus()).isEqualTo(SourceStatus.N_A);
        // V1 재고의 권위는 DB 다. 120 - 97 = 23.
        assertThat(stock.stockRemaining()).isEqualTo(23L);
        assertThat(stock.stockStatus()).isEqualTo(SourceStatus.VALID);
    }

    @Test
    @DisplayName("V2·V3 의 Redis 세 값은 한 스크립트로 원자적으로 읽는다 — 왕복을 나누면 LUA_GAP 이 오탐한다")
    void redisConsistencyValuesAreReadAtomically() throws Exception {
        givenCoupon(7L, 1000, 40);
        givenAggregate(1000, 40, 40, 40, null);
        given(redisTemplate.execute(any(RedisScript.class), anyList()))
            .willReturn(List.of("960", "40", 40L));

        DomainRawSnapshot snapshot = reader(EngineVersion.V3).read();

        ConsistencyRawValues values = snapshot.consistency().rawValues();
        assertThat(values.redisRemaining()).isEqualTo(960);
        assertThat(values.redisIssuedEverCount()).isEqualTo(40);
        assertThat(values.redisMemberEverCount()).isEqualTo(40);
        assertThat(snapshot.consistency().redisObservation().status()).isEqualTo(SourceStatus.VALID);
    }

    @Test
    @DisplayName("Redis 를 DB 집계보다 먼저 읽는다 — 시차의 방향을 고정해야 gap 해석이 일정하다")
    void redisIsReadBeforeTheSlowAggregate() throws Exception {
        givenCoupon(7L, 1000, 40);
        givenAggregate(1000, 40, 40, 40, null);
        given(redisTemplate.execute(any(RedisScript.class), anyList()))
            .willReturn(List.of("960", "40", 40L));

        reader(EngineVersion.V3).read();

        InOrder inOrder = inOrder(redisTemplate, observationJdbcTemplate);
        inOrder.verify(redisTemplate).execute(any(RedisScript.class), anyList());
        inOrder.verify(observationJdbcTemplate).query(
            argThat(sql -> sql != null && sql.contains("FROM issuances")),
            any(RowMapper.class), any(Object[].class));
    }

    @Test
    @DisplayName("Redis 왕복 시간이 시차에 포함된다 — 왕복 뒤에 시각을 찍으면 시차가 0 으로 보인다")
    void redisRoundTripCountsTowardTheSkew() throws Exception {
        givenCoupon(7L, 1000, 40);
        givenAggregate(1000, 40, 40, 40, null);
        MovingClock clock = new MovingClock(FIXED_NOW);
        given(redisTemplate.execute(any(RedisScript.class), anyList())).willAnswer(invocation -> {
            clock.advanceMillis(300);   // Redis 왕복 300ms
            return List.of("960", "40", 40L);
        });

        DomainRawSnapshot snapshot = new ConsistencyRawValueReader(
            observationJdbcTemplate, redisTemplate,
            properties(EngineVersion.V3, 7L), new TimeProvider(clock)).read();

        assertThat(snapshot.consistency().redisObservation().observedAt())
            .as("Redis 값의 기준 시각은 요청을 보낸 순간이지 응답을 받은 순간이 아니다")
            .isEqualTo(FIXED_NOW);
        assertThat(snapshot.consistency().databaseObservation().observedAt())
            .isEqualTo(FIXED_NOW.plusMillis(300));
    }

    @Test
    @DisplayName("V2·V3 재고 잔량의 권위는 Redis 카운터다")
    void redisOwnsStockOnV2AndV3() throws Exception {
        givenCoupon(7L, 1000, 40);
        ValueOperations<String, String> valueOperations = mock(ValueOperations.class);
        given(redisTemplate.opsForValue()).willReturn(valueOperations);
        given(valueOperations.get(anyString())).willReturn("960");
        given(redisTemplate.type(anyString())).willReturn(DataType.NONE);

        StockSnapshot stock = reader(EngineVersion.V3).readStock();

        assertThat(stock.stockRemaining()).isEqualTo(960L);
        assertThat(stock.stockStatus()).isEqualTo(SourceStatus.VALID);
        // 대기열 키가 아직 없다. 0 으로 단정하면 대기가 걸린 순간에도 화면이 평온해 보인다.
        assertThat(stock.queueLength()).isNull();
        assertThat(stock.queueStatus()).isEqualTo(SourceStatus.PENDING);
    }

    @Test
    @DisplayName("Redis 키가 아직 없으면 0 이 아니라 PENDING 이다 — 0 은 '다 팔렸다' 로 읽힌다")
    void missingRedisKeysArePending() throws Exception {
        givenCoupon(7L, 1000, 0);
        givenAggregate(1000, 0, 0, 0, null);
        given(redisTemplate.execute(any(RedisScript.class), anyList()))
            .willReturn(java.util.Arrays.asList(null, null, Boolean.FALSE));

        DomainRawSnapshot snapshot = reader(EngineVersion.V3).read();

        assertThat(snapshot.consistency().redisObservation().status()).isEqualTo(SourceStatus.PENDING);
    }

    @Test
    @DisplayName("Redis 값이 숫자가 아니면 예열이 아니라 고장이다")
    void nonNumericRedisValueIsAFailureNotWarmup() throws Exception {
        givenCoupon(7L, 1000, 0);
        givenAggregate(1000, 0, 0, 0, null);
        given(redisTemplate.execute(any(RedisScript.class), anyList()))
            .willReturn(List.of("abc", "40", 40L));

        DomainRawSnapshot snapshot = reader(EngineVersion.V3).read();

        assertThat(snapshot.consistency().redisObservation().status())
            .as("오설정을 PENDING 으로 두면 영영 안 나올 값을 '곧 나온다' 로 보여준다")
            .isEqualTo(SourceStatus.UNAVAILABLE);
    }

    @Test
    @DisplayName("대기열 키가 예상 밖 자료형이면 예열이 아니라 고장이다")
    void unexpectedQueueKeyTypeIsAFailureNotWarmup() throws Exception {
        givenCoupon(7L, 1000, 40);
        ValueOperations<String, String> valueOperations = mock(ValueOperations.class);
        given(redisTemplate.opsForValue()).willReturn(valueOperations);
        given(valueOperations.get(anyString())).willReturn("960");
        given(redisTemplate.type(anyString())).willReturn(DataType.HASH);

        StockSnapshot stock = reader(EngineVersion.V3).readStock();

        assertThat(stock.queueStatus())
            .as("키를 잘못 가리킨 설정과 예열 중을 같은 상태로 두면 아무 경보도 안 걸린다")
            .isEqualTo(SourceStatus.UNAVAILABLE);
    }

    @Test
    @DisplayName("Redis 통로가 죽으면 UNAVAILABLE 이다")
    void redisFailureIsUnavailable() throws Exception {
        givenCoupon(7L, 1000, 0);
        givenAggregate(1000, 0, 0, 0, null);
        given(redisTemplate.execute(any(RedisScript.class), anyList()))
            .willThrow(new IllegalStateException("연결이 끊겼다"));

        DomainRawSnapshot snapshot = reader(EngineVersion.V3).read();

        assertThat(snapshot.consistency().redisObservation().status())
            .isEqualTo(SourceStatus.UNAVAILABLE);
    }

    @Test
    @DisplayName("V2·V3 인데 Redis 통로가 없으면 UNAVAILABLE 이다")
    void missingRedisTemplateIsUnavailable() throws Exception {
        givenCoupon(7L, 1000, 0);
        givenAggregate(1000, 0, 0, 0, null);
        ConsistencyRawValueReader reader = new ConsistencyRawValueReader(
            observationJdbcTemplate, null, properties(EngineVersion.V3, null), timeProvider);

        assertThat(reader.read().consistency().redisObservation().status())
            .isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(reader.readStock().stockStatus()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    @Test
    @DisplayName("회차 선택 시각은 주입된 시계에서 온다 — 직접 now() 를 부르면 테스트가 못 고정한다")
    void couponSelectionUsesTheInjectedClock() throws Exception {
        givenCoupon(7L, 120, 97);

        reader(EngineVersion.V1).readStock();

        ArgumentCaptor<Object[]> args = ArgumentCaptor.forClass(Object[].class);
        verify(observationJdbcTemplate).query(
            argThat(sql -> sql.contains("c.open_at <= ?")), any(RowMapper.class), args.capture());
        assertThat(args.getValue())
            .containsExactly(LocalDateTime.ofInstant(FIXED_NOW, java.time.ZoneOffset.UTC));
    }

    @Test
    @DisplayName("회차를 고정하면 '가장 최근 열린 회차' 를 다시 고르지 않는다")
    void fixedCouponIdPinsTheTarget() throws Exception {
        givenCoupon(42L, 1000, 0);
        ConsistencyRawValueReader reader = new ConsistencyRawValueReader(
            observationJdbcTemplate, redisTemplate, properties(EngineVersion.V1, 42L), timeProvider);

        assertThat(reader.readStock().couponId()).isEqualTo(42L);
        verify(observationJdbcTemplate).query(
            argThat(sql -> sql.contains("WHERE c.id = ?")), any(RowMapper.class), any(Object[].class));
    }

    @Test
    @DisplayName("아직 발급이 없으면 마지막 발급 시각은 0 이 아니라 PENDING 이다")
    void lastIssueIsPendingBeforeFirstIssuance() throws Exception {
        givenCoupon(7L, 120, 0);
        givenAggregate(120, 0, 0, 0, null);

        DomainRawSnapshot snapshot = reader(EngineVersion.V1).read();

        assertThat(snapshot.lastSuccessfulIssueAt()).isNull();
        assertThat(snapshot.lastSuccessfulIssueStatus()).isEqualTo(SourceStatus.PENDING);
    }

    @Test
    @DisplayName("마지막 발급 시각은 UTC 로 해석한다 — JVM 기본 시간대로 읽으면 quiet period 가 틀어진다")
    void lastIssuedAtIsInterpretedAsUtc() throws Exception {
        givenCoupon(7L, 120, 1);
        givenAggregate(120, 1, 1, 1, LocalDateTime.parse("2026-08-20T01:02:03"));

        DomainRawSnapshot snapshot = reader(EngineVersion.V1).read();

        assertThat(snapshot.lastSuccessfulIssueAt()).isEqualTo(Instant.parse("2026-08-20T01:02:03Z"));
    }

    @Test
    @DisplayName("집계가 0행이면 대기가 아니라 고장이다 — 재고 행이 사라진 파손 상태다")
    void emptyAggregateIsAFailureNotPending() throws Exception {
        givenCoupon(7L, 120, 97);
        given(observationJdbcTemplate.query(
            argThat(sql -> sql != null && sql.contains("FROM issuances")),
            any(RowMapper.class), any(Object[].class)))
            .willReturn(List.of());

        DomainRawSnapshot snapshot = reader(EngineVersion.V1).read();

        assertThat(snapshot.consistency().databaseObservation().status())
            .as("PENDING 이면 실패로 안 세어 경보가 영영 안 걸린다")
            .isEqualTo(SourceStatus.UNAVAILABLE);
    }

    @Test
    @DisplayName("집계가 실패해도 어느 회차였는지는 남긴다")
    void failedAggregateKeepsTheCouponId() throws Exception {
        givenCoupon(7L, 120, 97);
        given(observationJdbcTemplate.query(
            argThat(sql -> sql != null && sql.contains("FROM issuances")),
            any(RowMapper.class), any(Object[].class)))
            .willThrow(new QueryTimeoutException("집계가 3초를 넘겼다"));

        DomainRawSnapshot snapshot = reader(EngineVersion.V1).read();

        assertThat(snapshot.couponId())
            .as("회차를 버리면 여러 회차 연속 측정에서 어느 회차가 죽었는지 못 가른다")
            .isEqualTo(7L);
    }

    @Test
    @DisplayName("V1 에서는 어떤 실패에서도 대기열이 N_A 다 — 없는 기능이 장애로 표시되면 안 된다")
    void v1QueueStaysNotApplicableOnEveryFailurePath() {
        given(observationJdbcTemplate.query(anyString(), any(RowMapper.class), any(Object[].class)))
            .willReturn(List.of());
        ConsistencyRawValueReader pinned = new ConsistencyRawValueReader(
            observationJdbcTemplate, redisTemplate, properties(EngineVersion.V1, 999L), timeProvider);

        StockSnapshot typo = pinned.readStock();

        assertThat(typo.stockStatus()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(typo.queueStatus()).isEqualTo(SourceStatus.N_A);
    }

    @Test
    @DisplayName("관측 쿼리가 실패하면 예외 대신 UNAVAILABLE 을 돌려준다")
    void databaseFailureBecomesUnavailable() {
        given(observationJdbcTemplate.query(anyString(), any(RowMapper.class), any(Object[].class)))
            .willThrow(new QueryTimeoutException("관측 쿼리가 3초를 넘겼다"));

        ConsistencyRawValueReader reader = reader(EngineVersion.V1);

        assertThat(reader.read().consistency().databaseObservation().status())
            .isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(reader.readStock().stockStatus()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(reader.readStock().couponId()).isNull();
    }

    @Test
    @DisplayName("회차는 있는데 재고 행이 없으면 고장이다 — 이전 회차를 조용히 보여주지 않는다")
    void couponWithoutStockRowIsAFailure() throws Exception {
        givenCouponWithoutStockRow(7L);

        StockSnapshot stock = reader(EngineVersion.V1).readStock();

        assertThat(stock.couponId()).as("어느 회차가 미완성인지는 알 수 있어야 한다").isEqualTo(7L);
        assertThat(stock.stockRemaining()).isNull();
        assertThat(stock.stockStatus()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    @Test
    @DisplayName("재고 행이 없는 회차도 조회 대상에서 빠지지 않는다 — LEFT JOIN 이어야 한다")
    void couponWithoutStockRowIsStillSelected() throws Exception {
        givenCouponWithoutStockRow(7L);

        reader(EngineVersion.V1).read();

        verify(observationJdbcTemplate).query(
            argThat(sql -> sql != null && sql.contains("LEFT JOIN coupon_stocks")),
            any(RowMapper.class), any(Object[].class));
    }

    @Test
    @DisplayName("회차가 아직 없으면 PENDING 이다")
    void noCouponYetIsPending() {
        given(observationJdbcTemplate.query(anyString(), any(RowMapper.class), any(Object[].class)))
            .willReturn(List.of());

        ConsistencyRawValueReader reader = reader(EngineVersion.V1);

        assertThat(reader.read().consistency().databaseObservation().status())
            .isEqualTo(SourceStatus.PENDING);
        assertThat(reader.readStock().stockRemaining()).isNull();
    }

    /** 명시적으로만 흐르는 시계. Redis 왕복을 그 안에서 흘려 보낸다. */
    private static final class MovingClock extends Clock {

        private Instant instant;

        private MovingClock(Instant instant) {
            this.instant = instant;
        }

        void advanceMillis(long millis) {
            instant = instant.plusMillis(millis);
        }

        @Override
        public java.time.ZoneId getZone() {
            return java.time.ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }

    private ConsistencyRawValueReader reader(EngineVersion engineVersion) {
        return new ConsistencyRawValueReader(
            observationJdbcTemplate, redisTemplate, properties(engineVersion, null), timeProvider);
    }

    private static DomainGaugeProperties properties(EngineVersion engineVersion, Long couponId) {
        return new DomainGaugeProperties(engineVersion, couponId, null, null, null, null, null);
    }

    /** LEFT JOIN 결과에서 재고 컬럼이 전부 null 인 행 — 회차는 있고 재고 행만 없는 상태. */
    @SuppressWarnings("unchecked")
    private void givenCouponWithoutStockRow(long couponId) throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        given(resultSet.getLong("coupon_id")).willReturn(couponId);
        given(resultSet.getObject("total_quantity", Long.class)).willReturn(null);
        given(resultSet.getObject("active_count", Long.class)).willReturn(null);
        given(observationJdbcTemplate.query(
            argThat(sql -> sql != null && sql.contains("FROM coupons")),
            any(RowMapper.class), any(Object[].class)))
            .willAnswer(invocation -> {
                RowMapper<Object> rowMapper = invocation.getArgument(1);
                return List.of(rowMapper.mapRow(resultSet, 0));
            });
    }

    @SuppressWarnings("unchecked")
    private void givenCoupon(long couponId, long totalQuantity, long activeCount) throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        given(resultSet.getLong("coupon_id")).willReturn(couponId);
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
        long totalQuantity,
        long activeCount,
        long dbActiveCount,
        long dbIssuedEverCount,
        LocalDateTime lastIssuedAt
    ) throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        given(resultSet.getLong("total_quantity")).willReturn(totalQuantity);
        given(resultSet.getLong("active_count")).willReturn(activeCount);
        given(resultSet.getLong("db_active_count")).willReturn(dbActiveCount);
        given(resultSet.getLong("db_issued_ever_count")).willReturn(dbIssuedEverCount);
        given(resultSet.getObject("last_issued_at", LocalDateTime.class)).willReturn(lastIssuedAt);
        given(observationJdbcTemplate.query(
            argThat(sql -> sql != null && sql.contains("FROM issuances")),
            any(RowMapper.class), any(Object[].class)))
            .willAnswer(invocation -> {
                RowMapper<Object> rowMapper = invocation.getArgument(1);
                return List.of(rowMapper.mapRow(resultSet, 0));
            });
    }
}
