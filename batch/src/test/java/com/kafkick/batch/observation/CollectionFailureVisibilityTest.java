package com.kafkick.batch.observation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.sql.ResultSet;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import com.kafkick.core.consistency.ConsistencySeverityPolicy;
import com.kafkick.core.consistency.DefaultConsistencyCalculator;
import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.support.TimeProvider;

/**
 * 수집 고장이 지표로 보이는지 본다.
 *
 * <p><b>리더를 목으로 만들지 않는다.</b> 리더는 조회 실패를 예외가 아니라 상태로 바꿔 정상
 * 반환하므로, 리더 전체를 목으로 세우고 예외를 던지게 하면 실제로는 존재하지 않는 경로를
 * 테스트가 만들어 준다 — 그래서 앞선 라운드의 가드가 통과하면서도 고장을 못 잡았다.
 * 여기서는 {@code JdbcTemplate} 이 실제로 던지는 예외에서 시작한다.
 */
class CollectionFailureVisibilityTest {

    private static final Instant NOW = Instant.parse("2026-08-20T12:00:00Z");

    /**
     * ⚠️ 시계를 고정하면 이 테스트가 무효가 된다 — "성공해서 갱신된 값" 과 "실패해서 유지된 값" 이
     * 같은 숫자가 되어, 실패 판정을 통째로 지워도 통과한다(실제로 그렇게 통과했다).
     * 수집 사이에 시간이 흐르게 해야 둘이 갈린다.
     */
    private final MutableClock clock = new MutableClock(NOW);

    private final JdbcTemplate observationJdbcTemplate = mock(JdbcTemplate.class);
    private final TimeProvider timeProvider = new TimeProvider(clock);
    private final MeterRegistry registry = new SimpleMeterRegistry();

    @Test
    @DisplayName("관측 쿼리가 상한에 잘리면 마지막 성공 시각이 갱신되지 않는다")
    void queryTimeoutDoesNotAdvanceLastSuccess() throws Exception {
        DomainGaugeRegistrar registrar = registrar();
        givenHealthyDatabase();
        registrar.refreshConsistency();
        assertThat(lastSuccess("consistency")).isEqualTo((double) NOW.getEpochSecond());

        // 리더는 이 예외를 잡아 UNAVAILABLE 스냅샷을 "정상 반환" 한다. 예외 기반 감지로는 안 잡힌다.
        given(observationJdbcTemplate.query(anyString(), any(RowMapper.class), any(Object[].class)))
            .willThrow(new QueryTimeoutException("max_execution_time 3000 에 잘렸다"));
        clock.advanceSeconds(60);

        registrar.refreshConsistency();

        assertThat(lastSuccess("consistency"))
            .as("실패했는데 마지막 성공 시각이 갱신되면 고장을 볼 방법이 없다")
            .isEqualTo((double) NOW.getEpochSecond());
        assertThat(gapState()).isEqualTo(6.0);
    }

    @Test
    @DisplayName("재고 수집도 같은 지표로 보인다 — 경로는 라벨로 구분한다")
    void stockPathIsVisibleToo() {
        DomainGaugeRegistrar registrar = registrar();
        given(observationJdbcTemplate.query(anyString(), any(RowMapper.class), any(Object[].class)))
            .willThrow(new QueryTimeoutException("관측 풀이 응답하지 않는다"));

        registrar.refreshStock();

        assertThat(lastSuccess("stock")).isNaN();
    }

    @Test
    @DisplayName("박은 회차가 존재하지 않으면 고장이다 — 오타 하나로 측정 회차 전체가 빈 그래프가 된다")
    void pinnedCouponThatDoesNotExistIsAFailure() {
        DomainGaugeRegistrar registrar = registrar();
        given(observationJdbcTemplate.query(anyString(), any(RowMapper.class), any(Object[].class)))
            .willReturn(List.of());
        clock.advanceSeconds(30);

        registrar.refreshStock();
        registrar.refreshConsistency();

        assertThat(lastSuccess("stock"))
            .as("없는 회차를 '준비 중' 으로 두면 30분 뒤에야 알아챈다")
            .isNaN();
        assertThat(lastSuccess("consistency")).isNaN();
        assertThat(gapState()).isEqualTo(6.0);
    }

    @Test
    @DisplayName("대기(PENDING)는 성공도 실패도 아니다 — 성공 시각을 전진시키면 안 된다")
    void pendingDoesNotAdvanceLastSuccess() throws Exception {
        DomainGaugeRegistrar registrar = registrarWithoutPinnedCoupon();
        givenHealthyDatabase();
        registrar.refreshStock();
        double afterSuccess = lastSuccess("stock");

        given(observationJdbcTemplate.query(anyString(), any(RowMapper.class), any(Object[].class)))
            .willReturn(List.of());
        clock.advanceSeconds(300);
        registrar.refreshStock();

        assertThat(lastSuccess("stock"))
            .as("값을 못 낸 수집을 '성공' 으로 기록하면 time() - 값 경보가 영영 안 울린다")
            .isEqualTo(afterSuccess);
    }

    @Test
    @DisplayName("회차를 안 박았고 아직 회차도 없으면 실패가 아니다 — 측정 전 대기 구간이다")
    void pendingIsNotAFailure() throws Exception {
        DomainGaugeRegistrar registrar = registrarWithoutPinnedCoupon();
        given(observationJdbcTemplate.query(anyString(), any(RowMapper.class), any(Object[].class)))
            .willReturn(List.of());

        clock.advanceSeconds(30);
        registrar.refreshStock();
        registrar.refreshConsistency();

        // 한 번도 성공한 적이 없으면 시각이 없다(NaN). time() - NaN 은 경보를 만들지 않는다.
        assertThat(lastSuccess("stock")).isNaN();
        assertThat(lastSuccess("consistency")).isNaN();
    }

    private DomainGaugeRegistrar registrar() {
        return registrar(7L);
    }

    private DomainGaugeRegistrar registrarWithoutPinnedCoupon() {
        return registrar(null);
    }

    private DomainGaugeRegistrar registrar(Long couponId) {
        DomainGaugeProperties properties =
            new DomainGaugeProperties(EngineVersion.V1, couponId, null, null, null, null, null);
        ConsistencyRawValueReader reader = new ConsistencyRawValueReader(
            observationJdbcTemplate, null, properties, timeProvider);
        return new DomainGaugeRegistrar(
            reader, new DefaultConsistencyCalculator(ConsistencySeverityPolicy.defaults()),
            properties, registry, timeProvider);
    }

    @SuppressWarnings("unchecked")
    private void givenHealthyDatabase() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        given(resultSet.getLong(anyString())).willReturn(10L);
        given(resultSet.getObject(anyString(), eq(Long.class))).willReturn(10L);
        given(observationJdbcTemplate.query(anyString(), any(RowMapper.class), any(Object[].class)))
            .willAnswer(invocation -> {
                RowMapper<Object> rowMapper = invocation.getArgument(1);
                return List.of(rowMapper.mapRow(resultSet, 0));
            });
    }

    private double lastSuccess(String path) {
        return registry.get(DomainMeterNames.COLLECT_LAST_SUCCESS_EPOCH)
            .tag(DomainMeterNames.TAG_COLLECT_PATH, path).gauge().value();
    }

    /** 수집 사이에 시간이 흐르는 시계. {@code Clock.fixed} 로는 이 테스트가 성립하지 않는다. */
    private static final class MutableClock extends Clock {

        private Instant instant;

        private MutableClock(Instant instant) {
            this.instant = instant;
        }

        void advanceSeconds(long seconds) {
            instant = instant.plusSeconds(seconds);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }

    private double gapState() {
        return registry.get(DomainMeterNames.CONSISTENCY_GAP_STATE)
            .tag(DomainMeterNames.TAG_GAP_TYPE, "db_counter").gauge().value();
    }
}
