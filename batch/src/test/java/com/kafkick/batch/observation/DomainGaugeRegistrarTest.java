package com.kafkick.batch.observation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;

import java.time.Instant;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import com.kafkick.batch.observation.ConsistencyRawValueReader.DomainRawSnapshot;
import com.kafkick.batch.observation.ConsistencyRawValueReader.StockSnapshot;
import com.kafkick.core.consistency.ConsistencyCalculator;
import com.kafkick.core.consistency.ConsistencyEvaluation;
import com.kafkick.core.consistency.ConsistencyGapType;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.consistency.ConsistencyRawSnapshot;
import com.kafkick.core.consistency.ConsistencyRawValues;
import com.kafkick.core.consistency.ConsistencySeverityPolicy;
import com.kafkick.core.consistency.DefaultConsistencyCalculator;
import com.kafkick.core.consistency.SourceObservation;
import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.observation.SourceStatusCode;
import com.kafkick.core.support.TimeProvider;

class DomainGaugeRegistrarTest {

    private static final Instant OBSERVED_AT = Instant.parse("2026-08-20T00:00:00Z");

    private final ConsistencyRawValueReader reader = mock(ConsistencyRawValueReader.class);
    private final ConsistencyCalculator calculator =
        new DefaultConsistencyCalculator(ConsistencySeverityPolicy.defaults());
    private final MeterRegistry registry = new SimpleMeterRegistry();

    @Test
    @DisplayName("첫 수집 전에는 값이 NaN 이고 상태가 PENDING 이다 — 0 은 정상값과 구분되지 않는다")
    void beforeFirstCollectionValuesAreNotZero() {
        register(EngineVersion.V1);

        assertThat(gap("db_counter")).isNaN();
        assertThat(gapState("db_counter")).isEqualTo(SourceStatusCode.of(SourceStatus.PENDING));
        assertThat(value(DomainMeterNames.STOCK_REMAINING)).isNaN();
        assertThat(value(DomainMeterNames.QUEUE_LENGTH)).isNaN();
        assertThat(value(DomainMeterNames.OBSERVED_COUPON_ID)).isNaN();
    }

    @Test
    @DisplayName("V1 회차에서 Redis 계열 gap 은 0 이 아니라 N_A 로 나온다")
    void redisGapsAreNotApplicableOnV1() {
        given(reader.read()).willReturn(snapshot(SourceStatus.VALID));
        given(reader.readStock()).willReturn(stockSnapshot());
        DomainGaugeRegistrar registrar = register(EngineVersion.V1);

        registrar.refreshConsistency();
        registrar.refreshStock();

        assertThat(gapState("lua")).isEqualTo(SourceStatusCode.of(SourceStatus.N_A));
        assertThat(gapState("persist")).isEqualTo(SourceStatusCode.of(SourceStatus.N_A));
        assertThat(gapState("active_db")).isEqualTo(SourceStatusCode.of(SourceStatus.N_A));
        assertThat(gap("lua")).isNaN();
        // DB 안에서 닫히는 대조라 Redis 없는 회차에서도 유효하다. 100 - 97 = 3.
        assertThat(gapState("db_counter")).isEqualTo(SourceStatusCode.of(SourceStatus.VALID));
        assertThat(gap("db_counter")).isEqualTo(3.0);
    }

    @Test
    @DisplayName("실패·대기 상태에서도 V1 은 Redis 계열 gap 과 대기열을 N_A 로 둔다")
    void v1KeepsRedisGapsNotApplicableEvenWhenCollectionFails() {
        given(reader.read()).willThrow(new IllegalStateException("계산기가 터졌다"));
        given(reader.readStock()).willThrow(new IllegalStateException("리더가 터졌다"));
        DomainGaugeRegistrar registrar = register(EngineVersion.V1);

        // 첫 수집 전(대기) 상태
        assertThat(gapState("lua")).isEqualTo(SourceStatusCode.of(SourceStatus.N_A));
        assertThat(value(DomainMeterNames.QUEUE_LENGTH_STATE))
            .isEqualTo(SourceStatusCode.of(SourceStatus.N_A));

        registrar.refreshConsistency();
        registrar.refreshStock();

        assertThat(gapState("lua"))
            .as("V1 에는 Redis 가 없다. 없는 원천이 장애로 뜨면 원자성 경보가 거짓으로 울린다")
            .isEqualTo(SourceStatusCode.of(SourceStatus.N_A));
        assertThat(gapState("persist")).isEqualTo(SourceStatusCode.of(SourceStatus.N_A));
        assertThat(gapState("active_db")).isEqualTo(SourceStatusCode.of(SourceStatus.N_A));
        assertThat(gapState("db_counter"))
            .as("DB 안에서 닫히는 대조는 V1 에서도 유효하므로 이건 진짜 장애다")
            .isEqualTo(SourceStatusCode.of(SourceStatus.UNAVAILABLE));
        assertThat(value(DomainMeterNames.QUEUE_LENGTH_STATE))
            .isEqualTo(SourceStatusCode.of(SourceStatus.N_A));
    }

    @Test
    @DisplayName("registrar 가 N_A 로 두는 gap 집합은 계산기가 V1 에서 내는 것과 같다")
    void notApplicableGapsMatchTheCalculator() {
        register(EngineVersion.V1);
        Set<String> registrarNotApplicable = Arrays.stream(ConsistencyGapType.values())
            .map(DomainMeterNames::gapTagValue)
            .filter(tag -> gapState(tag) == SourceStatusCode.of(SourceStatus.N_A))
            .collect(Collectors.toSet());

        ConsistencyEvaluation evaluation = calculator.evaluate(
            snapshotWithRedis(OBSERVED_AT, OBSERVED_AT).consistency(),
            ConsistencyPhase.LIVE, EngineVersion.V1);
        Set<String> calculatorNotApplicable = evaluation.gaps().entrySet().stream()
            .filter(entry -> entry.getValue().state() == SourceStatus.N_A)
            .map(entry -> DomainMeterNames.gapTagValue(entry.getKey()))
            .collect(Collectors.toSet());

        assertThat(registrarNotApplicable)
            .as("두 곳이 갈라지면 실패 구간에만 없는 원천이 장애로 뜬다")
            .isEqualTo(calculatorNotApplicable);
    }

    @Test
    @DisplayName("batch 가 믿고 있는 엔진 버전을 지표로 낸다 — 런타임 전환과 어긋나면 사람이 잡는다")
    void engineVersionIsExposed() {
        register(EngineVersion.V3);

        assertThat(value(DomainMeterNames.ENGINE_VERSION)).isEqualTo(3.0);
    }

    @Test
    @DisplayName("계산기가 낸 심각도를 미터로 낸다 — 안 내보내면 화면이 임계치를 다시 구현한다")
    void severityIsExposed() {
        // 총 100장인데 DB 활성이 120 — 초과 발급이라 CRITICAL 이다.
        given(reader.read()).willReturn(overIssuedSnapshot());
        DomainGaugeRegistrar registrar = register(EngineVersion.V1);

        registrar.refreshConsistency();

        assertThat(value(DomainMeterNames.CONSISTENCY_SEVERITY)).isEqualTo(2.0);
        assertThat(value(DomainMeterNames.CONSISTENCY_SEVERITY_STATE))
            .isEqualTo(SourceStatusCode.of(SourceStatus.VALID));
    }

    @Test
    @DisplayName("잴 수 없을 때 심각도와 시차는 값이 아니라 이유를 낸다")
    void severityAndSkewCarryTheirReason() {
        given(reader.read()).willReturn(snapshot(SourceStatus.VALID));
        DomainGaugeRegistrar registrar = register(EngineVersion.V1);

        registrar.refreshConsistency();

        // V1 은 Redis 가 없어 두 원천을 비교할 수 없다 — 시차가 0 인 것이 아니다.
        assertThat(value(DomainMeterNames.CONSISTENCY_SOURCE_SKEW_SECONDS)).isNaN();
        assertThat(value(DomainMeterNames.CONSISTENCY_SOURCE_SKEW_SECONDS_STATE))
            .isEqualTo(SourceStatusCode.of(SourceStatus.N_A));
    }

    @Test
    @DisplayName("판정을 못 내렸으면 그 이유를 낸다 — V1 이라서와 DB 가 죽어서는 다른 사건이다")
    void severityStateSaysWhyItCouldNotJudge() {
        // V1(redis N_A) + DB 장애 → 판정 불가. 이유는 "해당 없음" 이 아니라 "장애" 다.
        ConsistencyRawValues values = new ConsistencyRawValues(100, 0, 0, 0, 0, 0, 0);
        given(reader.read()).willReturn(new DomainRawSnapshot(
            7L,
            new ConsistencyRawSnapshot(
                values,
                new SourceObservation(SourceStatus.N_A, null),
                new SourceObservation(SourceStatus.UNAVAILABLE, null)),
            null, SourceStatus.UNAVAILABLE));
        DomainGaugeRegistrar registrar = register(EngineVersion.V1);

        registrar.refreshConsistency();

        assertThat(value(DomainMeterNames.CONSISTENCY_SEVERITY)).isNaN();
        assertThat(value(DomainMeterNames.CONSISTENCY_SEVERITY_STATE))
            .as("V1 이라 못 재는 것과 DB 가 죽어 못 재는 것이 같은 코드면 경보를 못 건다")
            .isEqualTo(SourceStatusCode.of(SourceStatus.UNAVAILABLE));
        // 시차는 반대다 — 비교할 원천이 애초에 없다.
        assertThat(value(DomainMeterNames.CONSISTENCY_SOURCE_SKEW_SECONDS_STATE))
            .isEqualTo(SourceStatusCode.of(SourceStatus.N_A));
    }

    @Test
    @DisplayName("평가에서 나온 미터에는 phase 를 붙인다 — LIVE 추세를 FINAL 판정으로 읽으면 안 된다")
    void evaluationMetersCarryThePhase() {
        register(EngineVersion.V1);

        assertThat(registry.get(DomainMeterNames.CONSISTENCY_GAP)
            .tag(DomainMeterNames.TAG_PHASE, DomainMeterNames.PHASE_LIVE)
            .tag(DomainMeterNames.TAG_GAP_TYPE, "db_counter").gauge()).isNotNull();
        assertThat(registry.get(DomainMeterNames.OVER_ISSUED)
            .tag(DomainMeterNames.TAG_PHASE, DomainMeterNames.PHASE_LIVE).gauge()).isNotNull();
    }

    @Test
    @DisplayName("값 Gauge 와 상태 Gauge 가 짝으로 등록된다")
    void valueAndStateAreSeparateMeters() {
        register(EngineVersion.V1);

        assertThat(registry.find(DomainMeterNames.CONSISTENCY_GAP).gauges()).hasSize(4);
        assertThat(registry.find(DomainMeterNames.CONSISTENCY_GAP_STATE).gauges()).hasSize(4);
        assertThat(registry.find(DomainMeterNames.OVER_ISSUED).gauge()).isNotNull();
        assertThat(registry.find(DomainMeterNames.OVER_ISSUED_STATE).gauge()).isNotNull();
        assertThat(registry.find(DomainMeterNames.LAST_SUCCESSFUL_ISSUE_EPOCH).gauge()).isNotNull();
        assertThat(registry.find(DomainMeterNames.LAST_SUCCESSFUL_ISSUE_EPOCH_STATE).gauge()).isNotNull();
    }

    @Test
    @DisplayName("회차 식별자는 라벨이 아니라 값이라 시계열이 늘지 않는다")
    void couponIdIsCarriedAsValueNotLabel() {
        given(reader.read()).willReturn(snapshot(SourceStatus.VALID));
        given(reader.readStock()).willReturn(stockSnapshot());
        DomainGaugeRegistrar registrar = register(EngineVersion.V1);

        registrar.refreshConsistency();
        registrar.refreshStock();

        assertThat(value(DomainMeterNames.OBSERVED_COUPON_ID)).isEqualTo(7.0);
        assertThat(registry.getMeters())
            .allSatisfy(meter -> assertThat(meter.getId().getTag("coupon_id")).isNull());
    }

    @Test
    @DisplayName("정합성 미터는 자기가 본 회차를 따로 낸다 — 재고와 갱신 주기가 달라 어긋난다")
    void consistencyCarriesItsOwnCouponId() {
        given(reader.read()).willReturn(snapshot(SourceStatus.VALID));
        given(reader.readStock()).willReturn(new StockSnapshot(8L, 3L, SourceStatus.VALID, null, SourceStatus.N_A));
        DomainGaugeRegistrar registrar = register(EngineVersion.V1);

        registrar.refreshConsistency();
        registrar.refreshStock();

        // 재고는 새 회차(8)로 넘어갔는데 정합성은 아직 이전 회차(7)의 집계다.
        assertThat(value(DomainMeterNames.OBSERVED_COUPON_ID)).isEqualTo(8.0);
        assertThat(value(DomainMeterNames.CONSISTENCY_COUPON_ID)).isEqualTo(7.0);
    }

    @Test
    @DisplayName("두 원천의 관측 시각 차이를 미터로 함께 낸다 — gap 중 얼마가 시차 탓인지 보게")
    void sourceSkewIsExposed() {
        given(reader.read()).willReturn(snapshotWithRedis(
            OBSERVED_AT, OBSERVED_AT.plusMillis(700)));
        DomainGaugeRegistrar registrar = register(EngineVersion.V3);

        registrar.refreshConsistency();

        // 초 단위로 자르면 0 이 되어 "시차 없음" 으로 읽힌다 — 실제 시차는 대부분 1초 미만이다.
        assertThat(value(DomainMeterNames.CONSISTENCY_SOURCE_SKEW_SECONDS)).isEqualTo(0.7);
    }

    @Test
    @DisplayName("수집이 실패하면 값을 0 으로 덮지 않고 UNAVAILABLE 로 표시한다")
    void collectionFailureDoesNotPublishZero() {
        given(reader.read()).willReturn(snapshot(SourceStatus.VALID));
        given(reader.readStock()).willReturn(stockSnapshot());
        DomainGaugeRegistrar registrar = register(EngineVersion.V1);
        registrar.refreshConsistency();
        registrar.refreshStock();
        given(reader.read()).willThrow(new IllegalStateException("관측 DB 가 응답하지 않는다"));
        given(reader.readStock()).willThrow(new IllegalStateException("관측 DB 가 응답하지 않는다"));

        registrar.refreshConsistency();
        registrar.refreshStock();

        assertThat(gap("db_counter")).isNaN();
        assertThat(gapState("db_counter")).isEqualTo(SourceStatusCode.of(SourceStatus.UNAVAILABLE));
        assertThat(value(DomainMeterNames.STOCK_REMAINING)).isNaN();
        assertThat(value(DomainMeterNames.STOCK_REMAINING_STATE))
            .isEqualTo(SourceStatusCode.of(SourceStatus.UNAVAILABLE));
    }

    @Test
    @DisplayName("마지막 발급 시각은 epoch 초로 나가고 아직 없으면 PENDING 이다")
    void lastSuccessfulIssueIsExposedAsEpochSeconds() {
        given(reader.read()).willReturn(snapshot(SourceStatus.VALID));
        given(reader.readStock()).willReturn(stockSnapshot());
        DomainGaugeRegistrar registrar = register(EngineVersion.V1);
        registrar.refreshConsistency();
        registrar.refreshStock();

        assertThat(value(DomainMeterNames.LAST_SUCCESSFUL_ISSUE_EPOCH))
            .isEqualTo((double) OBSERVED_AT.getEpochSecond());
        assertThat(value(DomainMeterNames.LAST_SUCCESSFUL_ISSUE_EPOCH_STATE))
            .isEqualTo(SourceStatusCode.of(SourceStatus.VALID));
    }

    private DomainGaugeRegistrar register(EngineVersion engineVersion) {
        DomainGaugeProperties properties =
            new DomainGaugeProperties(engineVersion, null, null, null, null, null, null);
        return new DomainGaugeRegistrar(reader, calculator, properties, registry,
            new TimeProvider(java.time.Clock.fixed(OBSERVED_AT, java.time.ZoneOffset.UTC)));
    }

    private static DomainRawSnapshot snapshot(SourceStatus databaseStatus) {
        ConsistencyRawValues values = new ConsistencyRawValues(100, 0, 0, 0, 100, 100, 97);
        return new DomainRawSnapshot(
            7L,
            new ConsistencyRawSnapshot(
                values,
                new SourceObservation(SourceStatus.N_A, null),
                new SourceObservation(databaseStatus, OBSERVED_AT)),
            OBSERVED_AT, SourceStatus.VALID
        );
    }

    /** Redis 를 먼저 읽고 DB 집계를 뒤에 돌린 결과 — 두 관측 시각이 벌어져 있다. */
    private static DomainRawSnapshot snapshotWithRedis(Instant redisAt, Instant databaseAt) {
        ConsistencyRawValues values = new ConsistencyRawValues(100, 60, 40, 40, 40, 40, 40);
        return new DomainRawSnapshot(
            7L,
            new ConsistencyRawSnapshot(
                values,
                new SourceObservation(SourceStatus.VALID, redisAt),
                new SourceObservation(SourceStatus.VALID, databaseAt)),
            databaseAt, SourceStatus.VALID
        );
    }

    /** 총 100장인데 활성이 120 — 초과 발급 20. */
    private static DomainRawSnapshot overIssuedSnapshot() {
        ConsistencyRawValues values = new ConsistencyRawValues(100, 0, 0, 0, 120, 120, 120);
        return new DomainRawSnapshot(
            7L,
            new ConsistencyRawSnapshot(
                values,
                new SourceObservation(SourceStatus.N_A, null),
                new SourceObservation(SourceStatus.VALID, OBSERVED_AT)),
            OBSERVED_AT, SourceStatus.VALID);
    }

    private static StockSnapshot stockSnapshot() {
        return new StockSnapshot(7L, 3L, SourceStatus.VALID, null, SourceStatus.N_A);
    }

    private double gap(String type) {
        return registry.get(DomainMeterNames.CONSISTENCY_GAP)
            .tag(DomainMeterNames.TAG_GAP_TYPE, type).gauge().value();
    }

    private double gapState(String type) {
        return registry.get(DomainMeterNames.CONSISTENCY_GAP_STATE)
            .tag(DomainMeterNames.TAG_GAP_TYPE, type).gauge().value();
    }

    private double value(String name) {
        Gauge gauge = registry.find(name).gauge();
        assertThat(gauge).as(name).isNotNull();
        return gauge.value();
    }
}
