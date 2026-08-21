package com.kafkick.batch.observation;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import com.kafkick.batch.observation.ConsistencyRawValueReader.DomainRawSnapshot;
import com.kafkick.batch.observation.ConsistencyRawValueReader.StockSnapshot;
import com.kafkick.core.consistency.ConsistencyCalculator;
import com.kafkick.core.consistency.ConsistencyEvaluation;
import com.kafkick.core.consistency.ConsistencyGapType;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.consistency.GapValue;
import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.observation.SourceStatusCode;
import com.kafkick.core.support.TimeProvider;

/**
 * 원시값을 읽어 정합성 계산기에 넘기고, 그 결과를 Gauge 로 내놓는다. 시계열 보관은 Prometheus 가
 * 하므로 이 클래스는 "지금 값" 만 들고 있는다.
 *
 * <p><b>값과 상태를 나눠 낸다.</b> Prometheus 샘플은 숫자 하나뿐이라 값이 없는 이유를 담을 자리가
 * 없다. 값 미터에 0 을 실으면 "정상인데 0" 과 구분되지 않아 재고 소진과 수집 실패가 같은 그래프가
 * 된다. 그래서 값이 없으면 값 미터는 NaN 이고, 이유는 짝이 되는 상태 미터가 낸다.
 *
 * <p><b>일관성의 범위</b> — 갱신은 불변 스냅샷을 통째로 갈아 끼우므로 한 번의 supplier 호출은
 * 언제나 한 시점의 값을 본다. 다만 scrape 는 미터를 하나씩 읽어 가므로, 그 사이에 갱신이 끼면
 * <b>값과 상태가 한 틱 어긋난 채 실릴 수 있다</b>(예: 상태 VALID + 값 NaN). Micrometer 에는
 * scrape 를 원자 구간으로 묶을 자리가 없어 두 미터 설계에서는 남는 창이다. 화면은 값이 없는 샘플을
 * 상태와 무관하게 "표시 없음" 으로 그려야 한다 — 값 없음을 0 으로 대체하는 것보다 훨씬 안전하다.
 *
 * <p><b>정합성은 계산하지 않는다.</b> 산식은 {@link ConsistencyCalculator} 소유다. 여기서 다시
 * 만들면 화면의 숫자와 검증 배치의 판정이 서로 다른 산식을 쓰게 된다.
 */
public class DomainGaugeRegistrar {

    private static final Logger log = LoggerFactory.getLogger(DomainGaugeRegistrar.class);

    private final ConsistencyRawValueReader reader;
    private final ConsistencyCalculator calculator;
    private final DomainGaugeProperties properties;
    private final AtomicReference<ConsistencyState> consistency;
    private final AtomicReference<StockState> stock;
    private final TimeProvider timeProvider;
    private final CollectOutcome consistencyOutcome = new CollectOutcome(DomainMeterNames.PATH_CONSISTENCY);
    private final CollectOutcome stockOutcome = new CollectOutcome(DomainMeterNames.PATH_STOCK);

    public DomainGaugeRegistrar(
        ConsistencyRawValueReader reader,
        ConsistencyCalculator calculator,
        DomainGaugeProperties properties,
        MeterRegistry registry,
        TimeProvider timeProvider
    ) {
        this.reader = reader;
        this.calculator = calculator;
        this.properties = properties;
        this.timeProvider = timeProvider;
        this.consistency = new AtomicReference<>(ConsistencyState.pending(properties.engineVersion()));
        this.stock = new AtomicReference<>(StockState.pending(properties.engineVersion()));
        registerGauges(registry);
    }

    /**
     * 재고 잔량·대기열 길이를 갱신한다. PK 조회라 1초 주기로 돌아도 측정 대상을 흔들지 않는다.
     */
    @Scheduled(fixedRateString = "${observation.domain-gauge.interval-ms:1000}")
    public void refreshStock() {
        try {
            StockSnapshot snapshot = reader.readStock();
            stock.set(StockState.of(snapshot));
            // 리더는 조회 실패를 예외가 아니라 상태로 돌려준다. 예외만 보면 고장을 영영 못 센다.
            record(stockOutcome, outcomeOf(snapshot.stockStatus(), snapshot.queueStatus()), null);
        } catch (RuntimeException exception) {
            stock.set(StockState.unavailable(properties.engineVersion()));
            record(stockOutcome, Outcome.FAILURE, exception);
        }
    }

    /**
     * 정합성 gap 을 갱신한다. issuances 를 훑으므로 재고보다 느린 주기로 돈다 — 1초마다 300만 행을
     * 집계하면 관측 풀을 나눠 놨어도 MySQL 의 버퍼풀·I/O 는 공유라 v1 측정이 오염된다.
     *
     * <p>수집에 실패해도 이전 값을 그대로 두지 않는다. 마지막 값이 현재값처럼 계속 보이면 멈춘
     * 수집이 정상 그래프로 읽힌다. 값을 지우고 상태를 UNAVAILABLE 로 바꿔 화면이 구분하게 한다.
     */
    // 값의 주인은 application.yml 이다. 여기 숫자는 키가 통째로 빠졌을 때만 쓰이는 안전값이라
    // yml 과 같게 유지한다 — 어긋나 있으면 키를 빠뜨린 배포가 조용히 더 촘촘하게 돈다.
    @Scheduled(fixedRateString = "${observation.domain-gauge.aggregate-interval-ms:30000}")
    public void refreshConsistency() {
        try {
            DomainRawSnapshot snapshot = reader.read();
            ConsistencyEvaluation evaluation = calculator.evaluate(
                snapshot.consistency(), ConsistencyPhase.LIVE, properties.engineVersion());
            consistency.set(ConsistencyState.of(snapshot, evaluation));
            record(consistencyOutcome, outcomeOf(
                snapshot.consistency().databaseObservation().status(),
                snapshot.consistency().redisObservation().status()), null);
        } catch (RuntimeException exception) {
            consistency.set(ConsistencyState.unavailable(properties.engineVersion()));
            record(consistencyOutcome, Outcome.FAILURE, exception);
        }
    }

    /**
     * 수집 한 번의 결말. <b>세 가지</b>다 — 값을 냈거나, 못 냈거나, 낼 것이 없었거나.
     *
     * <p>두 가지로만 나누면 "회차가 아직 없다 · Redis 예열 중"(NO_VALUE)이 성공으로 기록되어
     * 마지막 성공 시각이 계속 전진한다. 그러면 값이 몇 분째 안 나와도 경보가 울리지 않는다.
     * 반대로 실패로 세면 측정 시작 전 대기 구간이 통째로 경보가 된다. 그래서 <b>시각을 건드리지
     * 않는</b> 세 번째 결말이 필요하다.
     */
    private enum Outcome {
        SUCCESS,
        FAILURE,
        NO_VALUE
    }

    private static Outcome outcomeOf(SourceStatus... statuses) {
        boolean noValue = false;
        for (SourceStatus status : statuses) {
            if (status == SourceStatus.UNAVAILABLE) {
                return Outcome.FAILURE;
            }
            if (status == SourceStatus.PENDING) {
                noValue = true;
            }
        }
        return noValue ? Outcome.NO_VALUE : Outcome.SUCCESS;
    }

    private void record(CollectOutcome outcome, Outcome result, RuntimeException exception) {
        if (result == Outcome.NO_VALUE) {
            // 성공도 실패도 아니다. 마지막 성공 시각을 그대로 둔다 — 한 번도 성공한 적이 없으면
            // 값이 없는 상태(NaN)로 남고, time() - NaN 은 경보를 만들지 않는다.
            return;
        }
        if (result == Outcome.SUCCESS) {
            outcome.lastSuccessEpoch.set(timeProvider.instant().getEpochSecond());
            outcome.consecutiveFailures.set(0);
            return;
        }
        int failures = outcome.consecutiveFailures.incrementAndGet();
        // 한 번 끊긴 것과 계속 잘리는 것은 다른 사건이다. 후자는 값이 영원히 UNAVAILABLE 로 굳은
        // 상태라 화면은 조용한데 초과 발급 KPI 가 죽어 있다 — WARN 으로는 안 보인다.
        if (failures >= properties.consecutiveFailureAlarm()) {
            log.error("{} 수집이 {}회 연속 실패했다. 값이 계속 비어 있다.",
                outcome.path, failures, exception);
        } else {
            log.warn("{} 수집에 실패했다. 값을 비우고 UNAVAILABLE 로 표시한다.", outcome.path, exception);
        }
    }

    private void registerGauges(MeterRegistry registry) {
        for (ConsistencyGapType gapType : ConsistencyGapType.values()) {
            String tagValue = DomainMeterNames.gapTagValue(gapType);
            evaluationGauge(registry, DomainMeterNames.CONSISTENCY_GAP, tagValue,
                () -> value(consistency.get().gaps().get(gapType)));
            evaluationGauge(registry, DomainMeterNames.CONSISTENCY_GAP_STATE, tagValue,
                () -> code(consistency.get().gaps().get(gapType).state()));
        }
        evaluationGauge(registry, DomainMeterNames.OVER_ISSUED, null,
            () -> value(consistency.get().overIssued()));
        evaluationGauge(registry, DomainMeterNames.OVER_ISSUED_STATE, null,
            () -> code(consistency.get().overIssued().state()));
        evaluationGauge(registry, DomainMeterNames.CONSISTENCY_SEVERITY, null,
            () -> value(consistency.get().severityCode()));
        evaluationGauge(registry, DomainMeterNames.CONSISTENCY_SEVERITY_STATE, null,
            () -> code(consistency.get().severityStatus()));
        gauge(registry, DomainMeterNames.LAST_SUCCESSFUL_ISSUE_EPOCH, null,
            () -> value(consistency.get().lastSuccessfulIssueEpochSeconds()));
        gauge(registry, DomainMeterNames.LAST_SUCCESSFUL_ISSUE_EPOCH_STATE, null,
            () -> code(consistency.get().lastSuccessfulIssueStatus()));
        // 재고 미터와 갱신 주기가 달라 회차가 바뀌는 순간 둘이 어긋난다. 어느 회차를 본 gap 인지
        // 값 자체로 실어야 그 창을 질의에서 걸러낼 수 있다.
        gauge(registry, DomainMeterNames.CONSISTENCY_COUPON_ID, null,
            () -> value(consistency.get().couponId()));
        gauge(registry, DomainMeterNames.CONSISTENCY_SOURCE_SKEW_SECONDS, null,
            () -> value(consistency.get().sourceSkewSeconds()));
        gauge(registry, DomainMeterNames.CONSISTENCY_SOURCE_SKEW_SECONDS_STATE, null,
            () -> code(consistency.get().comparableStatus()));
        for (CollectOutcome outcome : List.of(consistencyOutcome, stockOutcome)) {
            Gauge.builder(DomainMeterNames.COLLECT_LAST_SUCCESS_EPOCH,
                    (Supplier<Number>) () -> value(outcome.lastSuccessEpoch.get()))
                .tag(DomainMeterNames.TAG_COLLECT_PATH, outcome.path)
                .strongReference(true)
                .register(registry);
        }

        gauge(registry, DomainMeterNames.QUEUE_LENGTH, null, () -> value(stock.get().queueLength()));
        gauge(registry, DomainMeterNames.QUEUE_LENGTH_STATE, null, () -> code(stock.get().queueStatus()));
        gauge(registry, DomainMeterNames.STOCK_REMAINING, null, () -> value(stock.get().stockRemaining()));
        gauge(registry, DomainMeterNames.STOCK_REMAINING_STATE, null,
            () -> code(stock.get().stockStatus()));
        gauge(registry, DomainMeterNames.OBSERVED_COUPON_ID, null, () -> value(stock.get().couponId()));
        // 기동 시점 설정이라 값이 변하지 않는다. 런타임 전환과 어긋났는지는 화면이 대조한다.
        gauge(registry, DomainMeterNames.ENGINE_VERSION, null,
            () -> SourceStatusCode.of(properties.engineVersion()));
    }

    /** 계산기의 LIVE 평가에서 나온 값. phase 라벨로 FINAL 판정과 구분한다. */
    private static void evaluationGauge(
        MeterRegistry registry, String name, String gapType, Supplier<Number> value
    ) {
        Gauge.Builder<Supplier<Number>> builder = Gauge.builder(name, value)
            .strongReference(true)
            .tag(DomainMeterNames.TAG_PHASE, DomainMeterNames.PHASE_LIVE);
        if (gapType != null) {
            builder.tag(DomainMeterNames.TAG_GAP_TYPE, gapType);
        }
        builder.register(registry);
    }

    /**
     * Micrometer Gauge 는 약한 참조가 기본이라 GC 가 지나가면 조용히 NaN 으로 굳는다.
     * 부하 테스트 중간에 그래프가 끊기고 원인은 남지 않는다.
     */
    private static void gauge(MeterRegistry registry, String name, String gapType, Supplier<Number> value) {
        Gauge.Builder<Supplier<Number>> builder = Gauge.builder(name, value).strongReference(true);
        if (gapType != null) {
            builder.tag(DomainMeterNames.TAG_GAP_TYPE, gapType);
        }
        builder.register(registry);
    }

    private static double value(GapValue gap) {
        return value(gap.value());
    }

    /** 값이 없으면 0 이 아니라 NaN 이다. 0 은 "정상인데 0" 과 구분되지 않는다. */
    private static double value(Long value) {
        return value == null ? Double.NaN : value;
    }

    private static double value(Double value) {
        return value == null ? Double.NaN : value;
    }

    private static double code(SourceStatus status) {
        return SourceStatusCode.of(status);
    }

    /** 수집 경로 하나의 성공·실패 이력. 미터로 나가는 것은 마지막 성공 시각뿐이다. */
    private static final class CollectOutcome {

        private final String path;
        private final AtomicReference<Long> lastSuccessEpoch = new AtomicReference<>();
        private final AtomicInteger consecutiveFailures = new AtomicInteger();

        private CollectOutcome(String path) {
            this.path = path;
        }
    }

    /** issuances 집계 한 번에서 나온 값 묶음. 통째로 교체해 서로 다른 시점이 섞이지 않게 한다. */
    private record ConsistencyState(
        Long couponId,
        Map<ConsistencyGapType, GapValue> gaps,
        GapValue overIssued,
        Instant lastSuccessfulIssueAt,
        SourceStatus lastSuccessfulIssueStatus,
        Double sourceSkewSeconds,
        SourceStatus comparableStatus,
        SourceStatus judgeableStatus,
        Severity severity
    ) {

        static ConsistencyState of(DomainRawSnapshot snapshot, ConsistencyEvaluation evaluation) {
            return new ConsistencyState(
                snapshot.couponId(),
                new EnumMap<>(evaluation.gaps()),
                evaluation.overIssued(),
                snapshot.lastSuccessfulIssueAt(),
                snapshot.lastSuccessfulIssueStatus(),
                skewSeconds(snapshot),
                comparableStatus(snapshot),
                judgeableStatus(snapshot),
                evaluation.severity()
            );
        }

        /**
         * <b>두 원천을 비교할 수 있는가</b> — 시차가 없다고 나올 때의 이유다.
         *
         * <p>없는 원천(N_A)이 먼저다. V1 은 Redis 가 아예 없어 비교 대상이 없는 것이지, DB 가
         * 죽어서 못 재는 것이 아니다.
         */
        private static SourceStatus comparableStatus(DomainRawSnapshot snapshot) {
            return firstOf(snapshot,
                SourceStatus.N_A, SourceStatus.UNAVAILABLE, SourceStatus.PENDING, SourceStatus.STALE);
        }

        /**
         * <b>판정을 내릴 수 있는가</b> — 심각도가 없다고 나올 때의 이유다.
         *
         * <p>순서가 시차와 반대다. V1 은 Redis 계열이 N_A 여도 DB 안에서 닫히는 대조로 판정할 수
         * 있다. 그래서 판정이 안 나왔다면 원인은 N_A 가 아니라 <b>장애나 대기</b> 쪽이다.
         */
        private static SourceStatus judgeableStatus(DomainRawSnapshot snapshot) {
            return firstOf(snapshot,
                SourceStatus.UNAVAILABLE, SourceStatus.PENDING, SourceStatus.STALE, SourceStatus.N_A);
        }

        private static SourceStatus firstOf(DomainRawSnapshot snapshot, SourceStatus... priority) {
            SourceStatus redis = snapshot.consistency().redisObservation().status();
            SourceStatus database = snapshot.consistency().databaseObservation().status();
            for (SourceStatus candidate : priority) {
                if (redis == candidate || database == candidate) {
                    return candidate;
                }
            }
            return SourceStatus.VALID;
        }

        Long severityCode() {
            return severity == null ? null : (long) SourceStatusCode.of(severity);
        }

        /** 심각도를 냈으면 그 값이 유효하다는 뜻이다. 못 냈으면 판정 불가 이유가 그 자리에 온다. */
        SourceStatus severityStatus() {
            return severity != null ? SourceStatus.VALID : judgeableStatus;
        }

        /**
         * 두 원천 모두 관측 시각이 있을 때만 잰다. 한쪽이 N_A·UNAVAILABLE 이면 잴 대상이 없다.
         *
         * <p>초 단위로 자르지 않는다. Redis 를 DB 집계 직전에 읽으므로 실제 시차는 대부분 1초
         * 미만이고, 잘라 버리면 미터가 언제나 0 이라 "시차 없음" 으로 오독된다.
         */
        private static Double skewSeconds(DomainRawSnapshot snapshot) {
            Instant redisAt = snapshot.consistency().redisObservation().observedAt();
            Instant databaseAt = snapshot.consistency().databaseObservation().observedAt();
            if (redisAt == null || databaseAt == null) {
                return null;
            }
            return Duration.between(redisAt, databaseAt).toMillis() / 1000.0;
        }

        /** 첫 수집 전. 값이 아직 없는 것이지 0 인 것이 아니다. */
        static ConsistencyState pending(EngineVersion engineVersion) {
            return uniform(SourceStatus.PENDING, engineVersion);
        }

        static ConsistencyState unavailable(EngineVersion engineVersion) {
            return uniform(SourceStatus.UNAVAILABLE, engineVersion);
        }

        /**
         * 없는 원천은 어떤 상태에서도 N_A 다. V1 에는 Redis 가 없으므로 Redis 를 항으로 쓰는 gap 은
         * 수집이 실패해도 "장애" 가 아니라 "해당 없음" 이다 — 존재하지 않는 원자성 위반 경보가 뜬다.
         *
         * <p>어느 gap 이 Redis 를 쓰는지는 {@code DefaultConsistencyCalculator} 가 정한다. 여기 목록이
         * 그것과 갈라지면 실패 구간에서만 다른 상태가 나가므로, {@code DomainGaugeRegistrarTest} 가
         * 두 집합을 대조한다.
         */
        private static ConsistencyState uniform(SourceStatus status, EngineVersion engineVersion) {
            GapValue value = new GapValue(null, status, null);
            GapValue notApplicable = new GapValue(null, SourceStatus.N_A, null);
            Map<ConsistencyGapType, GapValue> gaps = new EnumMap<>(ConsistencyGapType.class);
            for (ConsistencyGapType gapType : ConsistencyGapType.values()) {
                gaps.put(gapType, usesRedis(gapType) && engineVersion == EngineVersion.V1
                    ? notApplicable
                    : value);
            }
            return new ConsistencyState(null, gaps, value, null, status, null, status, status, null);
        }

        private static boolean usesRedis(ConsistencyGapType gapType) {
            return gapType != ConsistencyGapType.DB_COUNTER_GAP;
        }

        Long lastSuccessfulIssueEpochSeconds() {
            return lastSuccessfulIssueAt == null ? null : lastSuccessfulIssueAt.getEpochSecond();
        }
    }

    private record StockState(
        Long couponId,
        Long stockRemaining,
        SourceStatus stockStatus,
        Long queueLength,
        SourceStatus queueStatus
    ) {

        static StockState of(StockSnapshot snapshot) {
            return new StockState(
                snapshot.couponId(),
                snapshot.stockRemaining(), snapshot.stockStatus(),
                snapshot.queueLength(), snapshot.queueStatus());
        }

        static StockState pending(EngineVersion engineVersion) {
            return uniform(SourceStatus.PENDING, engineVersion);
        }

        static StockState unavailable(EngineVersion engineVersion) {
            return uniform(SourceStatus.UNAVAILABLE, engineVersion);
        }

        /** V1 에는 대기열이 없다. 리더와 같은 규칙을 registrar 도 따른다. */
        private static StockState uniform(SourceStatus status, EngineVersion engineVersion) {
            return new StockState(null, null, status, null,
                engineVersion == EngineVersion.V1 ? SourceStatus.N_A : status);
        }
    }
}
