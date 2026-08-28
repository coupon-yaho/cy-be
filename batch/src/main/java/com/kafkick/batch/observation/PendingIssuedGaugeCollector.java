package com.kafkick.batch.observation;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import com.kafkick.core.coupon.v2.IssuedValue;
import com.kafkick.core.coupon.v2.IssuedValueCodec;
import com.kafkick.core.coupon.v2.IssuedValueCorruptException;
import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.observation.SourceStatusCode;
import com.kafkick.core.support.TimeProvider;

/** FINAL 전 benchmark run의 v2 issued Hash를 회차별로 읽어 PENDING과 파손을 계측한다. */
public class PendingIssuedGaugeCollector {

    private static final Logger log = LoggerFactory.getLogger(PendingIssuedGaugeCollector.class);
    // coupon_id 에는 유일 제약이 없다. uk_run_running 이 묶는 것은 RUNNING 하나뿐이라
    // LOAD_STOPPED·OBSERVED 는 몇 개든 남는다. 회차당 한 행으로 접지 않으면 뒤늦은
    // WARMUP(V1) 행이 진행 중인 MAIN(V2) 의 결과를 N_A 로 덮는다. 최신 판정은 id 다.
    static final String OBSERVABLE_RUNS_SQL = """
        SELECT r.coupon_id, r.engine_version
          FROM benchmark_runs r
          JOIN (SELECT coupon_id, MAX(id) AS id
                  FROM benchmark_runs
                 WHERE run_status <> 'FINALIZED'
                   AND coupon_id IS NOT NULL
                 GROUP BY coupon_id) latest ON latest.id = r.id
         ORDER BY r.coupon_id
        """;

    private final JdbcTemplate observationJdbcTemplate;
    private final StringRedisTemplate redisTemplate;
    private final PendingIssuedGaugeProperties properties;
    private final MeterRegistry meterRegistry;
    private final TimeProvider timeProvider;
    private final IssuedValueCodec codec = new IssuedValueCodec();
    private final Map<Long, RoundMeters> meters = new ConcurrentHashMap<>();
    private volatile Set<Long> activeRoundIds = Set.of();
    private volatile Instant lastCycleStartedAt;
    private volatile Instant lastCycleEndedAt;
    private volatile Duration lastCycleDuration = Duration.ZERO;

    public PendingIssuedGaugeCollector(
        @Qualifier("obs") JdbcTemplate observationJdbcTemplate,
        StringRedisTemplate redisTemplate,
        PendingIssuedGaugeProperties properties,
        MeterRegistry meterRegistry,
        TimeProvider timeProvider
    ) {
        this.observationJdbcTemplate = observationJdbcTemplate;
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.meterRegistry = meterRegistry;
        this.timeProvider = timeProvider;
    }

    public void collect() {
        Instant startedAt = timeProvider.instant();
        lastCycleStartedAt = startedAt;
        try {
            collectOnce();
        } finally {
            Instant endedAt = timeProvider.instant();
            lastCycleDuration = Duration.between(startedAt, endedAt);
            lastCycleEndedAt = endedAt;
        }
    }

    private void collectOnce() {
        List<Target> targets;
        try {
            targets = targets();
        } catch (RuntimeException exception) {
            activeRoundIds.forEach(roundId -> {
                RoundMeters round = meters.get(roundId);
                if (round != null) {
                    round.update(Observation.unavailable(timeProvider.instant()));
                }
            });
            log.warn("PENDING 계측 대상 benchmark run 조회에 실패했다. 활성 회차 값을 비운다.", exception);
            return;
        }

        Set<Long> nextActiveRoundIds = new HashSet<>();
        targets.forEach(target -> nextActiveRoundIds.add(target.roundId()));
        activeRoundIds = Set.copyOf(nextActiveRoundIds);
        for (Target target : targets) {
            // 한 회차의 실패가 뒤 회차를 가리지 않게 회차 단위로 가둔다. 여기서 예외가 새면
            // 뒤에 오는 회차는 매 사이클 통째로 미수집이 된다.
            try {
                collect(target);
            } catch (RuntimeException exception) {
                log.warn("회차 {}의 PENDING 계측을 건너뛴다.", target.roundId(), exception);
            }
        }
        retireDepartedRounds();
    }

    private void collect(Target target) {
        long roundId = target.roundId();
        RoundMeters round = meters.computeIfAbsent(roundId, this::register);
        EngineVersion engine = target.engineVersion();
        if (engine == null) {
            // 원천을 못 읽은 것이지 값이 없는 것이 아니다.
            round.update(Observation.unavailable(timeProvider.instant()));
            log.warn("회차 {}의 engine_version 을 읽을 수 없다. 값을 비운다.", roundId);
            return;
        }
        switch (engine) {
            // V1 에는 Redis 가 없다. 값이 있을 수 없으므로 N_A 다.
            case V1 -> round.update(Observation.notApplicable(timeProvider.instant()));
            case V2 -> collectV2(roundId, round);
            // v3 는 아직 이 계측의 키 규약이 없다. 없다고 단정하지 않고 PENDING 으로 둔다.
            case V3 -> round.update(Observation.pending(timeProvider.instant()));
        }
    }

    /**
     * 대상에서 빠진 회차의 미터를 유예 뒤 내린다. 유예가 없으면 종료 시점의 잔여 PENDING 이
     * 스크레이프되기 전에 사라지고, 유예가 없으면 죽은 회차의 UNAVAILABLE 이 영원히 남는다.
     */
    private void retireDepartedRounds() {
        Instant now = timeProvider.instant();
        Duration retention = properties.interval().multipliedBy(4);
        meters.entrySet().removeIf(entry -> {
            if (activeRoundIds.contains(entry.getKey())) {
                return false;
            }
            Instant observedAt = entry.getValue().observation.get().observedAt();
            if (Duration.between(observedAt, now).compareTo(retention) <= 0) {
                return false;
            }
            retire(entry.getKey());
            return true;
        });
    }

    private void retire(long roundId) {
        String tag = Long.toString(roundId);
        List.of(
            DomainMeterNames.STALE_PENDING_COUNT,
            DomainMeterNames.STALE_PENDING_COUNT_STATE,
            DomainMeterNames.CORRUPT_FIELD_COUNT,
            DomainMeterNames.CORRUPT_FIELD_COUNT_STATE
        ).forEach(name -> {
            Gauge gauge = meterRegistry.find(name)
                .tag(DomainMeterNames.TAG_COUPON_ROUND_ID, tag).gauge();
            if (gauge != null) {
                meterRegistry.remove(gauge);
            }
        });
    }

    private List<Target> targets() {
        return observationJdbcTemplate.queryForList(OBSERVABLE_RUNS_SQL).stream()
            .map(row -> new Target(
                ((Number) row.get("coupon_id")).longValue(),
                engineVersion(row.get("engine_version"))))
            .toList();
    }

    /** 한 행의 알 수 없는 값이 대상 조회 전체를 실패로 만들지 않게 여기서 흡수한다. */
    private static EngineVersion engineVersion(Object raw) {
        try {
            return EngineVersion.valueOf(String.valueOf(raw));
        } catch (IllegalArgumentException exception) {
            return null;
        }
    }

    private void collectV2(long roundId, RoundMeters round) {
        if (redisTemplate == null) {
            round.update(Observation.unavailable(timeProvider.instant()));
            log.warn("V2 PENDING 계측 대상 회차 {}가 있지만 Redis 통로가 없다.", roundId);
            return;
        }
        try {
            Counts counts = scan(roundId);
            round.update(Observation.valid(counts, timeProvider.instant()));
            if (counts.corrupt() > 0) {
                // field는 memberId라 로그에 남기지 않는다. 회차와 건수만 조사 시작점으로 준다.
                log.warn("V2 issued 파손 field가 있다: couponRoundId={}, count={}",
                    roundId, counts.corrupt());
            }
        } catch (RuntimeException exception) {
            round.update(Observation.unavailable(timeProvider.instant()));
            log.warn("회차 {}의 v2 issued 계측에 실패했다. 값을 비운다.", roundId, exception);
        }
    }

    private Counts scan(long roundId) {
        long stale = 0;
        long corrupt = 0;
        long nowEpochMillis = timeProvider.instant().toEpochMilli();
        ScanOptions options = ScanOptions.scanOptions().count(properties.scanCount()).build();
        try (Cursor<Map.Entry<Object, Object>> cursor = redisTemplate.opsForHash()
            .scan(properties.issuedKey(roundId), options)) {
            while (cursor.hasNext()) {
                Object raw = cursor.next().getValue();
                try {
                    IssuedValue value = codec.decode(raw instanceof String string ? string : null);
                    if (value.status() == IssuedValue.Status.PENDING
                        && nowEpochMillis - value.claimedAtEpochMillis()
                            > properties.staleAfter().toMillis()) {
                        stale++;
                    }
                } catch (IssuedValueCorruptException exception) {
                    corrupt++;
                }
            }
        }
        return new Counts(stale, corrupt);
    }

    private RoundMeters register(long roundId) {
        try {
            return registerGauges(roundId);
        } catch (RuntimeException exception) {
            // 반쪽 등록을 남기면 살아남은 게이지가 버려진 관측을 영원히 읽는다. Micrometer 는
            // 같은 Meter.Id 재등록 시 기존 미터를 돌려주므로 다음 사이클에도 복구되지 않는다.
            retire(roundId);
            throw exception;
        }
    }

    private RoundMeters registerGauges(long roundId) {
        RoundMeters round = new RoundMeters(timeProvider.instant());
        String tag = Long.toString(roundId);
        gauge(DomainMeterNames.STALE_PENDING_COUNT, tag,
            () -> value(current(roundId, round).stalePending()));
        gauge(DomainMeterNames.STALE_PENDING_COUNT_STATE, tag,
            () -> SourceStatusCode.of(current(roundId, round).status()));
        gauge(DomainMeterNames.CORRUPT_FIELD_COUNT, tag,
            () -> value(current(roundId, round).corrupt()));
        gauge(DomainMeterNames.CORRUPT_FIELD_COUNT_STATE, tag,
            () -> SourceStatusCode.of(current(roundId, round).status()));
        return round;
    }

    /**
     * 대상에서 빠진 회차는 갱신이 멈추므로, 마지막 관측이 수집 주기 두 번을 넘기면 VALID 를 거둔다.
     * 값이 아직 참인지 알 수 없는 구간을 VALID 로 두면 소비자가 신선도를 판정할 수 없다.
     *
     * <p>대상에 남아 있는 회차는 나이로 재지 않는다. 갱신 간격은 {@code interval} 이 아니라
     * {@code interval + 사이클 소요시간} 이라, 회차가 늘어 한 바퀴가 길어지면 성공한 수집이
     * 노후로 뒤집힌다. 읽기가 실패한 회차는 이미 그 자리에서 UNAVAILABLE 로 적힌다.
     */
    private Observation current(long roundId, RoundMeters round) {
        Observation observation = round.observation.get();
        if (observation.status() != SourceStatus.VALID) {
            return observation;
        }
        if (activeRoundIds.contains(roundId)) {
            // 대상에 남아 있다는 사실은 "수집이 살아 있다" 를 뜻하지 않는다. 회차 나이 대신
            // 사이클이 다시 돌았는지로 판정한다 — 한 바퀴가 길어져도 오탐하지 않으면서
            // 수집이 멈춘 것은 잡는다.
            Instant startedAt = lastCycleStartedAt;
            Instant endedAt = lastCycleEndedAt;
            if (endedAt == null || (startedAt != null && startedAt.isAfter(endedAt))) {
                // 사이클이 지금 돌고 있다. 허용치는 직전 바퀴 길이로 잰 것이라, 부하가 실려
                // 이번 바퀴가 갑자기 길어지면 방금 읽은 회차까지 노후로 뒤집는다.
                return observation;
            }
            Instant freshest = observation.observedAt().isAfter(endedAt)
                ? observation.observedAt()
                : endedAt;
            Duration allowed = properties.interval().plus(lastCycleDuration).multipliedBy(2);
            return Duration.between(freshest, timeProvider.instant()).compareTo(allowed) > 0
                ? Observation.unavailable(observation.observedAt())
                : observation;
        }
        Duration age = Duration.between(observation.observedAt(), timeProvider.instant());
        return age.compareTo(properties.interval().multipliedBy(2)) > 0
            ? Observation.unavailable(observation.observedAt())
            : observation;
    }

    private void gauge(String name, String roundId, java.util.function.Supplier<Number> supplier) {
        Gauge.builder(name, supplier)
            .tag(DomainMeterNames.TAG_COUPON_ROUND_ID, roundId)
            .strongReference(true)
            .register(meterRegistry);
    }

    private static double value(Long value) {
        return value == null ? Double.NaN : value;
    }

    private static final class RoundMeters {
        private final AtomicReference<Observation> observation;

        private RoundMeters(Instant registeredAt) {
            this.observation = new AtomicReference<>(Observation.pending(registeredAt));
        }

        private void update(Observation next) {
            observation.set(next);
        }
    }

    private record Observation(
        Long stalePending, Long corrupt, SourceStatus status, Instant observedAt
    ) {
        private static Observation pending(Instant observedAt) {
            return new Observation(null, null, SourceStatus.PENDING, observedAt);
        }
        private static Observation unavailable(Instant observedAt) {
            return new Observation(null, null, SourceStatus.UNAVAILABLE, observedAt);
        }
        private static Observation notApplicable(Instant observedAt) {
            return new Observation(null, null, SourceStatus.N_A, observedAt);
        }
        private static Observation valid(Counts counts, Instant observedAt) {
            return new Observation(
                counts.stalePending(), counts.corrupt(), SourceStatus.VALID, observedAt);
        }
    }

    private record Counts(long stalePending, long corrupt) { }
    private record Target(long roundId, EngineVersion engineVersion) { }
}
