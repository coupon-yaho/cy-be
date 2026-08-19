package com.kafkick.core.consistency;

import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.exception.BusinessException;

import java.time.Instant;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * 정합성 gap 4종과 독립 KPI인 초과 발급 수를 계산하는 클래스입니다.
 *
 * <p>이 클래스는 Redis, DB, Prometheus, Micrometer에 의존하지 않는 순수 계산 도메인입니다.
 * 모든 뺄셈에는 overflow 검출을 적용하며, 계산할 수 없는 원천 상태를 숫자 0으로 바꾸지 않습니다.
 * V1은 Redis 의존 gap을 제외하고, V2·V3은 LIVE 동작 특성에 맞게 서로 다른 severity를 적용합니다.
 */
public final class DefaultConsistencyCalculator implements ConsistencyCalculator {

    private static final GapValue NOT_APPLICABLE = new GapValue(null, SourceStatus.N_A, null);
    private static final GapValue UNAVAILABLE = new GapValue(null, SourceStatus.UNAVAILABLE, null);
    private static final GapValue PENDING = new GapValue(null, SourceStatus.PENDING, null);
    private static final Set<SourceStatus> CONSISTENCY_SOURCE_STATES = Set.of(
            SourceStatus.VALID,
            SourceStatus.PENDING,
            SourceStatus.STALE,
            SourceStatus.UNAVAILABLE,
            SourceStatus.N_A
    );

    private final ConsistencySeverityPolicy severityPolicy;

    /**
     * 지정한 LIVE 드리프트 임계치로 계산기를 생성합니다.
     *
     * @param severityPolicy V2 LIVE의 WARN·CRITICAL 임계치 정책
     */
    public DefaultConsistencyCalculator(ConsistencySeverityPolicy severityPolicy) {
        this.severityPolicy = Objects.requireNonNull(severityPolicy, "severityPolicy");
    }

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException snapshot, phase 또는 engineVersion이 null인 경우
     * @throws BusinessException 원천 상태가 부적절하거나 FINAL 값이 준비되지 않았거나 계산이 overflow한 경우
     */
    @Override
    public ConsistencyEvaluation evaluate(
            ConsistencyRawSnapshot snapshot,
            ConsistencyPhase phase,
            EngineVersion engineVersion
    ) {
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(phase, "phase");
        Objects.requireNonNull(engineVersion, "engineVersion");
        requireConsistencySourceState(snapshot.redisObservation());
        requireConsistencySourceState(snapshot.databaseObservation());

        ConsistencyRawValues values = snapshot.rawValues();
        Map<ConsistencyGapType, GapValue> gaps = new EnumMap<>(ConsistencyGapType.class);

        // V1 발급 경로에는 Redis가 없지만 DB 행 집계와 저장 카운터의 대조는 여전히 유효합니다.
        if (engineVersion == EngineVersion.V1) {
            gaps.put(ConsistencyGapType.ACTIVE_DB_GAP, NOT_APPLICABLE);
            gaps.put(ConsistencyGapType.LUA_GAP, NOT_APPLICABLE);
            gaps.put(ConsistencyGapType.PERSIST_GAP, NOT_APPLICABLE);
        } else {
            gaps.put(ConsistencyGapType.ACTIVE_DB_GAP, gap(
                    () -> Math.subtractExact(
                            Math.subtractExact(values.totalQuantity(), values.redisRemaining()),
                            values.dbActiveCount()
                    ),
                    snapshot.redisObservation(),
                    snapshot.databaseObservation()
            ));
            gaps.put(ConsistencyGapType.LUA_GAP, gap(
                    () -> Math.subtractExact(
                            values.redisIssuedEverCount(),
                            values.redisMemberEverCount()
                    ),
                    snapshot.redisObservation()
            ));
            gaps.put(ConsistencyGapType.PERSIST_GAP, gap(
                    () -> Math.subtractExact(
                            values.redisIssuedEverCount(),
                            values.dbIssuedEverCount()
                    ),
                    snapshot.redisObservation(),
                    snapshot.databaseObservation()
            ));
        }

        gaps.put(ConsistencyGapType.DB_COUNTER_GAP, gap(
                () -> Math.subtractExact(values.dbActiveCount(), values.storedActiveCount()),
                snapshot.databaseObservation()
        ));
        GapValue overIssued = gap(
                () -> overIssued(values),
                snapshot.databaseObservation()
        );

        // lag·quiet period·권위 DB COUNT 같은 FINAL 진입 게이트는 호출자가 확인합니다.
        if (phase == ConsistencyPhase.FINAL) {
            requireFinalValues(gaps, overIssued, engineVersion);
            boolean failed = hasFinalMismatch(gaps, overIssued, engineVersion);
            return new ConsistencyEvaluation(
                    gaps,
                    overIssued,
                    ConsistencyPhase.FINAL,
                    failed ? Verdict.FAIL : Verdict.PASS,
                    failed ? Severity.CRITICAL : Severity.NONE
            );
        }

        return new ConsistencyEvaluation(
                gaps,
                overIssued,
                ConsistencyPhase.LIVE,
                null,
                liveSeverity(gaps, overIssued, engineVersion)
        );
    }

    /**
     * DB 활성 쿠폰이 총 발급 수량을 초과한 개수를 계산합니다.
     *
     * @param values DB 활성 수와 총 발급 수량을 포함한 원천값
     * @return 초과 발급 수; 초과하지 않았으면 0
     * @throws ArithmeticException 초과분이 {@code long} 범위를 벗어나는 경우
     */
    private static long overIssued(ConsistencyRawValues values) {
        if (values.dbActiveCount() <= values.totalQuantity()) {
            return 0;
        }
        return Math.subtractExact(values.dbActiveCount(), values.totalQuantity());
    }

    /**
     * FINAL 판정에 필요한 모든 값이 현재 시점에 유효한지 검증합니다.
     *
     * @param gaps 계산된 네 종류의 gap
     * @param overIssued 계산된 초과 발급 수
     * @param engineVersion 적용할 gap을 결정하는 발급 엔진 버전
     * @throws BusinessException 적용 가능한 값 중 하나라도 VALID가 아닌 경우
     */
    private static void requireFinalValues(
            Map<ConsistencyGapType, GapValue> gaps,
            GapValue overIssued,
            EngineVersion engineVersion
    ) {
        if (overIssued.state() != SourceStatus.VALID) {
            throw new BusinessException(
                    ConsistencyErrorCode.FINAL_VALUE_UNAVAILABLE,
                    "FINAL 평가에 필요한 overIssued가 유효하지 않습니다: " + overIssued.state()
            );
        }
        for (Map.Entry<ConsistencyGapType, GapValue> entry : gaps.entrySet()) {
            if (isApplicable(entry.getKey(), engineVersion)
                    && entry.getValue().state() != SourceStatus.VALID) {
                throw new BusinessException(
                        ConsistencyErrorCode.FINAL_VALUE_UNAVAILABLE,
                        "FINAL 평가에 필요한 gap이 유효하지 않습니다: "
                                + entry.getKey() + ", state=" + entry.getValue().state()
                );
            }
        }
    }

    /**
     * FINAL 결과를 실패로 만드는 불일치가 존재하는지 확인합니다.
     *
     * @param gaps 계산된 네 종류의 gap
     * @param overIssued 계산된 초과 발급 수
     * @param engineVersion 적용할 gap을 결정하는 발급 엔진 버전
     * @return 초과 발급이 있거나 적용 가능한 gap이 하나라도 0이 아니면 {@code true}
     */
    private static boolean hasFinalMismatch(
            Map<ConsistencyGapType, GapValue> gaps,
            GapValue overIssued,
            EngineVersion engineVersion
    ) {
        if (overIssued.value() > 0) {
            return true;
        }
        return gaps.entrySet().stream()
                .filter(entry -> isApplicable(entry.getKey(), engineVersion))
                .anyMatch(entry -> entry.getValue().value() != 0);
    }

    /**
     * 발급 엔진 버전에서 지정한 gap을 평가하는지 확인합니다.
     *
     * @param gapType 확인할 gap 종류
     * @param engineVersion 발급 엔진 버전
     * @return 평가 대상이면 {@code true}
     */
    private static boolean isApplicable(ConsistencyGapType gapType, EngineVersion engineVersion) {
        return engineVersion != EngineVersion.V1 || gapType == ConsistencyGapType.DB_COUNTER_GAP;
    }

    /**
     * 현재 계산 가능한 LIVE 값 중 가장 높은 운영 심각도를 선택합니다.
     *
     * @param gaps 계산된 네 종류의 gap
     * @param overIssued 계산된 초과 발급 수
     * @param engineVersion LIVE 심각도 정책을 결정하는 발급 엔진 버전
     * @return 최고 심각도; 계산 가능한 값이 없으면 {@code null}
     */
    private Severity liveSeverity(
            Map<ConsistencyGapType, GapValue> gaps,
            GapValue overIssued,
            EngineVersion engineVersion
    ) {
        Severity severity = null;

        // STALE을 포함한 평가 불가 값은 마지막 값으로 표시할 수 있어도 현재 위험도에는 합산하지 않습니다.
        if (overIssued.state() == SourceStatus.VALID) {
            severity = Severity.NONE;
            if (overIssued.value() > 0) {
                severity = Severity.CRITICAL;
            }
        }

        for (Map.Entry<ConsistencyGapType, GapValue> entry : gaps.entrySet()) {
            GapValue gap = entry.getValue();
            if (gap.state() != SourceStatus.VALID) {
                continue;
            }
            severity = moreSevere(severity, severityForGap(entry.getKey(), gap.value(), engineVersion));
        }
        return severity;
    }

    /**
     * gap 하나를 종류와 엔진 버전에 맞는 LIVE 심각도로 변환합니다.
     *
     * @param gapType gap 종류
     * @param value signed gap 값
     * @param engineVersion LIVE 심각도 정책을 결정하는 발급 엔진 버전
     * @return 해당 gap의 운영 심각도
     */
    private Severity severityForGap(
            ConsistencyGapType gapType,
            long value,
            EngineVersion engineVersion
    ) {
        if (value == 0) {
            return Severity.NONE;
        }
        if (gapType == ConsistencyGapType.LUA_GAP
                || gapType == ConsistencyGapType.DB_COUNTER_GAP) {
            // 두 gap은 동기화 지연이 아니라 각각 Redis 원자성과 DB 내부 불변식의 위반입니다.
            return Severity.CRITICAL;
        }
        if (engineVersion == EngineVersion.V3) {
            // V3의 Redis↔DB 차이는 비동기 영속화 중 자연스럽게 발생하므로 LIVE에서는 WARN입니다.
            return Severity.WARN;
        }
        if (engineVersion == EngineVersion.V2) {
            // V2 임계치는 운영 경보 수준만 조정하며 FINAL의 0 기준을 완화하지 않습니다.
            if (reachesMagnitude(value, severityPolicy.criticalThreshold())) {
                return Severity.CRITICAL;
            }
            if (reachesMagnitude(value, severityPolicy.warnThreshold())) {
                return Severity.WARN;
            }
        }
        return Severity.NONE;
    }

    /**
     * signed 값의 절댓값이 임계치 이상인지 overflow 없이 확인합니다.
     *
     * @param value 확인할 signed 값
     * @param threshold 양수 임계치
     * @return 양수 또는 음수 방향으로 임계치에 도달했으면 {@code true}
     */
    private static boolean reachesMagnitude(long value, long threshold) {
        return value >= threshold || value <= -threshold;
    }

    /**
     * 두 심각도 중 더 높은 값을 선택합니다.
     *
     * @param current 지금까지 계산된 심각도; 아직 값이 없으면 {@code null}
     * @param candidate 새로 계산한 심각도
     * @return 더 높은 심각도
     */
    private static Severity moreSevere(Severity current, Severity candidate) {
        if (current == null || candidate.ordinal() > current.ordinal()) {
            return candidate;
        }
        return current;
    }

    /**
     * 필요한 원천들의 상태를 합성하고, 계산 가능할 때만 실제 gap 산식을 실행합니다.
     *
     * @param valueSupplier 원천값이 계산 가능한 경우 실행할 signed gap 산식
     * @param observations 산식에 필요한 원천별 상태와 관측 시각
     * @return 상태, 값, 가장 오래된 관측 시각을 포함한 gap
     * @throws BusinessException 산식 계산이 {@code long} 범위를 벗어나는 경우
     */
    private static GapValue gap(LongSupplier valueSupplier, SourceObservation... observations) {
        // 값이 없는 상태를 먼저 확정해야 placeholder 원시값의 overflow나 가짜 0 계산을 피할 수 있습니다.
        if (hasStatus(observations, SourceStatus.N_A)) {
            return NOT_APPLICABLE;
        }
        if (hasStatus(observations, SourceStatus.UNAVAILABLE)) {
            return UNAVAILABLE;
        }
        if (hasStatus(observations, SourceStatus.PENDING)) {
            return PENDING;
        }

        SourceStatus state = hasStatus(observations, SourceStatus.STALE)
                ? SourceStatus.STALE
                : SourceStatus.VALID;
        Instant observedAt = Arrays.stream(observations)
                .map(SourceObservation::observedAt)
                .min(Instant::compareTo)
                .orElseThrow();
        try {
            return new GapValue(valueSupplier.getAsLong(), state, observedAt);
        } catch (ArithmeticException exception) {
            throw new BusinessException(
                    ConsistencyErrorCode.CALCULATION_OVERFLOW,
                    "정합성 gap 계산 중 long 범위를 초과했습니다.",
                    exception
            );
        }
    }

    /**
     * 원천 목록에 지정한 상태가 하나라도 포함되는지 확인합니다.
     *
     * @param observations 확인할 원천 목록
     * @param status 찾을 원천 상태
     * @return 같은 상태가 존재하면 {@code true}
     */
    private static boolean hasStatus(SourceObservation[] observations, SourceStatus status) {
        return Arrays.stream(observations).anyMatch(observation -> observation.status() == status);
    }

    /**
     * 원천 상태가 정합성 gap의 상태 모델로 변환 가능한지 검증합니다.
     *
     * @param observation 검증할 원천 관측 정보
     * @throws BusinessException 정합성 계산에서 지원하지 않는 상태인 경우
     */
    private static void requireConsistencySourceState(SourceObservation observation) {
        if (!CONSISTENCY_SOURCE_STATES.contains(observation.status())) {
            throw new BusinessException(
                    ConsistencyErrorCode.INVALID_SOURCE_STATE,
                    "정합성 원천에 사용할 수 없는 상태입니다: " + observation.status()
            );
        }
    }
}
