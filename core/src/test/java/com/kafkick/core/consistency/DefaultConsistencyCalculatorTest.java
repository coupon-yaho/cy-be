package com.kafkick.core.consistency;

import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.exception.BusinessException;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultConsistencyCalculatorTest {

    private static final Instant REDIS_OBSERVED_AT = Instant.parse("2026-08-19T01:00:00Z");
    private static final Instant DATABASE_OBSERVED_AT = Instant.parse("2026-08-19T01:00:01Z");

    private final DefaultConsistencyCalculator calculator = new DefaultConsistencyCalculator(
            ConsistencySeverityPolicy.defaults()
    );

    @Test
    void calculatesAllSignedGapsFromTheDefinedFormulas() {
        ConsistencyRawValues values = new ConsistencyRawValues(
                100,
                40,
                65,
                64,
                58,
                63,
                60
        );

        ConsistencyEvaluation result = calculator.evaluate(
                snapshot(values, SourceStatus.VALID, SourceStatus.VALID),
                ConsistencyPhase.LIVE,
                EngineVersion.V2
        );

        assertThat(result.gaps().get(ConsistencyGapType.ACTIVE_DB_GAP).value()).isEqualTo(2);
        assertThat(result.gaps().get(ConsistencyGapType.LUA_GAP).value()).isEqualTo(1);
        assertThat(result.gaps().get(ConsistencyGapType.PERSIST_GAP).value()).isEqualTo(2);
        assertThat(result.gaps().get(ConsistencyGapType.DB_COUNTER_GAP).value()).isEqualTo(-2);
        assertThat(result.overIssued().value()).isZero();
    }

    @Test
    void calculatesOverIssuedFromDatabaseActiveCount() {
        ConsistencyRawValues values = new ConsistencyRawValues(
                10,
                -2,
                12,
                12,
                12,
                12,
                12
        );

        ConsistencyEvaluation result = calculator.evaluate(
                snapshot(values, SourceStatus.VALID, SourceStatus.VALID),
                ConsistencyPhase.LIVE,
                EngineVersion.V2
        );

        assertThat(result.overIssued().value()).isEqualTo(2);
    }

    @Test
    void disablesOnlyRedisDependentGapsForV1() {
        ConsistencyRawValues values = new ConsistencyRawValues(
                100,
                40,
                65,
                64,
                58,
                63,
                60
        );

        ConsistencyEvaluation result = calculator.evaluate(
                snapshot(values, SourceStatus.N_A, SourceStatus.VALID),
                ConsistencyPhase.LIVE,
                EngineVersion.V1
        );

        assertNotApplicable(result.gaps().get(ConsistencyGapType.ACTIVE_DB_GAP));
        assertNotApplicable(result.gaps().get(ConsistencyGapType.LUA_GAP));
        assertNotApplicable(result.gaps().get(ConsistencyGapType.PERSIST_GAP));
        assertThat(result.gaps().get(ConsistencyGapType.DB_COUNTER_GAP).value()).isEqualTo(-2);
        assertThat(result.overIssued().value()).isZero();
    }

    @Test
    void assignsObservationTimeFromEverySourceUsedByEachGap() {
        ConsistencyRawValues values = new ConsistencyRawValues(100, 40, 65, 64, 58, 63, 60);

        ConsistencyEvaluation result = calculator.evaluate(
                snapshot(values, SourceStatus.VALID, SourceStatus.VALID),
                ConsistencyPhase.LIVE,
                EngineVersion.V2
        );

        assertThat(result.gaps().get(ConsistencyGapType.ACTIVE_DB_GAP).observedAt())
                .isEqualTo(REDIS_OBSERVED_AT);
        assertThat(result.gaps().get(ConsistencyGapType.LUA_GAP).observedAt())
                .isEqualTo(REDIS_OBSERVED_AT);
        assertThat(result.gaps().get(ConsistencyGapType.PERSIST_GAP).observedAt())
                .isEqualTo(REDIS_OBSERVED_AT);
        assertThat(result.gaps().get(ConsistencyGapType.DB_COUNTER_GAP).observedAt())
                .isEqualTo(DATABASE_OBSERVED_AT);
        assertThat(result.overIssued().observedAt()).isEqualTo(DATABASE_OBSERVED_AT);
    }

    @Test
    void propagatesUnavailableAndPendingWithoutInventingZeroValues() {
        ConsistencyRawValues values = new ConsistencyRawValues(100, 40, 65, 64, 58, 63, 60);

        ConsistencyEvaluation result = calculator.evaluate(
                snapshot(values, SourceStatus.UNAVAILABLE, SourceStatus.PENDING),
                ConsistencyPhase.LIVE,
                EngineVersion.V2
        );

        assertUnavailable(result.gaps().get(ConsistencyGapType.ACTIVE_DB_GAP));
        assertUnavailable(result.gaps().get(ConsistencyGapType.LUA_GAP));
        assertUnavailable(result.gaps().get(ConsistencyGapType.PERSIST_GAP));
        assertPending(result.gaps().get(ConsistencyGapType.DB_COUNTER_GAP));
        assertPending(result.overIssued());
    }

    @Test
    void preservesLastCalculatedValueForStaleSources() {
        ConsistencyRawValues values = new ConsistencyRawValues(100, 40, 65, 64, 58, 63, 60);

        ConsistencyEvaluation result = calculator.evaluate(
                snapshot(values, SourceStatus.STALE, SourceStatus.VALID),
                ConsistencyPhase.LIVE,
                EngineVersion.V2
        );

        GapValue activeDbGap = result.gaps().get(ConsistencyGapType.ACTIVE_DB_GAP);
        assertThat(activeDbGap.state()).isEqualTo(SourceStatus.STALE);
        assertThat(activeDbGap.value()).isEqualTo(2);
        assertThat(activeDbGap.observedAt()).isEqualTo(REDIS_OBSERVED_AT);
        assertThat(result.gaps().get(ConsistencyGapType.LUA_GAP).state()).isEqualTo(SourceStatus.STALE);
        assertThat(result.gaps().get(ConsistencyGapType.DB_COUNTER_GAP).state())
                .isEqualTo(SourceStatus.VALID);
    }

    @Test
    void pendingSourcePreventsACombinedGapFromUsingAnotherSourcesStaleValue() {
        ConsistencyRawValues values = new ConsistencyRawValues(100, 40, 65, 64, 58, 63, 60);

        ConsistencyEvaluation result = calculator.evaluate(
                snapshot(values, SourceStatus.STALE, SourceStatus.PENDING),
                ConsistencyPhase.LIVE,
                EngineVersion.V2
        );

        assertPending(result.gaps().get(ConsistencyGapType.ACTIVE_DB_GAP));
        assertPending(result.gaps().get(ConsistencyGapType.PERSIST_GAP));
    }

    @Test
    void rejectsSourceStatesThatHaveNoConsistencyGapMeaning() {
        ConsistencyRawValues values = new ConsistencyRawValues(100, 40, 65, 64, 58, 63, 60);

        assertThatThrownBy(() -> calculator.evaluate(
                snapshot(values, SourceStatus.WARMING_UP, SourceStatus.VALID),
                ConsistencyPhase.LIVE,
                EngineVersion.V2
        )).isInstanceOfSatisfying(BusinessException.class, exception ->
                assertThat(exception.getErrorCode())
                        .isEqualTo(ConsistencyErrorCode.INVALID_SOURCE_STATE));

        assertThatThrownBy(() -> calculator.evaluate(
                snapshot(values, SourceStatus.VALID, SourceStatus.NO_TRAFFIC),
                ConsistencyPhase.LIVE,
                EngineVersion.V2
        )).isInstanceOfSatisfying(BusinessException.class, exception ->
                assertThat(exception.getErrorCode())
                        .isEqualTo(ConsistencyErrorCode.INVALID_SOURCE_STATE));
    }

    @Test
    void appliesPrdThresholdsToV2LiveRedisDatabaseDriftByAbsoluteValue() {
        assertThat(evaluateV2Drift(9).severity()).isEqualTo(Severity.NONE);
        assertThat(evaluateV2Drift(10).severity()).isEqualTo(Severity.WARN);
        assertThat(evaluateV2Drift(99).severity()).isEqualTo(Severity.WARN);
        assertThat(evaluateV2Drift(100).severity()).isEqualTo(Severity.CRITICAL);
        assertThat(evaluateV2Drift(-10).severity()).isEqualTo(Severity.WARN);
        assertThat(evaluateV2Drift(-100).severity()).isEqualTo(Severity.CRITICAL);
    }

    @Test
    void treatsV2LiveInvariantViolationsAsCriticalWithoutThresholdRelaxation() {
        assertThat(evaluateLive(
                new ConsistencyRawValues(100, 50, 61, 60, 50, 61, 50),
                EngineVersion.V2
        ).severity()).isEqualTo(Severity.CRITICAL);

        assertThat(evaluateLive(
                new ConsistencyRawValues(100, 50, 60, 60, 50, 60, 49),
                EngineVersion.V2
        ).severity()).isEqualTo(Severity.CRITICAL);

        assertThat(evaluateLive(
                new ConsistencyRawValues(100, -1, 101, 101, 101, 101, 101),
                EngineVersion.V2
        ).severity()).isEqualTo(Severity.CRITICAL);
    }

    @Test
    void treatsV3LiveDriftAsWarningButInvariantViolationsAsCritical() {
        assertThat(evaluateLive(
                new ConsistencyRawValues(100, 49, 60, 60, 50, 59, 50),
                EngineVersion.V3
        ).severity()).isEqualTo(Severity.WARN);

        assertThat(evaluateLive(
                new ConsistencyRawValues(100, 50, 61, 60, 50, 61, 50),
                EngineVersion.V3
        ).severity()).isEqualTo(Severity.CRITICAL);
    }

    @Test
    void treatsV1DatabaseInvariantViolationAsCritical() {
        ConsistencyEvaluation result = calculator.evaluate(
                snapshot(
                        new ConsistencyRawValues(100, 0, 0, 0, 50, 0, 49),
                        SourceStatus.N_A,
                        SourceStatus.VALID
                ),
                ConsistencyPhase.LIVE,
                EngineVersion.V1
        );

        assertThat(result.severity()).isEqualTo(Severity.CRITICAL);
    }

    @Test
    void returnsNullSeverityWhenNoValueCanBeEvaluated() {
        ConsistencyEvaluation result = calculator.evaluate(
                snapshot(
                        new ConsistencyRawValues(100, 100, 0, 0, 0, 0, 0),
                        SourceStatus.PENDING,
                        SourceStatus.PENDING
                ),
                ConsistencyPhase.LIVE,
                EngineVersion.V2
        );

        assertThat(result.severity()).isNull();
    }

    @Test
    void excludesStaleValuesFromLiveSeverity() {
        ConsistencyEvaluation result = calculator.evaluate(
                snapshot(
                        new ConsistencyRawValues(100, 49, 61, 60, 50, 60, 50),
                        SourceStatus.STALE,
                        SourceStatus.VALID
                ),
                ConsistencyPhase.LIVE,
                EngineVersion.V2
        );

        assertThat(result.severity()).isEqualTo(Severity.NONE);
    }

    @Test
    void returnsFinalPassOnlyWhenEveryApplicableValueIsZero() {
        ConsistencyEvaluation result = calculator.evaluate(
                snapshot(
                        new ConsistencyRawValues(100, 50, 50, 50, 50, 50, 50),
                        SourceStatus.VALID,
                        SourceStatus.VALID
                ),
                ConsistencyPhase.FINAL,
                EngineVersion.V2
        );

        assertThat(result.phase()).isEqualTo(ConsistencyPhase.FINAL);
        assertThat(result.verdict()).isEqualTo(Verdict.PASS);
        assertThat(result.severity()).isEqualTo(Severity.NONE);
    }

    @Test
    void returnsFinalCriticalFailureForOneRemainingGapRegardlessOfLiveThreshold() {
        ConsistencyEvaluation result = calculator.evaluate(
                snapshot(
                        new ConsistencyRawValues(100, 49, 60, 60, 50, 59, 50),
                        SourceStatus.VALID,
                        SourceStatus.VALID
                ),
                ConsistencyPhase.FINAL,
                EngineVersion.V2
        );

        assertThat(result.gaps().get(ConsistencyGapType.ACTIVE_DB_GAP).value()).isEqualTo(1);
        assertThat(result.verdict()).isEqualTo(Verdict.FAIL);
        assertThat(result.severity()).isEqualTo(Severity.CRITICAL);
    }

    @Test
    void acceptsEngineDefinedNotApplicableGapsDuringV1FinalEvaluation() {
        ConsistencyEvaluation result = calculator.evaluate(
                snapshot(
                        new ConsistencyRawValues(100, 0, 0, 0, 50, 0, 50),
                        SourceStatus.N_A,
                        SourceStatus.VALID
                ),
                ConsistencyPhase.FINAL,
                EngineVersion.V1
        );

        assertThat(result.verdict()).isEqualTo(Verdict.PASS);
        assertThat(result.severity()).isEqualTo(Severity.NONE);
    }

    @Test
    void rejectsFinalEvaluationWhenAnApplicableSourceIsNotValid() {
        ConsistencyRawValues values = new ConsistencyRawValues(100, 50, 50, 50, 50, 50, 50);

        assertThatThrownBy(() -> calculator.evaluate(
                snapshot(values, SourceStatus.PENDING, SourceStatus.VALID),
                ConsistencyPhase.FINAL,
                EngineVersion.V2
        )).isInstanceOfSatisfying(BusinessException.class, exception ->
                assertThat(exception.getErrorCode())
                        .isEqualTo(ConsistencyErrorCode.FINAL_VALUE_UNAVAILABLE));

        assertThatThrownBy(() -> calculator.evaluate(
                snapshot(values, SourceStatus.STALE, SourceStatus.VALID),
                ConsistencyPhase.FINAL,
                EngineVersion.V2
        )).isInstanceOfSatisfying(BusinessException.class, exception ->
                assertThat(exception.getErrorCode())
                        .isEqualTo(ConsistencyErrorCode.FINAL_VALUE_UNAVAILABLE));

        assertThatThrownBy(() -> calculator.evaluate(
                snapshot(values, SourceStatus.VALID, SourceStatus.UNAVAILABLE),
                ConsistencyPhase.FINAL,
                EngineVersion.V2
        )).isInstanceOfSatisfying(BusinessException.class, exception ->
                assertThat(exception.getErrorCode())
                        .isEqualTo(ConsistencyErrorCode.FINAL_VALUE_UNAVAILABLE));

        assertThatThrownBy(() -> calculator.evaluate(
                snapshot(values, SourceStatus.N_A, SourceStatus.VALID),
                ConsistencyPhase.FINAL,
                EngineVersion.V2
        )).isInstanceOfSatisfying(BusinessException.class, exception ->
                assertThat(exception.getErrorCode())
                        .isEqualTo(ConsistencyErrorCode.FINAL_VALUE_UNAVAILABLE));
    }

    @Test
    void reportsCalculationOverflowWithItsDomainErrorCode() {
        ConsistencyRawValues values = new ConsistencyRawValues(
                Long.MAX_VALUE,
                -1,
                0,
                0,
                0,
                0,
                0
        );

        assertThatThrownBy(() -> calculator.evaluate(
                snapshot(values, SourceStatus.VALID, SourceStatus.VALID),
                ConsistencyPhase.LIVE,
                EngineVersion.V2
        )).isInstanceOfSatisfying(BusinessException.class, exception -> {
            assertThat(exception.getErrorCode())
                    .isEqualTo(ConsistencyErrorCode.CALCULATION_OVERFLOW);
            assertThat(exception).hasCauseInstanceOf(ArithmeticException.class);
        });
    }

    @Test
    void doesNotEvaluateArithmeticForAValueWhoseRequiredSourceIsPending() {
        ConsistencyRawValues values = new ConsistencyRawValues(
                Long.MAX_VALUE,
                -1,
                0,
                0,
                0,
                0,
                0
        );

        ConsistencyEvaluation result = calculator.evaluate(
                snapshot(values, SourceStatus.PENDING, SourceStatus.VALID),
                ConsistencyPhase.LIVE,
                EngineVersion.V2
        );

        assertPending(result.gaps().get(ConsistencyGapType.ACTIVE_DB_GAP));
        assertPending(result.gaps().get(ConsistencyGapType.LUA_GAP));
        assertPending(result.gaps().get(ConsistencyGapType.PERSIST_GAP));
    }

    private static ConsistencyRawSnapshot snapshot(
            ConsistencyRawValues values,
            SourceStatus redisStatus,
            SourceStatus databaseStatus
    ) {
        return new ConsistencyRawSnapshot(
                values,
                observation(redisStatus, REDIS_OBSERVED_AT),
                observation(databaseStatus, DATABASE_OBSERVED_AT)
        );
    }

    private static SourceObservation observation(SourceStatus status, Instant observedAt) {
        return switch (status) {
            case VALID, WARMING_UP, STALE, NO_TRAFFIC -> new SourceObservation(status, observedAt);
            case PENDING, UNAVAILABLE, N_A -> new SourceObservation(status, null);
        };
    }

    private static void assertNotApplicable(GapValue gap) {
        assertThat(gap.state()).isEqualTo(SourceStatus.N_A);
        assertThat(gap.value()).isNull();
        assertThat(gap.observedAt()).isNull();
    }

    private static void assertUnavailable(GapValue gap) {
        assertThat(gap.state()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(gap.value()).isNull();
        assertThat(gap.observedAt()).isNull();
    }

    private static void assertPending(GapValue gap) {
        assertThat(gap.state()).isEqualTo(SourceStatus.PENDING);
        assertThat(gap.value()).isNull();
        assertThat(gap.observedAt()).isNull();
    }

    private ConsistencyEvaluation evaluateV2Drift(long drift) {
        return evaluateLive(
                new ConsistencyRawValues(
                        100,
                        50 - drift,
                        200,
                        200,
                        50,
                        200 - drift,
                        50
                ),
                EngineVersion.V2
        );
    }

    private ConsistencyEvaluation evaluateLive(ConsistencyRawValues values, EngineVersion engineVersion) {
        return calculator.evaluate(
                snapshot(values, SourceStatus.VALID, SourceStatus.VALID),
                ConsistencyPhase.LIVE,
                engineVersion
        );
    }
}
