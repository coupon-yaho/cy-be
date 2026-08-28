package com.kafkick.core.consistency;

import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConsistencyEvaluationTest {

    private static final GapValue ZERO = new GapValue(
            0L,
            SourceStatus.VALID,
            Instant.parse("2026-08-15T05:02:31.120Z")
    );

    @Test
    void rejectsVerdictDuringLivePhase() {
        assertThatThrownBy(() -> new ConsistencyEvaluation(
                allGaps(), ZERO, ConsistencyPhase.LIVE, Verdict.PASS, Severity.NONE
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsMissingVerdictDuringFinalPhase() {
        assertThatThrownBy(() -> new ConsistencyEvaluation(
                allGaps(), ZERO, ConsistencyPhase.FINAL, null, Severity.NONE
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsMissingSeverityDuringFinalPhase() {
        assertThatThrownBy(() -> new ConsistencyEvaluation(
                allGaps(), ZERO, ConsistencyPhase.FINAL, Verdict.FAIL, null
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void acceptsFinalPassWithNoAlertSeverity() {
        new ConsistencyEvaluation(
                allGaps(), ZERO, ConsistencyPhase.FINAL, Verdict.PASS, Severity.NONE
        );
    }

    @Test
    void rejectsMissingGapType() {
        Map<ConsistencyGapType, GapValue> gaps = new EnumMap<>(allGaps());
        gaps.remove(ConsistencyGapType.PERSIST_GAP);

        assertThatThrownBy(() -> new ConsistencyEvaluation(
                gaps, ZERO, ConsistencyPhase.LIVE, null, Severity.NONE
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullGapValue() {
        Map<ConsistencyGapType, GapValue> gaps = new EnumMap<>(allGaps());
        gaps.put(ConsistencyGapType.ACTIVE_DB_GAP, null);

        assertThatThrownBy(() -> new ConsistencyEvaluation(
                gaps, ZERO, ConsistencyPhase.LIVE, null, null
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void acceptsLiveWithoutSeverityBeforeEvaluationIsAvailable() {
        new ConsistencyEvaluation(
                allGaps(), ZERO, ConsistencyPhase.LIVE, null, null
        );
    }

    @Test
    void preservesGapTypeDeclarationOrder() {
        Map<ConsistencyGapType, GapValue> reverseOrder = new java.util.LinkedHashMap<>();
        ConsistencyGapType[] gapTypes = ConsistencyGapType.values();
        for (int index = gapTypes.length - 1; index >= 0; index--) {
            reverseOrder.put(gapTypes[index], ZERO);
        }

        ConsistencyEvaluation evaluation = new ConsistencyEvaluation(
                reverseOrder, ZERO, ConsistencyPhase.LIVE, null, Severity.NONE
        );

        assertThat(evaluation.gaps().keySet()).containsExactly(ConsistencyGapType.values());
    }

    private static Map<ConsistencyGapType, GapValue> allGaps() {
        Map<ConsistencyGapType, GapValue> gaps = new EnumMap<>(ConsistencyGapType.class);
        for (ConsistencyGapType gapType : ConsistencyGapType.values()) {
            gaps.put(gapType, ZERO);
        }
        return gaps;
    }
}
