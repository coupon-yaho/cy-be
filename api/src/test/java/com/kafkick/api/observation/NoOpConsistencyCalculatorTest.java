package com.kafkick.api.observation;

import com.kafkick.core.consistency.ConsistencyEvaluation;
import com.kafkick.core.consistency.ConsistencyGapType;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.consistency.ConsistencyRawSnapshot;
import com.kafkick.core.consistency.ConsistencyRawValues;
import com.kafkick.core.consistency.GapValue;
import com.kafkick.core.consistency.SourceObservation;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.SourceStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.EnumSet;

import static org.assertj.core.api.Assertions.assertThat;

class NoOpConsistencyCalculatorTest {

    private final NoOpConsistencyCalculator calculator = new NoOpConsistencyCalculator();

    @Test
    void returnsNotApplicableWithoutPretendingValuesAreZero() {
        ConsistencyEvaluation result = calculator.evaluate(
                snapshot(),
                ConsistencyPhase.FINAL,
                EngineVersion.V3
        );

        assertThat(result.gaps().keySet()).isEqualTo(EnumSet.allOf(ConsistencyGapType.class));
        assertThat(result.gaps().values()).allSatisfy(gap -> {
            assertThat(gap.value()).isNull();
            assertThat(gap.state()).isEqualTo(SourceStatus.N_A);
            assertThat(gap.observedAt()).isNull();
        });
        assertThat(result.overIssued()).isEqualTo(new GapValue(null, SourceStatus.N_A, null));
        assertThat(result.phase()).isEqualTo(ConsistencyPhase.LIVE);
        assertThat(result.verdict()).isNull();
        assertThat(result.severity()).isNull();
    }

    private static ConsistencyRawSnapshot snapshot() {
        Instant observedAt = Instant.parse("2026-08-15T05:02:31.120Z");
        return new ConsistencyRawSnapshot(
                new ConsistencyRawValues(10, 10, 0, 0, 0, 0, 0),
                new SourceObservation(SourceStatus.VALID, observedAt),
                new SourceObservation(SourceStatus.VALID, observedAt)
        );
    }
}
