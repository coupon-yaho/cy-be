package com.kafkick.core.consistency;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.EnumMap;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.overview.calculator.ConsistencyActionContext;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;

class ConsistencyFinalObservationTest {
    @Test
    void onlyFourOuterStatesAreAllowedAndOnlyValidCarriesValue() {
        assertThatCode(() -> new ConsistencyFinalObservation(SourceStatus.PENDING, null))
                .doesNotThrowAnyException();
        assertThatCode(() -> new ConsistencyFinalObservation(SourceStatus.UNAVAILABLE, null))
                .doesNotThrowAnyException();
        assertThatCode(() -> new ConsistencyFinalObservation(SourceStatus.N_A, null))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> new ConsistencyFinalObservation(SourceStatus.STALE, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ConsistencyFinalObservation(SourceStatus.VALID, null))
                .isInstanceOf(IllegalArgumentException.class);

        // 불변식은 양방향 동치다. 한쪽 함의만 검증하면 동치를 약화시켜도 통과한다.
        ConsistencyActionContext value = context();
        assertThatCode(() -> new ConsistencyFinalObservation(SourceStatus.VALID, value))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> new ConsistencyFinalObservation(SourceStatus.PENDING, value))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ConsistencyFinalObservation(SourceStatus.UNAVAILABLE, value))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ConsistencyFinalObservation(SourceStatus.N_A, value))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static ConsistencyActionContext context() {
        Instant observedAt = Instant.parse("2026-08-26T00:12:00Z");
        var gaps = new EnumMap<ConsistencyGapType, GapValue>(ConsistencyGapType.class);
        for (ConsistencyGapType type : ConsistencyGapType.values()) {
            gaps.put(type, new GapValue(0L, SourceStatus.VALID, observedAt));
        }
        ConsistencyEvaluation evaluation = new ConsistencyEvaluation(gaps,
                new GapValue(0L, SourceStatus.VALID, observedAt), ConsistencyPhase.FINAL,
                Verdict.PASS, Severity.NONE);
        return new ConsistencyActionContext(11L, "first", null,
                Instant.parse("2026-08-26T00:00:00Z"), EngineVersion.V3, evaluation);
    }
}
