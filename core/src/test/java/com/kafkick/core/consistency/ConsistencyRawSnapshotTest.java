package com.kafkick.core.consistency;

import com.kafkick.core.observation.SourceStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConsistencyRawSnapshotTest {

    private static final ConsistencyRawValues RAW_VALUES =
            new ConsistencyRawValues(10, 10, 0, 0, 0, 0, 0);
    private static final SourceObservation OBSERVATION =
            new SourceObservation(SourceStatus.PENDING, null);
    private static final Instant OBSERVED_AT = Instant.parse("2026-08-15T05:02:31.120Z");

    @Test
    void requiresRawValuesAndSourceObservations() {
        assertThatThrownBy(() -> new ConsistencyRawSnapshot(null, OBSERVATION, OBSERVATION))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ConsistencyRawSnapshot(RAW_VALUES, null, OBSERVATION))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ConsistencyRawSnapshot(RAW_VALUES, OBSERVATION, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void sourceObservationRequiresStatus() {
        assertThatThrownBy(() -> new SourceObservation(null, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void observedSourceStatesRequireObservationTime() {
        assertThatThrownBy(() -> new SourceObservation(SourceStatus.VALID, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SourceObservation(SourceStatus.WARMING_UP, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SourceObservation(SourceStatus.STALE, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SourceObservation(SourceStatus.NO_TRAFFIC, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void unobservedSourceStatesRejectObservationTime() {
        assertThatThrownBy(() -> new SourceObservation(SourceStatus.PENDING, OBSERVED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SourceObservation(SourceStatus.UNAVAILABLE, OBSERVED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SourceObservation(SourceStatus.N_A, OBSERVED_AT))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
