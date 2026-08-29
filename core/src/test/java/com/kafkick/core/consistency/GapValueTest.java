package com.kafkick.core.consistency;

import com.kafkick.core.observation.SourceStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GapValueTest {

    private static final Instant OBSERVED_AT = Instant.parse("2026-08-15T05:02:31.120Z");

    @Test
    void rejectsValueForPendingUnavailableAndNotApplicable() {
        assertThatThrownBy(() -> new GapValue(0L, SourceStatus.PENDING, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new GapValue(0L, SourceStatus.UNAVAILABLE, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new GapValue(0L, SourceStatus.N_A, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsObservationTimeForUnavailable() {
        assertThatThrownBy(() -> new GapValue(null, SourceStatus.UNAVAILABLE, OBSERVED_AT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void pendingRejectsObservationTimeAcrossSourceAndGapContracts() {
        assertThatThrownBy(() -> new SourceObservation(SourceStatus.PENDING, OBSERVED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new GapValue(null, SourceStatus.PENDING, OBSERVED_AT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsObservationTimeForNotApplicable() {
        assertThatThrownBy(() -> new GapValue(null, SourceStatus.N_A, OBSERVED_AT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsMissingValueOrTimeForValid() {
        assertThatThrownBy(() -> new GapValue(null, SourceStatus.VALID, OBSERVED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new GapValue(0L, SourceStatus.VALID, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsSourceStatesThatHaveNoGapMeaning() {
        assertThatThrownBy(() -> new GapValue(null, SourceStatus.WARMING_UP, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new GapValue(null, SourceStatus.NO_TRAFFIC, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsStaleWithoutValueOrLastSuccessfulObservationTime() {
        assertThatThrownBy(() -> new GapValue(null, SourceStatus.STALE, OBSERVED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new GapValue(3L, SourceStatus.STALE, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
