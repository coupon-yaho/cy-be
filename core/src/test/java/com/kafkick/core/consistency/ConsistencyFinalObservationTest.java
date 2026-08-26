package com.kafkick.core.consistency;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

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
    }
}
