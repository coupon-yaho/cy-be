package com.kafkick.core.queuegateway;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import com.kafkick.core.observation.SourceStatus;

class QueueGatewayCouponRoundStateTest {

    private static final Instant OBSERVED_AT = Instant.parse("2026-08-31T00:00:00Z");

    @Test
    void acceptsNormalAndValueLessStates() {
        new QueueGatewayCouponRoundState(1L, 10L, SourceStatus.VALID, OBSERVED_AT);
        new QueueGatewayCouponRoundState(2L, null, SourceStatus.UNAVAILABLE, null);
    }

    @Test
    void rejectsNonPositiveCouponId() {
        assertThatThrownBy(() -> new QueueGatewayCouponRoundState(
                0L, 10L, SourceStatus.VALID, OBSERVED_AT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNegativeStock() {
        assertThatThrownBy(() -> new QueueGatewayCouponRoundState(
                1L, -1L, SourceStatus.VALID, OBSERVED_AT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void requiresValueAndObservedAtTogetherAccordingToStatus() {
        assertThatThrownBy(() -> new QueueGatewayCouponRoundState(
                1L, 10L, SourceStatus.UNAVAILABLE, OBSERVED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new QueueGatewayCouponRoundState(
                1L, null, SourceStatus.VALID, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
