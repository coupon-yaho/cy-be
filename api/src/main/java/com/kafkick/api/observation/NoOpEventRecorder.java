package com.kafkick.api.observation;

import com.kafkick.core.observation.EventRecorder;
import com.kafkick.core.observation.IssuanceFlowEvent;

/** Used only when a process intentionally has neither Micrometer nor the Kafka publisher. */
public final class NoOpEventRecorder implements EventRecorder {

    @Override
    public void record(IssuanceFlowEvent event) {
        // Observation infrastructure is absent; issuance must remain available.
    }
}
