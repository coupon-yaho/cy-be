package com.kafkick.core.observation;

public interface EventRecorder {

    void record(IssuanceFlowEvent event);
}
