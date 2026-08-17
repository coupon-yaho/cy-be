package com.kafkick.api.observation;

import com.kafkick.core.observation.EventRecorder;
import com.kafkick.core.observation.IssuanceFlowEvent;

public final class NoOpEventRecorder implements EventRecorder {

    @Override
    public void record(IssuanceFlowEvent event) {
        // 관측 구현의 부재가 발급 흐름에 영향을 주지 않아야 한다.
    }
}
