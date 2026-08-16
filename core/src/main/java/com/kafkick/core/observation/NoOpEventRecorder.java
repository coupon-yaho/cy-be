package com.kafkick.core.observation;

public class NoOpEventRecorder implements EventRecorder {

    @Override
    public void record(IssuanceFlowEvent event) {
        // 관측 구현의 부재가 발급 흐름에 영향을 주지 않아야 한다.
    }
}
