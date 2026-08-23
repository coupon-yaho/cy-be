package com.kafkick.api.observation.issuance;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

import com.kafkick.core.observation.EventRecorder;
import com.kafkick.core.observation.IssuanceFlowEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Delivers one issuance event to each independent observation sink. */
public final class CompositeEventRecorder implements EventRecorder {

    private static final Logger log = LoggerFactory.getLogger(CompositeEventRecorder.class);

    private final List<EventRecorder> delegates;
    private final FailureLogThrottle failureLog;

    public CompositeEventRecorder(EventRecorder... delegates) {
        this(IssuanceObservationService.DEFAULT_LOG_INTERVAL, delegates);
    }

    /**
     * 전달 실패 로그의 유량 제한 간격을 외부 설정에서 받는 생성자다.
     *
     * <p>{@link MeterEventRecorder} 와 같은 값을 쓴다 — 두 기록기가 서로 다른 간격으로 짖으면
     * 부하 회차마다 한쪽만 튜닝되어 로그량이 갈린다.
     *
     * @param failureLogInterval 같은 실패를 다시 로그로 남기기까지의 최소 간격
     * @param delegates 이벤트를 함께 받을 관측 sink 들
     */
    public CompositeEventRecorder(Duration failureLogInterval, EventRecorder... delegates) {
        this.failureLog = new FailureLogThrottle(failureLogInterval);
        this.delegates = Arrays.stream(Objects.requireNonNull(delegates, "delegates"))
                .map(delegate -> Objects.requireNonNull(delegate, "delegate"))
                .toList();
        if (this.delegates.isEmpty()) {
            throw new IllegalArgumentException("delegates must not be empty");
        }
    }

    @Override
    public void record(IssuanceFlowEvent event) {
        for (EventRecorder delegate : delegates) {
            try {
                delegate.record(event);
            } catch (RuntimeException exception) {
                logFailureAtMostOncePerInterval(delegate, exception);
            }
        }
    }

    private void logFailureAtMostOncePerInterval(EventRecorder delegate, RuntimeException exception) {
        OptionalLong total = failureLog.recordFailure();
        if (total.isEmpty()) {
            return;
        }
        log.warn("발급 관측 전달에 실패했습니다. 다른 관측기는 계속 기록합니다. "
                        + "누적 {}건, recorder={}, cause={}",
                total.getAsLong(), delegate.getClass().getName(), exception.getClass().getSimpleName());
    }
}
