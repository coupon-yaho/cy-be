package com.kafkick.api.observation.issuance;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;

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
        this.delegates = flatten(Objects.requireNonNull(delegates, "delegates"));
        if (this.delegates.isEmpty()) {
            throw new IllegalArgumentException("delegates must not be empty");
        }
    }

    /**
     * 중첩된 합성 기록기를 펼치고 같은 sink 를 한 번만 남긴다.
     *
     * <p>fan-out 은 컨텍스트에 등록된 기록기를 전부 모으므로, 누군가 자기 합성 기록기로 이미
     * 빈으로 올라와 있는 sink 를 감싸면 그 sink 가 두 번 불린다 — 미터라면 값이 그대로 두 배가
     * 되고, 예외가 아니라 그래프로만 드러난다. 동일성은 {@code equals} 가 아니라 참조로 본다.
     */
    private static List<EventRecorder> flatten(EventRecorder[] delegates) {
        List<EventRecorder> leaves = new ArrayList<>();
        Set<EventRecorder> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (EventRecorder delegate : delegates) {
            expand(delegate, leaves, seen);
        }
        return List.copyOf(leaves);
    }

    private static void expand(
            EventRecorder delegate,
            List<EventRecorder> leaves,
            Set<EventRecorder> seen
    ) {
        Objects.requireNonNull(delegate, "delegate");
        if (delegate instanceof CompositeEventRecorder composite) {
            composite.delegates.forEach(nested -> expand(nested, leaves, seen));
            return;
        }
        if (seen.add(delegate)) {
            leaves.add(delegate);
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
