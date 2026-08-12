package com.kafkick.core.support;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;

import org.springframework.stereotype.Component;

@Component
public class TimeProvider {

    private final Clock clock;

    public TimeProvider(Clock clock) {
        this.clock = clock;
    }

    public LocalDateTime now() {
        return LocalDateTime.now(clock);
    }

    public LocalDate today() {
        return LocalDate.now(clock);
    }

    /**
     * 카프카 이벤트 등 타임존에 독립적인 절대 시각이 필요할 때 사용
     */
    public Instant instant() {
        return clock.instant();
    }
}
