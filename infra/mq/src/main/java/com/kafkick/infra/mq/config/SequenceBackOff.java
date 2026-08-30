package com.kafkick.infra.mq.config;

import java.util.Arrays;

import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;

final class SequenceBackOff implements BackOff {
    private final long[] intervals;

    SequenceBackOff(long... intervals) {
        if (intervals == null || intervals.length == 0
                || Arrays.stream(intervals).anyMatch(interval -> interval < 0)) {
            throw new IllegalArgumentException("backoff 간격은 하나 이상의 음이 아닌 값이어야 합니다.");
        }
        this.intervals = intervals.clone();
    }

    @Override
    public BackOffExecution start() {
        return new BackOffExecution() {
            private int index;

            @Override
            public long nextBackOff() {
                return index < intervals.length ? intervals[index++] : STOP;
            }
        };
    }
}
