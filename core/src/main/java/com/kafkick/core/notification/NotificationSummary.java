package com.kafkick.core.notification;

import java.time.Instant;
import java.util.Objects;

import com.kafkick.core.observation.SourceStatus;

public record NotificationSummary(Long couponId, Instant snapshotAt,
        Metric<Long> totalRequests, Metric<Long> sentCount, Metric<Long> failedCount,
        Metric<Long> remainingCount, Metric<Double> sentRate) {

    public NotificationSummary {
        Objects.requireNonNull(totalRequests, "totalRequests");
        Objects.requireNonNull(sentCount, "sentCount");
        Objects.requireNonNull(failedCount, "failedCount");
        Objects.requireNonNull(remainingCount, "remainingCount");
        Objects.requireNonNull(sentRate, "sentRate");
    }

    public record Metric<T>(T value, SourceStatus state, Instant observedAt) {
        /**
         * @throws IllegalArgumentException 값이 있는 상태에 값·관측 시각이 없거나,
         *         값이 없는 상태에 값·관측 시각이 있는 경우
         */
        public Metric {
            Objects.requireNonNull(state, "state");
            if ((state.carriesValue() && (value == null || observedAt == null))
                    || (!state.carriesValue() && (value != null || observedAt != null))) {
                throw new IllegalArgumentException("알림 요약 값과 상태가 일치하지 않습니다.");
            }
        }

        public static <T> Metric<T> observed(T value, SourceStatus state, Instant at) {
            return new Metric<>(value, state, at);
        }

        public static <T> Metric<T> absent(SourceStatus state) {
            return new Metric<>(null, state, null);
        }
    }
}
