package com.kafkick.api.admin.notification.dto;

import java.time.Instant;

import com.kafkick.api.admin.support.ObservedValue;
import com.kafkick.core.notification.NotificationSummary;

/** 고객 알림 파이프라인의 비즈니스 진행률 요약입니다. */
public record NotificationSummaryResponse(Long couponId, Instant snapshotAt,
                                          ObservedValue<Long> totalRequests,
                                          ObservedValue<Long> sentCount,
                                          ObservedValue<Long> failedCount,
                                          ObservedValue<Long> remainingCount,
                                          ObservedValue<Double> sentRate) {
    public static NotificationSummaryResponse from(NotificationSummary summary) {
        return new NotificationSummaryResponse(summary.couponId(), summary.snapshotAt(),
                observed(summary.totalRequests()), observed(summary.sentCount()),
                observed(summary.failedCount()), observed(summary.remainingCount()),
                observed(summary.sentRate()));
    }

    private static <T> ObservedValue<T> observed(NotificationSummary.Metric<T> metric) {
        return new ObservedValue<>(metric.value(), metric.state(), metric.observedAt());
    }
}
