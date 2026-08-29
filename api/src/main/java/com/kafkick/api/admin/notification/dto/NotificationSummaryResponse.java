package com.kafkick.api.admin.notification.dto;

import java.time.Instant;

import com.kafkick.api.admin.support.ObservedValue;

/** 고객 알림 파이프라인의 비즈니스 진행률 요약입니다. */
public record NotificationSummaryResponse(Long couponId, Instant snapshotAt,
                                          ObservedValue<Long> totalRequests,
                                          ObservedValue<Long> sentCount,
                                          ObservedValue<Long> failedCount,
                                          ObservedValue<Long> remainingCount,
                                          ObservedValue<Double> sentRate) {
}
