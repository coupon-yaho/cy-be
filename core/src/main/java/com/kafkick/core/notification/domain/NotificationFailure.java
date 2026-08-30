package com.kafkick.core.notification.domain;

import java.time.Instant;

public record NotificationFailure(Long notificationId, Long couponId, Long memberId,
        NotifyFailureReason reason, int attemptCount, Instant failedAt) { }
