package com.kafkick.core.notification;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.core.notification.NotificationSummary.Metric;
import com.kafkick.core.notification.domain.NotificationFailure;
import com.kafkick.core.notification.domain.NotificationStatus;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;

public class NotificationQueryService {
    private static final List<NotificationStatus> SENT = List.of(NotificationStatus.SENT);
    private static final List<NotificationStatus> FAILED =
            List.of(NotificationStatus.FAILED, NotificationStatus.DEAD);
    private static final List<NotificationStatus> REMAINING =
            List.of(NotificationStatus.PENDING, NotificationStatus.SENDING);

    private final NotificationRepository notifications;
    private final CouponRoundRepository couponRounds;
    private final TimeProvider timeProvider;

    public NotificationQueryService(NotificationRepository notifications,
            CouponRoundRepository couponRounds, TimeProvider timeProvider) {
        this.notifications = Objects.requireNonNull(notifications, "notifications");
        this.couponRounds = Objects.requireNonNull(couponRounds, "couponRounds");
        this.timeProvider = Objects.requireNonNull(timeProvider, "timeProvider");
    }

    @Transactional(readOnly = true)
    public NotificationSummary getSummary(Long couponId) {
        try {
            if (couponId != null && couponRounds.findById(couponId).isEmpty()) {
                return absentSummary(couponId, SourceStatus.N_A);
            }
            long total = couponId == null
                    ? notifications.countAll() : notifications.countByCouponId(couponId);
            long sent = count(couponId, SENT);
            long failed = count(couponId, FAILED);
            long remaining = count(couponId, REMAINING);
            Instant snapshotAt = timeProvider.instant();
            SourceStatus countStatus = total == 0
                    ? SourceStatus.NO_TRAFFIC : SourceStatus.VALID;
            Metric<Double> rate = total == 0
                    ? Metric.absent(SourceStatus.N_A)
                    : Metric.observed((double) sent / total, SourceStatus.VALID, snapshotAt);
            return new NotificationSummary(couponId, snapshotAt,
                    Metric.observed(total, countStatus, snapshotAt),
                    Metric.observed(sent, countStatus, snapshotAt),
                    Metric.observed(failed, countStatus, snapshotAt),
                    Metric.observed(remaining, countStatus, snapshotAt), rate);
        } catch (RuntimeException failure) {
            return absentSummary(couponId, SourceStatus.UNAVAILABLE);
        }
    }

    @Transactional(readOnly = true)
    public NotificationFailurePage getFailures(Long beforeId, int limit) {
        if (beforeId != null && beforeId <= 0) {
            throw new IllegalArgumentException("beforeId는 양수여야 합니다.");
        }
        if (limit < 1 || limit > 200) {
            throw new IllegalArgumentException("limit은 1 이상 200 이하여야 합니다.");
        }
        List<NotificationFailure> fetched = notifications.findFailuresBeforeId(beforeId, limit + 1);
        boolean hasOlder = fetched.size() > limit;
        List<NotificationFailure> publicItems = hasOlder
                ? List.copyOf(fetched.subList(0, limit)) : List.copyOf(fetched);
        Long next = hasOlder ? publicItems.get(publicItems.size() - 1).notificationId() : null;
        return new NotificationFailurePage(publicItems, next, hasOlder);
    }

    private long count(Long couponId, List<NotificationStatus> statuses) {
        return couponId == null
                ? notifications.countByStatusIn(statuses)
                : notifications.countByCouponIdAndStatusIn(couponId, statuses);
    }

    private static NotificationSummary absentSummary(Long couponId, SourceStatus status) {
        return new NotificationSummary(couponId, null,
                Metric.absent(status), Metric.absent(status), Metric.absent(status),
                Metric.absent(status), Metric.absent(status));
    }
}
