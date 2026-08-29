package com.kafkick.core.notification;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import org.springframework.transaction.annotation.Propagation;
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

    /**
     * 집계 전체를 하나의 트랜잭션으로 묶지 않고 조회 실패를 UNAVAILABLE 요약으로 변환합니다.
     * 호출자 트랜잭션도 중단해 참여 조회가 rollback-only로 표시한 실패가 응답 경계 밖에서
     * {@code UnexpectedRollbackException}으로 바뀌지 않게 합니다.
     */
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
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

    /**
     * 실패 알림을 과거 방향 keyset 페이지로 조회합니다.
     *
     * @throws IllegalArgumentException {@code beforeId}가 0 이하이거나 {@code limit}이
     *         1 이상 200 이하가 아닌 경우
     */
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
