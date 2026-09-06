package com.kafkick.core.notification;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import com.kafkick.core.notification.domain.Notification;
import com.kafkick.core.notification.domain.NotificationStatus;
import com.kafkick.core.notification.domain.NotificationFailure;

public interface NotificationRepository {
    Notification save(Notification notification);
    Optional<Notification> findById(Long notificationId);

    /**
     * 여러 건을 <b>한 번에</b> 읽는다.
     *
     * <p><b>커넥션 빌림을 줄이려고 있다.</b> 릴레이 워커가 건당 {@link #findById} 를 부르면
     * 발행 한 건마다 커넥션을 두 번 빌리는데(조회·표시), 그 풀을 <b>접수 요청과 공유</b>한다
     * — CY-923 이 그 압력의 절벽을 쟀다(풀 13 에서 워커 12 부터 요청 p99 4 배).
     *
     * <p><b>없는 id 는 결과에서 빠진다</b> — 예외가 아니다. 부르는 쪽이 "무엇이 없는지" 를
     * 알아야 하고(릴레이는 그것을 {@code NOTIFICATION_MISSING} 으로 되돌린다),
     * 하나가 없다고 나머지 63건의 발행을 막을 이유가 없다.
     *
     * @param notificationIds 읽을 id 들. 빈 것을 주면 빈 결과다
     */
    List<Notification> findAllByIdIn(Collection<Long> notificationIds);
    long countByCouponId(Long couponId);
    long countAll();
    long countByCouponIdAndStatusIn(Long couponId, List<NotificationStatus> statuses);
    long countByStatusIn(List<NotificationStatus> statuses);
    List<NotificationFailure> findFailuresBeforeId(Long beforeId, int limit);
    boolean saveIfStatus(Notification notification, NotificationStatus expectedStatus,
            int expectedAttemptCount, int expectedResendCount);
}
