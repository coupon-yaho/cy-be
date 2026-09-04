package com.kafkick.core.notification;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.notification.domain.NotificationOutbox;
import com.kafkick.core.notification.domain.NotificationOutboxClaim;

public interface NotificationOutboxRepository {
    NotificationOutbox save(NotificationOutbox outbox);
    Optional<AttemptTrigger> findTriggerByNotificationIdAndAttemptSeq(
            Long notificationId, int attemptSeq);
    /**
     * 발행할 명령을 <b>한 번에 여러 건</b> 선점한다.
     *
     * <p><b>배치인 이유는 왕복이 아니라 경합이다.</b> 한 건씩 집으면 워커가 늘어도
     * 선점 문장이 서로를 기다린다 — MySQL 레퍼런스가 <i>"큐 같은 테이블에 여러 세션이
     * 접근할 때 락 경합을 피하는 데 쓸 수 있다"</i> 며 {@code SKIP LOCKED} 를 지목한
     * 자리가 정확히 이것이다.
     *
     * @param lease 선점 유효 기간. 이보다 오래 잡고 있으면 회수 대상이 된다
     * @param max   한 번에 집을 최대 건수
     * @return 선점한 것들. 없으면 빈 목록. <b>요청한 수보다 적을 수 있다</b> —
     *         남이 이미 잠근 행은 조용히 건너뛴다
     * @throws IllegalArgumentException {@code max} 가 1 미만일 때. 0 이면 {@code LIMIT 0} 이
     *         오류 없이 0건을 돌려줘 <b>릴레이가 조용히 멈춘다</b>
     */
    List<NotificationOutboxClaim> claimBatch(Duration lease, int max);
    boolean markPublished(Long outboxId, String claimToken, Instant publishedAt);
    boolean markFailed(Long outboxId, String claimToken, Duration retryDelay);
}
