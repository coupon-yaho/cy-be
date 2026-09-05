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
    /**
     * 실패를 적고 되돌린다. 실패 횟수가 상한에 닿으면 <b>되돌리지 않고 종착시킨다.</b>
     *
     * <p><b>{@code reason} 은 지표 때문에 받는다.</b> 무엇이 실패했는지는 부르는 쪽만 알고,
     * <b>그 쓰기가 실제로 먹었는지·상한을 넘겼는지는 여기만 안다.</b> 둘이 만나야 지표가
     * 맞으므로 사유가 포트를 타고 넘어온다 — 안 받으면 어댑터가 모든 실패를 한 사유로
     * 뭉뚱그리고, 되돌린 것과 <b>종착한 것</b>을 구분하지 못해 재시도 수를 부풀린다.
     *
     * @return 되돌렸거나 종착시켰으면 {@code true}. 토큰이 안 맞거나 이미 남이 가져갔으면
     *         {@code false} — <b>그때는 아무것도 세지 않는다</b>, 이 건은 이미 남의 것이다
     */
    boolean markFailed(Long outboxId, String claimToken, Duration retryDelay,
            OutboxRetryReason reason);

    /**
     * <b>집어 놓고 보내 보지도 못한 것을 되돌린다.</b> 즉시 다시 집을 수 있는 상태로 만들고
     * {@code failure_count} 는 <b>건드리지 않는다.</b>
     *
     * <p>{@link #markFailed} 와 다른 이유가 이것이다. 워커 풀이 제출을 거부한 건은
     * <b>발행이 실패한 것이 아니라 시작도 안 한 것</b>이라, 실패로 세면 거부가 잦은 순간에
     * {@code failure_count} 가 실제 발행 실패 없이 10 에 닿아 <b>한 번도 안 보낸 알림이
     * {@code DEAD} 가 된다.</b>
     *
     * <p>안 되돌리면 그 행은 <b>lease 가 만료될 때까지</b> {@code IN_PROGRESS} 로 아무도
     * 못 집는다. 기본 lease 가 30초이므로 그만큼 늦는다.
     *
     * @return 되돌렸으면 {@code true}. 토큰이 안 맞거나 이미 남이 가져갔으면 {@code false} —
     *         <b>그 경우 아무것도 하지 않는 것이 맞다</b>, 이 건은 이미 남의 것이다
     */
    boolean releaseClaim(Long outboxId, String claimToken);

    /**
     * 아직 안 나간 발행 명령 수 — <b>백로그</b>.
     *
     * <p><b>인플라이트 게이지만으로는 한가한 것과 막힌 것을 구분하지 못한다.</b>
     * 상한에 붙어 있는데 백로그가 안 줄면 워커가 모자란 것이고, 백로그도 0 이면 그냥
     * 보낼 것이 없는 것이다 — 같은 게이지 값이 두 뜻이라, 이 수와 <b>함께 봐야</b> 갈린다.
     *
     * <p>{@code PENDING} 과 {@code IN_PROGRESS} 를 <b>함께</b> 센다. 둘 다 "아직 안 나갔다"
     * 이고, {@code IN_PROGRESS} 를 빼면 <b>릴레이가 붙잡고 못 끝내는 상태에서 백로그가
     * 0 으로 보인다</b> — 그것이 정확히 사고 상태다.
     *
     * <p>{@code DEAD} 는 안 센다. 그것은 <b>다시 시도되지 않으므로</b> 백로그가 아니라
     * 사람이 처리할 목록이고, 그 축은 {@code app.outbox.dead} 가 따로 진다.
     *
     * @return 아직 안 나간 건수
     */
    long countBacklog();
}
