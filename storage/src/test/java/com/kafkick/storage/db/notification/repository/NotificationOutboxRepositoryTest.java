package com.kafkick.storage.db.notification.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.notification.NotificationOutboxRepository;
import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.notification.domain.NotificationOutbox;
import com.kafkick.core.notification.domain.NotificationOutboxStatus;
import com.kafkick.core.notification.retry.NotificationRetryBackOffConfig;
import com.kafkick.storage.db.RepositoryTest;

@RepositoryTest
// 지연 정책은 core 가 소유한다(CY-907). @DataJpaTest 는 그 @Configuration 을 스캔하지 않으므로
// 여기서 명시로 붙인다 — 안 붙이면 어댑터가 백오프를 못 받아 컨텍스트가 안 뜬다.
@Import({NotificationOutboxRepositoryImpl.class, NotificationRetryBackOffConfig.class,
        OutboxMeterTestConfig.class})
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class NotificationOutboxRepositoryTest {
    private static final Instant AT = Instant.parse("2026-08-27T00:00:00Z");

    @Autowired NotificationOutboxRepository repository;
    @Autowired JdbcTemplate jdbcTemplate;

    @AfterEach
    void cleanCommittedFixtures() {
        jdbcTemplate.update("DELETE FROM notification_outbox WHERE notification_id BETWEEN 1 AND 4");
        jdbcTemplate.update("DELETE FROM notification_outbox WHERE notification_id=9001");
        jdbcTemplate.update("DELETE FROM notification_outbox WHERE notification_id=9002");
        jdbcTemplate.update("DELETE FROM notification_outbox WHERE notification_id=9003");
        jdbcTemplate.update("DELETE FROM notification_outbox WHERE notification_id=9004");
        jdbcTemplate.update("DELETE FROM notification_outbox WHERE notification_id=9005");
        jdbcTemplate.update("DELETE FROM notification_attempts WHERE notification_id IN (9004, 9005)");
        jdbcTemplate.update("DELETE FROM notifications WHERE id IN (9001, 9002, 9003, 9004, 9005)");
    }

    @Test
    void pendingCommandIsClaimedAndPublishedOnlyByFencingToken() {
        NotificationOutbox saved = repository.save(NotificationOutbox.pending(
                1L, 1, AttemptTrigger.INITIAL, AT));
        assertThat(saved.createdAt()).isEqualTo(AT);
        assertThat(repository.findTriggerByNotificationIdAndAttemptSeq(1L, 1))
                .contains(AttemptTrigger.INITIAL);
        var claim = claimOne(Duration.ofMinutes(1)).orElseThrow();
        assertThat(claim.outboxId()).isEqualTo(saved.id());
        assertThat(claim.requestedAt()).isEqualTo(AT);
        assertThat(claimOne(Duration.ofMinutes(1))).isEmpty();
        assertThat(repository.markPublished(saved.id(), "wrong", AT.plusSeconds(1))).isFalse();
        assertThat(repository.markPublished(saved.id(), claim.claimToken(), AT.plusSeconds(1))).isTrue();
    }

    @Test
    void expiredClaimIsRecoveredAndPoisonCommandDoesNotBlockNext() {
        NotificationOutbox poison = repository.save(NotificationOutbox.pending(
                3L, 1, AttemptTrigger.INITIAL, AT));
        NotificationOutbox next = repository.save(NotificationOutbox.pending(
                4L, 1, AttemptTrigger.INITIAL, AT));
        var first = claimOne(Duration.ofSeconds(1)).orElseThrow();
        jdbcTemplate.update("UPDATE notification_outbox SET claimed_at="
                + "TIMESTAMPADD(SECOND,-2,CURRENT_TIMESTAMP(6)) WHERE id=?", poison.id());
        var nextClaim = claimOne(Duration.ofSeconds(1)).orElseThrow();
        assertThat(nextClaim.outboxId()).isEqualTo(next.id());
        assertThat(repository.markPublished(next.id(), nextClaim.claimToken(), AT.plusSeconds(1))).isTrue();
        // 재시도 시각을 **테스트가 정한다.** 예전에는 회수 지연이 고정 1초라 그냥
        // isEmpty() 를 걸어도 됐는데, 지금은 Full Jitter 라 random(0, 400ms) 다(CY-907) —
        // 0 에 가까운 값이 나오면 곧바로 다시 집힌다. 그것은 지터의 정의대로이지 결함이
        // 아니므로, **여기서 재려는 것**(재시도 시각 전에는 안 집힌다)만 남기고 값에 대한
        // 의존을 끊는다.
        jdbcTemplate.update("UPDATE notification_outbox SET next_attempt_at="
                + "TIMESTAMPADD(SECOND,60,CURRENT_TIMESTAMP(6)) WHERE id=?", poison.id());
        assertThat(claimOne(Duration.ofSeconds(1))).isEmpty();
        jdbcTemplate.update("UPDATE notification_outbox SET next_attempt_at="
                + "TIMESTAMPADD(SECOND,-1,CURRENT_TIMESTAMP(6)) WHERE id=?", poison.id());
        var recovered = claimOne(Duration.ofSeconds(1)).orElseThrow();
        assertThat(recovered.outboxId()).isEqualTo(poison.id());
        assertThat(recovered.claimToken()).isNotEqualTo(first.claimToken());
        for (int failure = 0; failure < 9; failure++) {
            assertThat(repository.markFailed(poison.id(), recovered.claimToken(), Duration.ZERO))
                    .isTrue();
            if (failure < 8) {
                recovered = claimOne(Duration.ofSeconds(1)).orElseThrow();
            }
        }
        assertThat(claimOne(Duration.ofSeconds(1))).isEmpty();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT failure_count FROM notification_outbox WHERE id=?", Integer.class, poison.id()))
                .isEqualTo(10);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM notification_outbox WHERE id=?", String.class, poison.id()))
                .isEqualTo("DEAD");
        assertThat(repository.markFailed(
                poison.id(), recovered.claimToken(), Duration.ZERO)).isFalse();
    }

    @Test
    void genericSaveCannotRewritePublishedState() {
        NotificationOutbox saved = repository.save(NotificationOutbox.pending(
                2L, 1, AttemptTrigger.MANUAL, AT));
        NotificationOutbox published = new NotificationOutbox(saved.id(), saved.notificationId(),
                saved.attemptSeq(), saved.trigger(), NotificationOutboxStatus.PUBLISHED,
                saved.createdAt(), AT.plusSeconds(1));
        assertThatThrownBy(() -> repository.save(published))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>집어 놓고 보내 보지도 못한 것을 되돌린다 — {@code failure_count} 는 안 건드린다.</b>
     *
     * <p>워커 풀이 제출을 거부한 건이 여기로 온다. {@code markFailed} 로 세면 거부가 잦은
     * 순간에 {@code failure_count} 가 실제 발행 실패 없이 10 에 닿아 <b>한 번도 안 보낸
     * 알림이 {@code DEAD} 가 된다.</b>
     *
     * <p>되돌린 뒤 <b>즉시</b> 다시 집혀야 한다. 지연을 주면 되돌린 의미가 없다 — 이 건은
     * 아직 아무 대가도 치르지 않았다.
     */
    @Test
    void releaseClaimReturnsTheRowImmediatelyWithoutCountingAFailure() {
        NotificationOutbox saved = repository.save(NotificationOutbox.pending(
                2L, 1, AttemptTrigger.INITIAL, AT));
        var claim = claimOne(Duration.ofMinutes(1)).orElseThrow();

        assertThat(repository.releaseClaim(saved.id(), claim.claimToken())).isTrue();

        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM notification_outbox WHERE id=?", String.class, saved.id()))
                .isEqualTo("PENDING");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT failure_count FROM notification_outbox WHERE id=?",
                Integer.class, saved.id()))
                .as("거부는 발행 실패가 아니다 — 세면 안 보낸 알림이 DEAD 로 간다")
                .isZero();

        var again = claimOne(Duration.ofMinutes(1)).orElseThrow();
        assertThat(again.outboxId()).as("되돌렸으면 즉시 다시 집혀야 합니다")
                .isEqualTo(saved.id());
        assertThat(again.claimToken()).isNotEqualTo(claim.claimToken());
    }

    /** 토큰이 안 맞으면 아무것도 안 한다 — 그 건은 이미 남의 것이다. */
    @Test
    void releaseClaimDoesNothingForAStaleToken() {
        NotificationOutbox saved = repository.save(NotificationOutbox.pending(
                2L, 1, AttemptTrigger.INITIAL, AT));
        var claim = claimOne(Duration.ofMinutes(1)).orElseThrow();

        assertThat(repository.releaseClaim(saved.id(), "wrong")).isFalse();

        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM notification_outbox WHERE id=?", String.class, saved.id()))
                .as("남의 클레임을 풀어 주면 그 워커가 아직 발행 중인데 남이 또 집습니다")
                .isEqualTo("IN_PROGRESS");
        assertThat(repository.markPublished(saved.id(), claim.claimToken(), AT.plusSeconds(1)))
                .isTrue();
    }

    @Test
    void manualOutboxDeadReturnsSendingNotificationToFailed() {
        insertSendingNotification(9001L);
        NotificationOutbox outbox = repository.save(NotificationOutbox.pending(
                9001L, 1, AttemptTrigger.MANUAL, AT));
        for (int failure = 0; failure < 10; failure++) {
            var claim = claimOne(Duration.ofSeconds(1)).orElseThrow();
            assertThat(repository.markFailed(outbox.id(), claim.claimToken(), Duration.ZERO)).isTrue();
        }

        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM notifications WHERE id=9001", String.class)).isEqualTo("FAILED");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT last_failure_reason FROM notifications WHERE id=9001", String.class))
                .isEqualTo("OUTBOX_PUBLISH_FAILED");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT resend_count FROM notifications WHERE id=9001", Integer.class)).isZero();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM notification_attempts WHERE notification_id=9001"
                        + " AND attempt_seq=1 AND result='FAILED'"
                        + " AND failure_reason='OUTBOX_PUBLISH_FAILED'", Integer.class)).isEqualTo(1);
    }

    @Test
    void repeatedLeaseExpiryAlsoReturnsManualNotificationToFailed() {
        insertSendingNotification(9002L);
        repository.save(NotificationOutbox.pending(9002L, 1, AttemptTrigger.MANUAL, AT));
        var claim = claimOne(Duration.ofSeconds(1)).orElseThrow();
        for (int expiry = 0; expiry < 10; expiry++) {
            jdbcTemplate.update("UPDATE notification_outbox SET claimed_at="
                    + "TIMESTAMPADD(SECOND,-2,CURRENT_TIMESTAMP(6)) WHERE id=?", claim.outboxId());
            var next = claimOne(Duration.ofSeconds(1));
            if (expiry < 9) {
                assertThat(next).isEmpty();
                jdbcTemplate.update("UPDATE notification_outbox SET next_attempt_at="
                        + "TIMESTAMPADD(SECOND,-1,CURRENT_TIMESTAMP(6)) WHERE id=?", claim.outboxId());
                claim = claimOne(Duration.ofSeconds(1)).orElseThrow();
            } else {
                assertThat(next).isEmpty();
            }
        }

        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM notifications WHERE id=9002", String.class)).isEqualTo("FAILED");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT last_failure_reason FROM notifications WHERE id=9002", String.class))
                .isEqualTo("OUTBOX_PUBLISH_FAILED");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT resend_count FROM notifications WHERE id=9002", Integer.class)).isZero();
    }

    @Test
    void initialOutboxDeadLeavesPendingNotificationUnstarted() {
        insertPendingNotification(9003L);
        NotificationOutbox outbox = repository.save(NotificationOutbox.pending(
                9003L, 1, AttemptTrigger.INITIAL, AT));
        for (int failure = 0; failure < 10; failure++) {
            var claim = claimOne(Duration.ofSeconds(1)).orElseThrow();
            assertThat(repository.markFailed(outbox.id(), claim.claimToken(), Duration.ZERO)).isTrue();
        }

        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM notification_outbox WHERE id=?", String.class, outbox.id()))
                .isEqualTo("DEAD");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM notifications WHERE id=9003", String.class)).isEqualTo("PENDING");
    }

    @Test
    void completedSuccessWinsOverManualOutboxDeadCompensation() {
        insertSendingNotification(9004L);
        jdbcTemplate.update("""
                INSERT INTO notification_attempts (
                    notification_id, attempt_seq, `trigger`, result, failure_reason,
                    started_at, finished_at, created_at
                ) VALUES (9004, 1, 'MANUAL', 'SUCCESS', NULL,
                    CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6))
                """);
        NotificationOutbox outbox = repository.save(NotificationOutbox.pending(
                9004L, 1, AttemptTrigger.MANUAL, AT));
        for (int failure = 0; failure < 10; failure++) {
            var claim = claimOne(Duration.ofSeconds(1)).orElseThrow();
            assertThat(repository.markFailed(outbox.id(), claim.claimToken(), Duration.ZERO)).isTrue();
        }

        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM notifications WHERE id=9004", String.class)).isEqualTo("SENDING");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT resend_count FROM notifications WHERE id=9004", Integer.class)).isEqualTo(1);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT result FROM notification_attempts WHERE notification_id=9004 AND attempt_seq=1",
                String.class)).isEqualTo("SUCCESS");
    }

    @Test
    void completedFailureWinsOverManualOutboxDeadCompensationWithoutRefund() {
        insertSendingNotification(9005L);
        jdbcTemplate.update("""
                INSERT INTO notification_attempts (
                    notification_id, attempt_seq, `trigger`, result, failure_reason,
                    started_at, finished_at, created_at
                ) VALUES (9005, 1, 'MANUAL', 'FAILED', 'SEND_TIMEOUT',
                    CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6))
                """);
        NotificationOutbox outbox = repository.save(NotificationOutbox.pending(
                9005L, 1, AttemptTrigger.MANUAL, AT));
        for (int failure = 0; failure < 10; failure++) {
            var claim = claimOne(Duration.ofSeconds(1)).orElseThrow();
            assertThat(repository.markFailed(outbox.id(), claim.claimToken(), Duration.ZERO)).isTrue();
        }

        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM notifications WHERE id=9005", String.class)).isEqualTo("SENDING");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT last_failure_reason FROM notifications WHERE id=9005", String.class))
                .isNull();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT resend_count FROM notifications WHERE id=9005", Integer.class)).isEqualTo(1);
    }

    private void insertSendingNotification(long id) {
        jdbcTemplate.update("""
                INSERT INTO notifications (
                    id, coupon_id, member_id, issuance_id, channel, status, attempt_count,
                    resend_count, recipient_contact, message_body, created_at, updated_at)
                VALUES (?, 1, 1, ?, 'DEFAULT', 'SENDING', 1, 1, 'contact', 'message',
                        CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6))
                """, id, id);
    }

    private void insertPendingNotification(long id) {
        jdbcTemplate.update("""
                INSERT INTO notifications (
                    id, coupon_id, member_id, issuance_id, channel, status, attempt_count,
                    resend_count, recipient_contact, message_body, created_at, updated_at)
                VALUES (?, 1, 1, ?, 'DEFAULT', 'PENDING', 0, 0, 'contact', 'message',
                        CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6))
                """, id, id);
    }


    /**
     * 한 건만 집는다. 배치 API 를 그대로 쓰되 이 테스트들이 보는 단위가 한 건이라
     * 여기서만 좁힌다 — 포트에 편의 메서드를 남기면 <b>운영이 안 쓰는 API</b> 가 생긴다.
     */
    private java.util.Optional<com.kafkick.core.notification.domain.NotificationOutboxClaim>
            claimOne(Duration lease) {
        return repository.claimBatch(lease, 1).stream().findFirst();
    }
}
