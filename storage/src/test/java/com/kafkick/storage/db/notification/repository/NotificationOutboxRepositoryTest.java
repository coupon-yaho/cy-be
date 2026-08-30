package com.kafkick.storage.db.notification.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
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
import com.kafkick.storage.db.RepositoryTest;

@RepositoryTest
@Import(NotificationOutboxRepositoryImpl.class)
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
        var claim = repository.claimNext(Duration.ofMinutes(1)).orElseThrow();
        assertThat(claim.outboxId()).isEqualTo(saved.id());
        assertThat(claim.requestedAt()).isEqualTo(AT);
        assertThat(repository.claimNext(Duration.ofMinutes(1))).isEmpty();
        assertThat(repository.markPublished(saved.id(), "wrong", AT.plusSeconds(1))).isFalse();
        assertThat(repository.markPublished(saved.id(), claim.claimToken(), AT.plusSeconds(1))).isTrue();
    }

    @Test
    void expiredClaimIsRecoveredAndPoisonCommandDoesNotBlockNext() {
        NotificationOutbox poison = repository.save(NotificationOutbox.pending(
                3L, 1, AttemptTrigger.INITIAL, AT));
        NotificationOutbox next = repository.save(NotificationOutbox.pending(
                4L, 1, AttemptTrigger.INITIAL, AT));
        var first = repository.claimNext(Duration.ofSeconds(1)).orElseThrow();
        jdbcTemplate.update("UPDATE notification_outbox SET claimed_at="
                + "TIMESTAMPADD(SECOND,-2,CURRENT_TIMESTAMP(6)) WHERE id=?", poison.id());
        var nextClaim = repository.claimNext(Duration.ofSeconds(1)).orElseThrow();
        assertThat(nextClaim.outboxId()).isEqualTo(next.id());
        assertThat(repository.markPublished(next.id(), nextClaim.claimToken(), AT.plusSeconds(1))).isTrue();
        assertThat(repository.claimNext(Duration.ofSeconds(1))).isEmpty();
        jdbcTemplate.update("UPDATE notification_outbox SET next_attempt_at="
                + "TIMESTAMPADD(SECOND,-1,CURRENT_TIMESTAMP(6)) WHERE id=?", poison.id());
        var recovered = repository.claimNext(Duration.ofSeconds(1)).orElseThrow();
        assertThat(recovered.outboxId()).isEqualTo(poison.id());
        assertThat(recovered.claimToken()).isNotEqualTo(first.claimToken());
        for (int failure = 0; failure < 9; failure++) {
            assertThat(repository.markFailed(poison.id(), recovered.claimToken(), Duration.ZERO))
                    .isTrue();
            if (failure < 8) {
                recovered = repository.claimNext(Duration.ofSeconds(1)).orElseThrow();
            }
        }
        assertThat(repository.claimNext(Duration.ofSeconds(1))).isEmpty();
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

    @Test
    void manualOutboxDeadReturnsSendingNotificationToFailed() {
        insertSendingNotification(9001L);
        NotificationOutbox outbox = repository.save(NotificationOutbox.pending(
                9001L, 1, AttemptTrigger.MANUAL, AT));
        for (int failure = 0; failure < 10; failure++) {
            var claim = repository.claimNext(Duration.ofSeconds(1)).orElseThrow();
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
        var claim = repository.claimNext(Duration.ofSeconds(1)).orElseThrow();
        for (int expiry = 0; expiry < 10; expiry++) {
            jdbcTemplate.update("UPDATE notification_outbox SET claimed_at="
                    + "TIMESTAMPADD(SECOND,-2,CURRENT_TIMESTAMP(6)) WHERE id=?", claim.outboxId());
            var next = repository.claimNext(Duration.ofSeconds(1));
            if (expiry < 9) {
                assertThat(next).isEmpty();
                jdbcTemplate.update("UPDATE notification_outbox SET next_attempt_at="
                        + "TIMESTAMPADD(SECOND,-1,CURRENT_TIMESTAMP(6)) WHERE id=?", claim.outboxId());
                claim = repository.claimNext(Duration.ofSeconds(1)).orElseThrow();
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
            var claim = repository.claimNext(Duration.ofSeconds(1)).orElseThrow();
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
            var claim = repository.claimNext(Duration.ofSeconds(1)).orElseThrow();
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
            var claim = repository.claimNext(Duration.ofSeconds(1)).orElseThrow();
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

}
