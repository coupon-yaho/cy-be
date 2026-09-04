package com.kafkick.storage.db.notification.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.notification.NotificationOutboxRepository;
import com.kafkick.core.notification.NotificationRejectedAuditWriter;
import com.kafkick.core.notification.NotificationRepository;
import com.kafkick.core.notification.NotificationResendAuditRepository;
import com.kafkick.core.notification.NotificationResendRejectedException;
import com.kafkick.core.notification.NotificationResendService;
import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.notification.domain.NotificationOutbox;
import com.kafkick.core.notification.domain.NotificationOutboxClaim;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.storage.db.RepositoryTest;

@RepositoryTest
@Import({
        NotificationRepositoryImpl.class,
        NotificationOutboxRepositoryImpl.class,
        NotificationResendAuditRepositoryImpl.class,
        TransactionalNotificationRejectedAuditWriter.class,
        NotificationResendTransactionIntegrationTest.Config.class
})
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class NotificationResendTransactionIntegrationTest {
    private static final Instant AT = Instant.parse("2026-08-30T00:00:00Z");
    private static final long FAILED_NOTIFICATION_ID = 9_101L;
    private static final long PENDING_NOTIFICATION_ID = 9_102L;

    @jakarta.annotation.Resource
    NotificationResendService service;
    @jakarta.annotation.Resource
    JdbcTemplate jdbcTemplate;

    @BeforeEach
    void insertNotifications() {
        jdbcTemplate.update("""
                INSERT INTO notifications (
                    id, coupon_id, member_id, issuance_id, channel, status, attempt_count,
                    resend_count, last_failure_reason, recipient_contact, message_body,
                    created_at, updated_at, failed_at)
                VALUES (?, 1, 1, ?, 'DEFAULT', 'FAILED', 1, 0, 'SEND_TIMEOUT',
                        'contact', 'message', ?, ?, ?)
                """, FAILED_NOTIFICATION_ID, FAILED_NOTIFICATION_ID, AT, AT, AT);
        jdbcTemplate.update("""
                INSERT INTO notifications (
                    id, coupon_id, member_id, issuance_id, channel, status, attempt_count,
                    resend_count, recipient_contact, message_body, created_at, updated_at)
                VALUES (?, 1, 1, ?, 'DEFAULT', 'PENDING', 0, 0,
                        'contact', 'message', ?, ?)
                """, PENDING_NOTIFICATION_ID, PENDING_NOTIFICATION_ID, AT, AT);
    }

    @AfterEach
    void cleanCommittedFixtures() {
        jdbcTemplate.update("DELETE FROM notification_outbox WHERE notification_id IN (?, ?)",
                FAILED_NOTIFICATION_ID, PENDING_NOTIFICATION_ID);
        jdbcTemplate.update("DELETE FROM notification_resend_audits WHERE notification_id IN (?, ?)",
                FAILED_NOTIFICATION_ID, PENDING_NOTIFICATION_ID);
        jdbcTemplate.update("DELETE FROM notifications WHERE id IN (?, ?)",
                FAILED_NOTIFICATION_ID, PENDING_NOTIFICATION_ID);
    }

    @Test
    void outboxFailureRollsBackNotificationClaimAndAcceptedAudit() {
        assertThatThrownBy(() -> service.resend(FAILED_NOTIFICATION_ID, 77L))
                .isInstanceOf(ForcedOutboxFailure.class);

        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM notifications WHERE id=?", String.class,
                FAILED_NOTIFICATION_ID)).isEqualTo("FAILED");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT attempt_count FROM notifications WHERE id=?", Integer.class,
                FAILED_NOTIFICATION_ID)).isEqualTo(1);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT resend_count FROM notifications WHERE id=?", Integer.class,
                FAILED_NOTIFICATION_ID)).isZero();
        assertThat(rowCount("notification_resend_audits", FAILED_NOTIFICATION_ID)).isZero();
        assertThat(rowCount("notification_outbox", FAILED_NOTIFICATION_ID)).isZero();
    }

    @Test
    void rejectedAuditCommitsDespiteOuterResendRollback() {
        assertThatThrownBy(() -> service.resend(PENDING_NOTIFICATION_ID, 77L))
                .isInstanceOf(NotificationResendRejectedException.class);

        assertThat(rowCount("notification_resend_audits", PENDING_NOTIFICATION_ID)).isEqualTo(1);
        assertThat(jdbcTemplate.queryForObject("""
                SELECT reject_code FROM notification_resend_audits
                 WHERE notification_id=?
                """, String.class, PENDING_NOTIFICATION_ID)).isEqualTo("ADMIN-006");
    }

    private int rowCount(String table, long notificationId) {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM " + table + " WHERE notification_id=?",
                Integer.class, notificationId);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class Config {
        @Bean
        TimeProvider timeProvider() {
            return new TimeProvider(Clock.fixed(AT, ZoneOffset.UTC));
        }

        @Bean
        NotificationOutboxRepository failingNotificationOutboxRepository(
                NotificationOutboxRepositoryImpl delegate) {
            return new FailingAfterSaveOutboxRepository(delegate);
        }

        @Bean
        NotificationResendService notificationResendService(
                NotificationRepository notifications,
                @Qualifier("failingNotificationOutboxRepository") NotificationOutboxRepository outboxes,
                NotificationResendAuditRepository audits,
                NotificationRejectedAuditWriter rejectedAudits,
                TimeProvider timeProvider) {
            return new NotificationResendService(notifications, outboxes, audits,
                    rejectedAudits, timeProvider);
        }
    }

    private static final class FailingAfterSaveOutboxRepository
            implements NotificationOutboxRepository {
        private final NotificationOutboxRepository delegate;

        private FailingAfterSaveOutboxRepository(NotificationOutboxRepository delegate) {
            this.delegate = delegate;
        }

        @Override
        public NotificationOutbox save(NotificationOutbox outbox) {
            delegate.save(outbox);
            throw new ForcedOutboxFailure();
        }

        @Override
        public Optional<AttemptTrigger> findTriggerByNotificationIdAndAttemptSeq(
                Long notificationId, int attemptSeq) {
            return delegate.findTriggerByNotificationIdAndAttemptSeq(notificationId, attemptSeq);
        }

        @Override
        public java.util.List<NotificationOutboxClaim> claimBatch(Duration lease, int max) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean markPublished(Long outboxId, String claimToken, Instant publishedAt) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean markFailed(Long outboxId, String claimToken, Duration retryDelay) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class ForcedOutboxFailure extends RuntimeException {
    }
}
