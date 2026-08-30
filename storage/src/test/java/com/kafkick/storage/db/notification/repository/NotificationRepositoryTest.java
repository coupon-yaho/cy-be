package com.kafkick.storage.db.notification.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;

import jakarta.persistence.EntityManager;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import com.kafkick.core.notification.NotificationRepository;
import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.notification.domain.Notification;
import com.kafkick.core.notification.domain.NotifyFailureReason;
import com.kafkick.storage.db.RepositoryTest;
import com.kafkick.storage.db.notification.entity.NotificationEntity;

@RepositoryTest
@Import(NotificationRepositoryImpl.class)
class NotificationRepositoryTest {
    private static final Instant AT = Instant.parse("2026-08-27T00:00:00Z");

    @Autowired NotificationRepository repository;
    @Autowired NotificationJpaRepository jpaRepository;
    @Autowired EntityManager entityManager;
    @Autowired JdbcTemplate jdbcTemplate;

    @Test
    void staleAttemptSnapshotCannotClaimAfterStatusCyclesBack() {
        Notification pending = repository.save(pending(1L));
        Notification initialSending = pending.startSending(AttemptTrigger.INITIAL, AT);
        assertThat(repository.saveIfStatus(initialSending, pending.status(), 0, 0)).isTrue();
        Notification storedFailed = initialSending.markFailed(NotifyFailureReason.SEND_TIMEOUT, AT);
        assertThat(repository.saveIfStatus(storedFailed, initialSending.status(), 1, 0)).isTrue();

        Notification firstClaim = storedFailed.startSending(AttemptTrigger.MANUAL, AT);
        Notification staleClaim = storedFailed.startSending(AttemptTrigger.MANUAL, AT);
        assertThat(repository.saveIfStatus(firstClaim, storedFailed.status(), 1, 0)).isTrue();
        Notification failedAfterManual = firstClaim.markFailed(
                NotifyFailureReason.SEND_TIMEOUT, AT);
        assertThat(repository.saveIfStatus(
                failedAfterManual, firstClaim.status(), 2, 1)).isTrue();
        assertThat(repository.saveIfStatus(staleClaim, storedFailed.status(), 1, 0)).isFalse();

        Notification secondManual = failedAfterManual.startSending(AttemptTrigger.MANUAL, AT);
        assertThat(repository.saveIfStatus(secondManual, failedAfterManual.status(),
                failedAfterManual.attemptCount(), 0)).isFalse();
    }

    @Test
    void existingNotificationCannotBypassCasAndUnrelatedEntityStaysManaged() {
        Notification first = repository.save(pending(10L));
        Notification second = repository.save(pending(11L));
        NotificationEntity unrelated = jpaRepository.findById(second.id()).orElseThrow();

        assertThatThrownBy(() -> repository.save(first))
                .isInstanceOf(IllegalArgumentException.class);

        Notification sending = first.startSending(AttemptTrigger.INITIAL, AT);
        assertThat(repository.saveIfStatus(sending, first.status(), first.attemptCount(), first.resendCount())).isTrue();
        assertThat(entityManager.contains(unrelated)).isTrue();
    }

    @Test
    void failureFirstPageSupportsNullCursorAndReturnsNoPiiProjection() {
        Notification pending = repository.save(pending(20L));
        Notification sending = pending.startSending(AttemptTrigger.INITIAL, AT);
        assertThat(repository.saveIfStatus(sending, pending.status(), 0, 0)).isTrue();
        Notification failed = sending.markFailed(NotifyFailureReason.SEND_TIMEOUT, AT.plusSeconds(1));
        assertThat(repository.saveIfStatus(failed, sending.status(), 1, 0)).isTrue();
        jdbcTemplate.update("UPDATE notifications SET recipient_contact='contact',"
                + " message_body='message' WHERE id=?", pending.id());
        entityManager.clear();

        assertThat(repository.findFailuresBeforeId(null, 10))
                .singleElement()
                .satisfies(item -> {
                    assertThat(item.notificationId()).isEqualTo(pending.id());
                    assertThat(item.reason()).isEqualTo(NotifyFailureReason.SEND_TIMEOUT);
                });
        assertThat(repository.findFailuresBeforeId(pending.id(), 10)).isEmpty();
    }

    @Test
    void storesRecipientAndMessageForPrototypeSender() {
        Notification saved = repository.save(Notification.pending(
                1L, 30L, 30L, "010-1234-5678", "private-message", AT));
        String contact = jdbcTemplate.queryForObject(
                "SELECT recipient_contact FROM notifications WHERE id=?", String.class, saved.id());
        String body = jdbcTemplate.queryForObject(
                "SELECT message_body FROM notifications WHERE id=?", String.class, saved.id());

        assertThat(contact).isEqualTo("010-1234-5678");
        assertThat(body).isEqualTo("private-message");
        assertThat(repository.findById(saved.id()).orElseThrow().recipientContact())
                .isEqualTo("010-1234-5678");
        assertThat(saved.createdAt()).isEqualTo(AT);
        assertThat(saved.updatedAt()).isEqualTo(AT);
    }

    private static Notification pending(long issuanceId) {
        return Notification.pending(1L, issuanceId, issuanceId, "recipient", "message", AT);
    }
}
