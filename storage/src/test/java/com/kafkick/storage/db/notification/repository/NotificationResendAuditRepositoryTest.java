package com.kafkick.storage.db.notification.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

import com.kafkick.core.notification.NotificationResendAuditRepository;
import com.kafkick.core.notification.domain.NotificationResendAudit;
import com.kafkick.storage.db.RepositoryTest;

@RepositoryTest
@Import(NotificationResendAuditRepositoryImpl.class)
class NotificationResendAuditRepositoryTest {
    private static final Instant AT = Instant.parse("2026-08-27T00:00:00Z");

    @Autowired NotificationResendAuditRepository repository;
    @Test
    void latestAcceptedIgnoresNewerRejectedAuditAndRowsAreInsertOnly() {
        NotificationResendAudit accepted = repository.save(
                new NotificationResendAudit(null, 1L, 1, 9L, AT, true, null, AT));
        assertThat(accepted.createdAt()).isEqualTo(AT);
        repository.save(new NotificationResendAudit(null, 1L, null, 9L,
                AT.plusSeconds(1), false, "ADMIN-006", AT.plusSeconds(1)));

        assertThat(repository.findLatestAcceptedByNotificationId(1L))
                .contains(accepted);
        assertThatThrownBy(() -> repository.save(accepted))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
