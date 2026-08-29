package com.kafkick.storage.db.notification.repository;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

import com.kafkick.core.notification.NotificationAttemptRepository;
import com.kafkick.core.notification.domain.AttemptResult;
import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.notification.domain.NotificationAttempt;
import com.kafkick.storage.db.RepositoryTest;

@RepositoryTest
@Import(NotificationAttemptRepositoryImpl.class)
class NotificationAttemptRepositoryTest {
    @Autowired NotificationAttemptRepository repository;
    @Test
    void completedAttemptIsInsertOnly() {
        Instant at = Instant.parse("2026-08-27T00:00:00Z");
        NotificationAttempt saved = repository.save(new NotificationAttempt(null, 1L, 1,
                AttemptTrigger.INITIAL, AttemptResult.SUCCESS, null, at, at, at));
        assertThat(saved.createdAt()).isEqualTo(at);
        assertThatThrownBy(() -> repository.save(saved)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void completedAttemptInsertHasSingleWinnerWithoutUniqueKeyException() {
        Instant at = Instant.parse("2026-08-27T00:00:00Z");
        NotificationAttempt attempt = new NotificationAttempt(null, 2L, 1,
                AttemptTrigger.INITIAL, AttemptResult.SUCCESS, null, at, at, at);

        assertThat(repository.saveIfAbsent(attempt)).isTrue();
        assertThat(repository.saveIfAbsent(attempt)).isFalse();
        assertThat(repository.findByNotificationId(2L)).hasSize(1);
    }
}
