package com.kafkick.storage.db.notification.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.jdbc.core.JdbcTemplate;

import com.kafkick.core.notification.NotificationRepository;
import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.notification.domain.Notification;
import com.kafkick.storage.db.RepositoryTest;

@RepositoryTest
@Import(NotificationRepositoryImpl.class)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class NotificationCasConcurrencyTest {
    @Autowired NotificationRepository repository;
    @Autowired JdbcTemplate jdbcTemplate;

    @AfterEach
    void cleanCommittedFixture() {
        jdbcTemplate.update("DELETE FROM notifications WHERE issuance_id=200");
    }

    @Test
    void concurrentConsumersClaimOneAttemptSequenceOnly() throws Exception {
        Instant at = Instant.parse("2026-08-27T00:00:00Z");
        Notification pending = repository.save(
                Notification.pending(1L, 200L, 200L, "recipient", "message", at));
        Notification sending = pending.startSending(AttemptTrigger.INITIAL, at);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var tasks = List.of(1, 2).stream().map(ignored -> executor.submit(() -> {
                ready.countDown();
                start.await();
                return repository.saveIfStatus(sending, pending.status(), pending.attemptCount());
            })).toList();
            ready.await();
            start.countDown();
            int succeeded = 0;
            for (var task : tasks) if (task.get()) succeeded++;
            assertThat(succeeded).isEqualTo(1);
        }
        assertThat(repository.findById(pending.id()).orElseThrow().attemptCount()).isEqualTo(1);
    }
}
