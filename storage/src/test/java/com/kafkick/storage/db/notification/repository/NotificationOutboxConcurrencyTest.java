package com.kafkick.storage.db.notification.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
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

import com.kafkick.core.notification.NotificationOutboxRepository;
import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.notification.domain.NotificationOutbox;
import com.kafkick.storage.db.RepositoryTest;

@RepositoryTest
@Import(NotificationOutboxRepositoryImpl.class)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class NotificationOutboxConcurrencyTest {
    @Autowired NotificationOutboxRepository repository;
    @Autowired JdbcTemplate jdbcTemplate;

    @AfterEach
    void cleanCommittedFixture() {
        jdbcTemplate.update("DELETE FROM notification_outbox WHERE notification_id=100");
    }

    @Test
    void concurrentRelaysCannotClaimSameCommand() throws Exception {
        Instant now = Instant.parse("2026-08-27T00:00:00Z");
        repository.save(NotificationOutbox.pending(100L, 1, AttemptTrigger.INITIAL, now));
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var tasks = List.of(1, 2).stream().map(ignored -> executor.submit(() -> {
                ready.countDown();
                start.await();
                return repository.claimNext(Duration.ofMinutes(1));
            })).toList();
            ready.await();
            start.countDown();
            long claimed = 0;
            for (var task : tasks) if (task.get().isPresent()) claimed++;
            assertThat(claimed).isEqualTo(1);
        }
    }
}
