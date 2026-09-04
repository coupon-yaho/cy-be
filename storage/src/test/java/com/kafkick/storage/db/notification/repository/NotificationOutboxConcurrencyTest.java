package com.kafkick.storage.db.notification.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
import com.kafkick.core.notification.domain.NotificationOutboxClaim;
import com.kafkick.core.notification.retry.NotificationRetryBackOffConfig;
import com.kafkick.storage.db.RepositoryTest;

@RepositoryTest
// 지연 정책은 core 가 소유한다(CY-907). @DataJpaTest 는 그 @Configuration 을 스캔하지 않으므로
// 여기서 명시로 붙인다 — 안 붙이면 어댑터가 백오프를 못 받아 컨텍스트가 안 뜬다.
@Import({NotificationOutboxRepositoryImpl.class, NotificationRetryBackOffConfig.class})
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class NotificationOutboxConcurrencyTest {
    private static final Duration LEASE = Duration.ofMinutes(1);
    private static final Instant NOW = Instant.parse("2026-08-27T00:00:00Z");

    @Autowired NotificationOutboxRepository repository;
    @Autowired JdbcTemplate jdbcTemplate;

    @AfterEach
    void cleanCommittedFixture() {
        jdbcTemplate.update("DELETE FROM notification_outbox WHERE notification_id=100");
    }

    /** 같은 명령이 두 워커에 배정되면 외부로 두 번 나간다. 이 테스트가 그것을 막는다. */
    @Test
    void concurrentRelaysCannotClaimSameCommand() throws Exception {
        repository.save(NotificationOutbox.pending(100L, 1, AttemptTrigger.INITIAL, NOW));
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var tasks = List.of(1, 2).stream().map(ignored -> executor.submit(() -> {
                ready.countDown();
                start.await();
                return repository.claimBatch(LEASE, 64);
            })).toList();
            ready.await();
            start.countDown();
            long claimed = 0;
            for (var task : tasks) claimed += task.get().size();
            assertThat(claimed).isEqualTo(1);
        }
    }

    /**
     * <b>배치로 집어도 겹치면 안 된다.</b> 한 건씩 집을 때는 겹침이 구조적으로 어려웠지만,
     * {@code SKIP LOCKED} 는 <b>잠긴 행을 조용히 건너뛰는</b> 방식이라 겹침이 없다는 것을
     * 따로 확인해야 한다 — 조용히 건너뛴다는 성질은 조용히 <b>겹치는</b> 것과 증상이 다르지만
     * 둘 다 눈에 안 보인다.
     */
    @Test
    void concurrentBatchClaimsNeverOverlap() throws Exception {
        int commands = 60;
        for (int seq = 1; seq <= commands; seq++) {
            repository.save(NotificationOutbox.pending(100L, seq, AttemptTrigger.INITIAL, NOW));
        }

        int workers = 6;
        CountDownLatch ready = new CountDownLatch(workers);
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(workers)) {
            var tasks = new ArrayList<java.util.concurrent.Future<List<NotificationOutboxClaim>>>();
            for (int i = 0; i < workers; i++) {
                tasks.add(executor.submit(() -> {
                    ready.countDown();
                    start.await();
                    return repository.claimBatch(LEASE, 16);
                }));
            }
            ready.await();
            start.countDown();

            List<Long> all = new ArrayList<>();
            for (var task : tasks) {
                task.get().forEach(claim -> all.add(claim.outboxId()));
            }
            Set<Long> distinct = new HashSet<>(all);

            assertThat(all)
                    .as("같은 명령이 두 워커에 배정됐습니다 — 외부로 두 번 나갑니다")
                    .hasSameSizeAs(distinct);
            assertThat(all).isNotEmpty();
        }
    }

    /** 요청한 수보다 많이 집으면 lease 를 넘겨 잡는 것이 생긴다. */
    @Test
    void neverClaimsMoreThanAsked() {
        for (int seq = 1; seq <= 20; seq++) {
            repository.save(NotificationOutbox.pending(100L, seq, AttemptTrigger.INITIAL, NOW));
        }

        assertThat(repository.claimBatch(LEASE, 5)).hasSize(5);
    }

    /** 행마다 토큰이 달라야 한다 — {@code uk_notification_outbox_claim_token} 이 유일 제약이고, 펜싱도 행 단위다. */
    @Test
    void givesEveryClaimItsOwnFencingToken() {
        for (int seq = 1; seq <= 10; seq++) {
            repository.save(NotificationOutbox.pending(100L, seq, AttemptTrigger.INITIAL, NOW));
        }

        List<NotificationOutboxClaim> claims = repository.claimBatch(LEASE, 10);

        assertThat(claims).hasSize(10);
        assertThat(claims).extracting(NotificationOutboxClaim::claimToken)
                .doesNotHaveDuplicates()
                .doesNotContainNull();
    }

    @Test
    void rejectsNonPositiveBatchSize() {
        assertThat(org.assertj.core.api.Assertions
                .catchThrowable(() -> repository.claimBatch(LEASE, 0)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
