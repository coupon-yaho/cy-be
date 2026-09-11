// 발행을 마치고 기록하려는데 선점이 이미 회수돼 있던 경우를 셉니다.
package com.kafkick.storage.db.notification.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import io.micrometer.core.instrument.MeterRegistry;

import com.kafkick.core.notification.NotificationOutboxRepository;
import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.notification.domain.NotificationOutbox;
import com.kafkick.core.notification.domain.NotificationOutboxClaim;
import com.kafkick.core.notification.retry.NotificationRetryBackOffConfig;
import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.storage.db.RepositoryTest;

/**
 * <b>펜스가 무너진 것을 어댑터가 센다.</b>
 *
 * <p>{@code markPublished} 가 0행을 돌려주는 것은 {@code claim_token} 이 안 맞는다는
 * 뜻이다 — 우리가 발행하는 사이 lease 가 만료돼 남이 가져갔다. <b>우리는 이미
 * 발행했으므로</b> 그 명령은 대개 한 번 더 나간다.
 *
 * <p><b>왜 릴레이가 아니라 여기서 세나.</b> {@code NotificationOutboxMeter} 가
 * <i>"결과를 아는 곳이 여기뿐"</i> 이라고 적어 뒀고, 그 결과가 바로
 * {@code markPublished} 안의 갱신 행 수다. 한때 릴레이에서 세려 했는데
 * <b>규칙이 지정한 자리에 같은 정보가 이미 있었다.</b>
 *
 * <p>모의 객체로는 이 축을 못 잰다 — 0행이 나오는 조건 자체가 SQL 의
 * {@code claim_token} 검사라서, 진짜 DB 여야 한다.
 */
@RepositoryTest
@Import({NotificationOutboxRepositoryImpl.class, NotificationRetryBackOffConfig.class,
        OutboxMeterTestConfig.class})
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class NotificationOutboxFenceLostTest {

    private static final Instant NOW = Instant.parse("2026-09-11T00:00:00Z");
    private static final long NOTIFICATION_ID = 7_777L;
    private static final Duration LEASE = Duration.ofSeconds(30);

    @Autowired NotificationOutboxRepository repository;
    @Autowired JdbcTemplate jdbcTemplate;
    @Autowired MeterRegistry registry;

    @BeforeEach
    @AfterEach
    void clean() {
        jdbcTemplate.update("DELETE FROM notification_outbox WHERE notification_id=?",
                NOTIFICATION_ID);
    }

    /**
     * 선점 → lease 만료 → 남이 회수 → <b>원래 워커가 기록을 시도</b>.
     *
     * <p>{@code claimed_at} 을 과거로 밀어 만료를 만든다. 그다음 선점이
     * {@code recoverExpiredClaims} 를 먼저 돌리므로 그 호출이 회수 역할을 한다.
     */
    @Test
    @DisplayName("회수된 뒤에 기록하려 하면 펜싱 손실로 센다")
    void aRecordAfterRecoveryCountsAsALostFence() {
        double before = fenceLost();
        repository.save(NotificationOutbox.pending(
                NOTIFICATION_ID, 1, AttemptTrigger.INITIAL, NOW));
        List<NotificationOutboxClaim> mine = repository.claimBatch(LEASE, 1);
        assertThat(mine).as("전제 — 한 건을 집어야 한다").hasSize(1);

        expireLease();
        repository.claimBatch(LEASE, 1);   // 회수가 여기서 돈다

        boolean recorded = repository.markPublished(
                mine.get(0).outboxId(), mine.get(0).claimToken(), NOW);

        assertThat(recorded).as("토큰이 안 맞으므로 갱신이 안 먹는다").isFalse();
        assertThat(fenceLost() - before)
                .as("이 값이 없으면 발행이 두 번 나가도 아무 데도 안 남는다")
                .isEqualTo(1);
    }

    /** 정상 경로에서는 안 센다 — 늘 오르면 그 값이 아무 말도 못 한다. */
    @Test
    @DisplayName("선점을 지킨 채 기록하면 안 센다")
    void aRecordThatKeepsItsFenceIsNotCounted() {
        double before = fenceLost();
        repository.save(NotificationOutbox.pending(
                NOTIFICATION_ID, 1, AttemptTrigger.INITIAL, NOW));
        List<NotificationOutboxClaim> mine = repository.claimBatch(LEASE, 1);

        boolean recorded = repository.markPublished(
                mine.get(0).outboxId(), mine.get(0).claimToken(), NOW);

        assertThat(recorded).isTrue();
        assertThat(fenceLost() - before).isZero();
    }

    /**
     * <b>기록이 아예 안 불린 경로에서는 안 센다.</b>
     *
     * <p>이 카운터의 뜻(<i>"우리가 발행한 뒤에 선점을 잃었다"</i>)을 지키는 성질이
     * 그것이다. 발행이 던져 {@code markPublished} 에 닿지도 않았는데 세면, 값이
     * <b>브로커 장애까지 같이 세어</b> lease 지표가 아니게 된다.
     */
    @Test
    @DisplayName("기록을 시도하지 않으면 펜싱 손실도 없다")
    void noRecordAttemptMeansNoLostFence() {
        double before = fenceLost();
        repository.save(NotificationOutbox.pending(
                NOTIFICATION_ID, 1, AttemptTrigger.INITIAL, NOW));
        List<NotificationOutboxClaim> mine = repository.claimBatch(LEASE, 1);

        // 발행이 던진 상황 — 릴레이는 markPublished 를 안 부르고 되돌린다.
        repository.markFailed(mine.get(0).outboxId(), mine.get(0).claimToken(),
                Duration.ofSeconds(1), com.kafkick.core.notification.OutboxRetryReason.PUBLISH_FAILED);

        assertThat(fenceLost() - before).isZero();
    }

    private void expireLease() {
        jdbcTemplate.update("UPDATE notification_outbox"
                + " SET claimed_at = claimed_at - INTERVAL 3600 SECOND"
                + " WHERE notification_id=?", NOTIFICATION_ID);
    }

    private double fenceLost() {
        return registry.get(DomainMeterNames.OUTBOX_FENCE_LOST).counter().count();
    }
}
