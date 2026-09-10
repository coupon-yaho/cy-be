package com.kafkick.storage.db.notification.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.Locale;
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
import com.kafkick.core.notification.domain.NotificationOutboxClaim;
import com.kafkick.core.notification.retry.NotificationRetryBackOffConfig;
import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.storage.db.RepositoryTest;

/**
 * <b>한 종류의 적체가 나머지를 굶기지 않는다.</b>
 *
 * <p>고치기 전에는 선점이 {@code ORDER BY next_attempt_at, id} 하나였다. 운영자가 방금
 * 누른 재발송은 {@code next_attempt_at = now} 라 <b>이미 밀린 자동 건 전부보다 뒤에
 * 선다</b> — 실측으로 due {@code INITIAL} 5,000건 앞에서 {@code MANUAL} 1건이
 * <b>79 회차째</b>에 잡혔다.
 *
 * <p>여기 있는 것은 그 성질을 <b>양방향</b>으로 못 박는다. 한 방향만 걸면
 * <i>"{@code MANUAL} 을 앞세운다"</i> 는 반대 결함이 그대로 통과한다 — 그것도
 * 기능명세가 금지하는 상태다.
 */
@RepositoryTest
@Import({NotificationOutboxRepositoryImpl.class, NotificationRetryBackOffConfig.class,
        OutboxMeterTestConfig.class})
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class NotificationOutboxQuotaTest {

    private static final Duration LEASE = Duration.ofMinutes(1);
    private static final int BATCH = 64;

    /** 한쪽 적체의 크기. {@link #BATCH} 보다 훨씬 커야 "줄 서 있다" 가 된다. */
    private static final int BACKLOG = 2_000;

    /** 이 클래스가 쓰는 구간. 형제와 안 겹치게 좁게 잡고 <b>그 구간만</b> 지운다. */
    private static final long ID_BASE = 7_100_000L;
    private static final long ID_MAX = 7_199_999L;

    /** 두 번째 종류를 심는 자리. {@link #ID_MAX} 안에 있어야 정리가 샌다. */
    private static final long SECOND_BLOCK = 50_000L;

    @Autowired NotificationOutboxRepository repository;
    @Autowired JdbcTemplate jdbcTemplate;
    @Autowired MeterRegistry registry;

    /**
     * <b>앞뒤로 지운다.</b> {@code @AfterEach} 만 두면 앞 실행이 비정상 종료했을 때
     * 남은 행을 다음 실행이 본다 — 컨테이너는 JVM 보다 오래 산다.
     * {@code MySqlContainerConfig} 가 "데이터를 읽는 테스트는 비우고 시작하라" 고
     * 못 박아 둔 규칙이고, 형제 {@code BacklogPlanContractTest} 도 그렇게 한다.
     */
    @BeforeEach
    @AfterEach
    void clean() {
        jdbcTemplate.update(
                "DELETE FROM notification_outbox WHERE notification_id BETWEEN ? AND ?",
                ID_BASE, ID_MAX);
    }

    @Test
    @DisplayName("자동 발송이 2,000건 밀려 있어도 운영자 재발송은 같은 회차에 잡힌다")
    void manualIsClaimedDespiteAnInitialBacklog() {
        seed(AttemptTrigger.INITIAL, BACKLOG, 0);
        seed(AttemptTrigger.MANUAL, 1, SECOND_BLOCK);

        List<NotificationOutboxClaim> claims = repository.claimBatch(LEASE, BATCH);

        assertThat(kinds(claims, AttemptTrigger.MANUAL))
                .as("MANUAL 이 이번 회차에 안 잡히면 ⌈%d/%d⌉ 주기를 기다립니다",
                        BACKLOG, BATCH)
                .isEqualTo(1);
    }

    @Test
    @DisplayName("반대로 재발송이 2,000건 밀려 있어도 자동 발송은 같은 회차에 잡힌다")
    void initialIsClaimedDespiteAManualBacklog() {
        seed(AttemptTrigger.MANUAL, BACKLOG, 0);
        seed(AttemptTrigger.INITIAL, 1, SECOND_BLOCK);

        List<NotificationOutboxClaim> claims = repository.claimBatch(LEASE, BATCH);

        assertThat(kinds(claims, AttemptTrigger.INITIAL))
                .as("MANUAL 을 우선순위로 앞세우면 여기서 굶습니다")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("둘 다 밀려 있으면 각자 최소 절반씩 가져간다")
    void neitherKindTakesMoreThanItsShareWhenBothAreBacked() {
        seed(AttemptTrigger.INITIAL, BACKLOG, 0);
        seed(AttemptTrigger.MANUAL, BACKLOG, SECOND_BLOCK);

        List<NotificationOutboxClaim> claims = repository.claimBatch(LEASE, BATCH);

        assertThat(claims).hasSize(BATCH);
        assertThat(kinds(claims, AttemptTrigger.INITIAL)).isGreaterThanOrEqualTo(BATCH / 2);
        assertThat(kinds(claims, AttemptTrigger.MANUAL)).isGreaterThanOrEqualTo(BATCH / 2);
    }

    /**
     * <b>몫이 처리량을 깎으면 안 된다.</b> 상대가 없을 때 자기 몫만 집고 마는 구현은
     * 굶는 것을 고치는 대신 <b>평상시를 절반으로 만든다</b> — 평상시가 정확히 이 형상이다
     * ({@code MANUAL} 은 사람이 누를 때만 생긴다).
     *
     * <p>id 가 겹치지 않는 것까지 본다. 남은 자리를 커서 없이 이어 집으면
     * <b>같은 앞머리를 다시 집어</b> 여기서 32건이 된다.
     *
     * <p><b>두 회차를 본다.</b> 먼저 몫을 떼는 종류가 회차마다 뒤바뀌므로, 한 회차만
     * 보면 이 클래스의 <b>실행 순서에 따라</b> 빈 쪽이 앞설 때만 결함이 드러난다 —
     * 두 회차면 어느 패리티에서 시작하든 {@code INITIAL} 이 앞서는 회차가 반드시 낀다.
     */
    @Test
    @DisplayName("한 종류만 밀려 있으면 두 회차 모두 그 종류가 배치를 다 쓴다")
    void oneKindAloneStillFillsTheBatch() {
        seed(AttemptTrigger.INITIAL, BACKLOG, 0);
        List<Long> expectedFirst = dueOrder(BATCH);

        List<Long> first = ids(repository.claimBatch(LEASE, BATCH));
        List<Long> second = ids(repository.claimBatch(LEASE, BATCH));

        assertThat(first).as("1 회차").hasSize(BATCH).doesNotHaveDuplicates();
        assertThat(second).as("2 회차").hasSize(BATCH).doesNotHaveDuplicates();
        assertThat(first)
                .as("줄 앞에서부터 집어야 합니다 — id 순으로 집으면 여기서 갈립니다")
                .containsExactlyInAnyOrderElementsOf(expectedFirst);
        assertThat(second)
                .as("1 회차가 이미 집은 행을 다시 집으면 같은 알림이 두 번 나갑니다")
                .doesNotContainAnyElementsOf(first);
    }

    /**
     * <b>백프레셔가 배치를 1 로 자르는 순간을 태운다.</b> 자리가 하나면 몫을 나눌 수가
     * 없어, 순서가 고정이면 그 회차들 동안 뒤쪽은 <b>영영</b> 안 잡힌다.
     */
    @Test
    @DisplayName("배치가 1이면 회차마다 종류가 번갈아 잡힌다")
    void aSingleSlotAlternatesBetweenKinds() {
        // 앞선 쪽에 여러 건을 둔다. 한 건씩만 두면 <b>먼저 집힌 것이 사라져</b>
        // 다음 회차에 남은 종류가 저절로 잡힌다 — 고치기 전 구현도 통과한다.
        seed(AttemptTrigger.INITIAL, 5, 0);
        seed(AttemptTrigger.MANUAL, 5, SECOND_BLOCK);

        List<NotificationOutboxClaim> first = repository.claimBatch(LEASE, 1);
        List<NotificationOutboxClaim> second = repository.claimBatch(LEASE, 1);

        assertThat(first).hasSize(1);
        assertThat(second).hasSize(1);
        assertThat(first.get(0).trigger())
                .as("자리가 하나일 때 순서가 고정이면 뒤쪽은 영영 안 잡힙니다")
                .isNotEqualTo(second.get(0).trigger());
    }

    /**
     * <b>양쪽 델타를 다 본다.</b> {@code MANUAL} 만 보면 "{@code MANUAL} 만 센다" 는
     * 구현이 통과한다 — 그러면 {@code initial} 시계열이 영원히 0 인데, 그것이
     * {@link DomainMeterNames#OUTBOX_CLAIMED} javadoc 이 금지한 상태다.
     *
     * <p>{@code INITIAL} 이 61 인 것은 <b>회전 패리티와 무관하다</b>:
     * 앞선 쪽이 {@code INITIAL} 이면 32(몫)+29(이어받기), {@code MANUAL} 이면
     * 3 을 뗀 나머지 61 — 어느 쪽이든 64-3 이다.
     */
    @Test
    @DisplayName("집힌 수를 종류별로 센다")
    void countsClaimsByKind() {
        double beforeManual = claimedCount(AttemptTrigger.MANUAL);
        double beforeInitial = claimedCount(AttemptTrigger.INITIAL);
        seed(AttemptTrigger.INITIAL, BACKLOG, 0);
        seed(AttemptTrigger.MANUAL, 3, SECOND_BLOCK);

        repository.claimBatch(LEASE, BATCH);

        assertThat(claimedCount(AttemptTrigger.MANUAL) - beforeManual).isEqualTo(3);
        assertThat(claimedCount(AttemptTrigger.INITIAL) - beforeInitial).isEqualTo(BATCH - 3);
    }

    private double claimedCount(AttemptTrigger kind) {
        return registry.get(DomainMeterNames.OUTBOX_CLAIMED)
                .tag(DomainMeterNames.TAG_TRIGGER, kind.name().toLowerCase(Locale.ROOT))
                .counter().count();
    }

    private long kinds(List<NotificationOutboxClaim> claims, AttemptTrigger kind) {
        return claims.stream().filter(c -> c.trigger() == kind).count();
    }

    private List<Long> ids(List<NotificationOutboxClaim> claims) {
        return claims.stream().map(NotificationOutboxClaim::outboxId).toList();
    }

    /**
     * due 인 {@code PENDING} 행을 심는다.
     *
     * <p><b>{@code id} 와 {@code next_attempt_at} 을 일부러 어긋나게 둔다.</b> 두 축이
     * 같은 방향이면 {@code ORDER BY next_attempt_at, id} 를 <b>{@code ORDER BY id} 로
     * 바꿔도 결과가 똑같아</b> 어느 테스트도 그 변경을 못 잡는다. 운영에서는
     * {@code markFailed} 가 {@code next_attempt_at} 을 앞으로 밀어 두 축이 갈린다.
     *
     * <p>첫 블록({@code idOffset == 0})은 {@code [now-3600s, now-61s]} 에 흩고,
     * 두 번째 블록은 {@code now-1s} 에 몰아 둔다 — <b>나중 블록이 정렬에서 뒤에
     * 서야</b> "줄 서 있다" 가 재현된다.
     */
    private void seed(AttemptTrigger kind, int count, long idOffset) {
        boolean scattered = idOffset == 0;
        jdbcTemplate.batchUpdate("""
                INSERT INTO notification_outbox
                       (notification_id, attempt_seq, `trigger`, status, failure_count,
                        next_attempt_at, created_at)
                VALUES (?, 1, ?, 'PENDING', 0,
                        CURRENT_TIMESTAMP(6) - INTERVAL ? SECOND, NOW(6))
                """, new org.springframework.jdbc.core.BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws java.sql.SQLException {
                ps.setLong(1, ID_BASE + idOffset + i);
                ps.setString(2, kind.name());
                ps.setInt(3, scattered ? 61 + (int) ((long) i * 7919 % 3540) : 1);
            }

            @Override
            public int getBatchSize() {
                return count;
            }
        });
    }

    /**
     * 지금 줄의 <b>앞에서부터</b> {@code limit} 건의 id — 선점이 집어야 할 바로 그 집합.
     *
     * <p>테스트가 기대값을 손으로 적지 않고 <b>DB 에게 물어본다.</b> 흩어 심은 순서를
     * 테스트가 다시 계산하면 그 계산이 구현과 같이 틀릴 수 있다.
     */
    private List<Long> dueOrder(int limit) {
        return jdbcTemplate.queryForList("""
                SELECT id FROM notification_outbox
                 WHERE status='PENDING' AND next_attempt_at <= CURRENT_TIMESTAMP(6)
                   AND notification_id BETWEEN ? AND ?
                 ORDER BY next_attempt_at, id
                 LIMIT ?
                """, Long.class, ID_BASE, ID_MAX, limit);
    }
}
