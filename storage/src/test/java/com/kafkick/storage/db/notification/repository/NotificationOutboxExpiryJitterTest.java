package com.kafkick.storage.db.notification.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import com.kafkick.core.notification.OutboxRetryReason;
import com.kafkick.core.notification.NotificationOutboxRepository;
import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.notification.domain.NotificationOutbox;
import com.kafkick.core.notification.retry.FullJitterBackOff;
import com.kafkick.storage.db.RepositoryTest;

/**
 * <b>한꺼번에 만료된 것들이 같은 시각으로 돌아오면 안 된다.</b>
 *
 * <p>CY-903 은 발행 실패 경로만 흩뜨렸고 lease 만료 회수는 고정 1초로 남아 있었다.
 * 그런데 <b>이쪽이 더 잘 뭉친다</b> — 발행 실패는 확률적으로 흩어져 나지만, 릴레이가
 * 죽거나 재기동이 느리면 <b>인플라이트 lease 가 한꺼번에</b> 만료되고 그것들이 전부 같은
 * 창으로 돌아온다. 돌아온 것들이 다시 같이 실패하면 같은 뭉침이 반복된다.
 *
 * <h2>첫 판은 아무것도 못 재고 있었다</h2>
 *
 * <p>처음에는 되돌아온 {@code next_attempt_at} 이 <b>서로 다른지</b>만 봤다. 그런데
 * 되돌리는 문장이 행마다 따로 돌고 {@code CURRENT_TIMESTAMP(6)} 은 <b>문장마다 다르다</b> —
 * 고정 1초를 써도 절대 시각은 마이크로초 단위로 갈린다. 고정 지연으로 되돌려도, 배치
 * 전체에 한 값을 재사용해도 <b>둘 다 통과했다.</b> 재고 있던 것은 지터가 아니라 시계였다.
 *
 * <h2>두 번째 판도 완전하지 않았다</h2>
 *
 * <p>그다음에는 읽는 시점 기준의 <b>남은 초</b>가 서로 다른지를 봤다. 그것도 부족하다 —
 * 되돌리는 {@code UPDATE} 가 행마다 따로 돌므로, <b>그 문장들이 여러 초에 걸쳐 실행되면</b>
 * 고정 지연으로도 초 버킷이 갈린다. 내 돌연변이 실험은 그 문장들이 빠르게 끝나는 기계에서
 * 돌았을 뿐이고, <b>그 소요 시간을 재지 않았다.</b> 리뷰가 짚었다.
 *
 * <p>그래서 이제 <b>그 소요 시간을 직접 재서 기준으로 쓴다.</b> 회수 전후의 DB 시각을
 * 찍어 {@code span} 을 구하면, <b>어떤 고정 지연 구현이든</b> 행별 지연의 폭은 그
 * {@code span} 을 넘을 수 없다 — 지연이 같고 기준 시각만 그 안에서 움직이기 때문이다.
 * 행마다 새로 뽑을 때만 폭이 {@code span} 을 넘는다. 기계가 느리든 빠르든 성립한다.
 *
 * <h2>왜 base 를 크게 주나</h2>
 *
 * <p>기본값(200ms)이면 폭이 400ms 라 느린 기계의 {@code span} 과 겹칠 수 있다. 여기서
 * 재려는 것은 <b>값이 흩어지는가</b>이지 기본값이 얼마인가가 아니므로 10초로 넓힌다 —
 * 20건의 폭이 {@code span}(보통 수 ms)보다 작을 확률은 사실상 0 이다.
 */
@RepositoryTest
@Import({NotificationOutboxRepositoryImpl.class,
        NotificationOutboxExpiryJitterTest.WideBackOff.class,
        OutboxMeterTestConfig.class})
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class NotificationOutboxExpiryJitterTest {

    private static final Instant AT = Instant.parse("2026-08-27T00:00:00Z");
    private static final long NOTIFICATION_ID = 9006L;
    private static final int ROWS = 20;

    /**
     * 흩어짐이 초 단위로 보이도록 넓힌 백오프. 정책의 기본값을 재는 자리가 아니다.
     *
     * <p><b>{@code @Configuration} 이 아니라 {@code @TestConfiguration} 이다</b> —
     * 중첩 {@code @Configuration} 은 테스트의 <b>주 설정</b>으로 잡혀 {@code @DataJpaTest} 의
     * {@code @SpringBootConfiguration} 탐색을 밀어낸다(그러면 자동설정 기준 패키지를 못 찾아
     * 컨텍스트가 아예 안 뜬다). 형제인 {@code NotificationResendTransactionIntegrationTest}
     * 가 같은 이유로 같은 것을 쓴다.
     */
    @TestConfiguration(proxyBeanMethods = false)
    static class WideBackOff {
        @Bean
        FullJitterBackOff notificationRetryBackOff() {
            return new FullJitterBackOff(Duration.ofSeconds(10), Duration.ofSeconds(10));
        }
    }

    @Autowired NotificationOutboxRepository repository;
    @Autowired JdbcTemplate jdbcTemplate;
    @Autowired MeterRegistry registry;

    @AfterEach
    void cleanCommittedFixture() {
        jdbcTemplate.update("DELETE FROM notification_outbox WHERE notification_id=?",
                NOTIFICATION_ID);
    }

    @Test
    @DisplayName("동시에 만료된 것들이 서로 다른 지연으로 돌아온다 — 지연의 폭을 회수 소요 시간과 견준다")
    void simultaneouslyExpiredClaimsComeBackWithScatteredDelays() {
        for (int i = 0; i < ROWS; i++) {
            repository.save(NotificationOutbox.pending(
                    NOTIFICATION_ID, i + 1, AttemptTrigger.INITIAL, AT));
        }
        assertThat(repository.claimBatch(Duration.ofMinutes(1), ROWS)).hasSize(ROWS);

        // 스무 건을 **같은 순간** 만료시킨다 — 릴레이가 죽었을 때 벌어지는 일이다.
        jdbcTemplate.update("UPDATE notification_outbox"
                + " SET claimed_at=TIMESTAMPADD(SECOND,-120,CURRENT_TIMESTAMP(6))"
                + " WHERE notification_id=?", NOTIFICATION_ID);

        // 회수는 claimBatch 안에서 돈다. 되돌린 것 중 일부는 지연이 0 에 가까우면 같은
        // 회차에 다시 집힐 수도 있다 — 선점은 next_attempt_at 을 안 건드리므로 아래 판정에는
        // 영향이 없다.
        long before = dbNowMicros();
        repository.claimBatch(Duration.ofSeconds(60), ROWS);
        long span = dbNowMicros() - before;

        List<Long> delays = jdbcTemplate.queryForList(
                "SELECT TIMESTAMPDIFF(MICROSECOND, ?, next_attempt_at)"
                        + " FROM notification_outbox WHERE notification_id=?",
                Long.class, new java.sql.Timestamp(before / 1_000L), NOTIFICATION_ID);

        assertThat(delays).hasSize(ROWS);
        long spread = delays.stream().mapToLong(Long::longValue).max().orElseThrow()
                - delays.stream().mapToLong(Long::longValue).min().orElseThrow();

        // **span 이 기준이다.** 지연이 하나뿐이면(고정이든 배치 재사용이든) 행별 값의 폭은
        // 기준 시각이 움직인 만큼, 즉 span 을 넘을 수 없다. 행마다 새로 뽑을 때만 넘는다.
        assertThat(spread)
                .as("회수에 걸린 시간(%dus)보다 지연의 폭(%dus)이 넓어야 행마다 새로 뽑은 것이다",
                        span, spread)
                .isGreaterThan(span);
    }

    /**
     * <b>지표가 실제 흩어짐을 보여 주는가.</b>
     *
     * <p>이 티켓(CY-908)이 만든 히스토그램은 <b>분포가 평평한지</b>를 보려고 있다.
     * 그런데 미터를 붙여 놓고 <b>실제로 적힌 값과 다른 것을 세면</b> 지표가 거짓을 그린다 —
     * 지표가 없는 것보다 나쁘다, 믿고 보는 사람이 있으니까.
     *
     * <p>그래서 DB 에 적힌 지연과 미터에 들어간 지연을 <b>맞대어</b> 본다. 회수는
     * 조건부 갱신이라 0행일 수 있는데, 그때 세면 지표가 실제 회수보다 커진다 — 그것도
     * 여기서 걸린다(건수가 안 맞는다).
     */
    @Test
    @DisplayName("만료 회수가 세어지고, 히스토그램에 들어간 값이 DB 에 적힌 지연과 같다")
    void theRecoveryIsCountedWithTheDelaysActuallyWritten() {
        for (int i = 0; i < ROWS; i++) {
            repository.save(NotificationOutbox.pending(
                    NOTIFICATION_ID, i + 1, AttemptTrigger.INITIAL, AT));
        }
        repository.claimBatch(Duration.ofMinutes(1), ROWS);
        long before = dbNowMicros();
        jdbcTemplate.update("UPDATE notification_outbox"
                + " SET claimed_at=TIMESTAMPADD(SECOND,-120,CURRENT_TIMESTAMP(6))"
                + " WHERE notification_id=?", NOTIFICATION_ID);

        repository.claimBatch(Duration.ofSeconds(60), ROWS);

        Timer delays = registry.find(DomainMeterNames.OUTBOX_RETRY_DELAY)
                .tag(DomainMeterNames.TAG_REASON, OutboxRetryReason.LEASE_EXPIRED.tag())
                .timer();
        assertThat(delays).as("미터가 등록되지 않았습니다").isNotNull();
        assertThat(delays.count())
                .as("회수한 건수와 센 건수가 다르면 조건부 갱신이 0행인 경우를 잘못 센 것이다")
                .isEqualTo(ROWS);

        // DB 에 적힌 지연의 합. 기준 시각이 행마다 조금씩 다르므로 정확히 같을 수는 없고,
        // **회수에 걸린 시간(span)만큼** 벌어질 수 있다. 그 폭 안이면 같은 값이다.
        Long writtenSumMicros = jdbcTemplate.queryForObject(
                "SELECT SUM(TIMESTAMPDIFF(MICROSECOND, ?, next_attempt_at))"
                        + " FROM notification_outbox WHERE notification_id=?",
                Long.class, new java.sql.Timestamp(before / 1_000L), NOTIFICATION_ID);
        long meteredMicros = (long) delays.totalTime(TimeUnit.MICROSECONDS);
        long span = dbNowMicros() - before;

        assertThat(Math.abs(writtenSumMicros - meteredMicros))
                .as("적어 넣은 값(%dus)과 세어 넣은 값(%dus)이 회수 소요(%dus)보다 더 벌어졌다"
                        + " — 미터가 다른 값을 세고 있다", writtenSumMicros, meteredMicros, span)
                .isLessThanOrEqualTo(span * ROWS);

        // 흩어짐이 히스토그램에도 남는지. 한 값만 반복해 넣으면 최대와 평균이 같아진다.
        assertThat(delays.max(TimeUnit.MICROSECONDS))
                .as("최대와 평균이 같으면 히스토그램이 한 점으로 뭉친 것이다")
                .isGreaterThan(delays.mean(TimeUnit.MICROSECONDS));
    }

    /**
     * <b>종착한 것은 재시도가 아니다.</b>
     *
     * <p>열 번째 실패에서 명령은 {@code DEAD} 로 가고 <b>다시 시도되지 않는다.</b> 그런데
     * 첫 판은 그것까지 {@code app.outbox.retry} 로 셌고, <b>아무도 기다리지 않을 지연을</b>
     * 히스토그램에 넣었다 — 재시도 수는 부풀고 분포는 오염된다. 리뷰가 짚었다.
     *
     * <p>{@code failure_count} 를 상한 직전까지 올려 두고 한 번 더 만료시킨다.
     * 그 한 건은 {@code dead} 로만 세어져야 한다.
     */
    @Test
    @DisplayName("종착한 건은 dead 로만 세어진다 — 기다릴 일 없는 지연이 분포에 안 섞인다")
    void aDeadCommandIsNotCountedAsARetry() {
        repository.save(NotificationOutbox.pending(NOTIFICATION_ID, 1, AttemptTrigger.INITIAL, AT));
        repository.claimBatch(Duration.ofMinutes(1), 1);
        // 다음 실패가 상한(10)에 닿도록 직전까지 올려 둔다.
        jdbcTemplate.update("UPDATE notification_outbox SET failure_count=9"
                + " WHERE notification_id=?", NOTIFICATION_ID);
        jdbcTemplate.update("UPDATE notification_outbox"
                + " SET claimed_at=TIMESTAMPADD(SECOND,-120,CURRENT_TIMESTAMP(6))"
                + " WHERE notification_id=?", NOTIFICATION_ID);

        double retriesBefore = retryCount(OutboxRetryReason.LEASE_EXPIRED);
        double deathsBefore = deadCount(OutboxRetryReason.LEASE_EXPIRED);
        long delaysBefore = delayCount(OutboxRetryReason.LEASE_EXPIRED);

        repository.claimBatch(Duration.ofSeconds(60), 5);

        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM notification_outbox WHERE notification_id=?",
                String.class, NOTIFICATION_ID)).isEqualTo("DEAD");
        assertThat(deadCount(OutboxRetryReason.LEASE_EXPIRED) - deathsBefore).isEqualTo(1.0);
        assertThat(retryCount(OutboxRetryReason.LEASE_EXPIRED) - retriesBefore)
                .as("다시 시도되지 않는 건을 재시도로 세면 재시도 수가 부풀고 분포가 오염된다")
                .isZero();
        assertThat(delayCount(OutboxRetryReason.LEASE_EXPIRED) - delaysBefore)
                .as("아무도 기다리지 않을 지연이 히스토그램에 섞이면 분포가 거짓이 된다")
                .isZero();
    }

    /**
     * <b>롤백된 회수가 지표에 남으면 안 된다.</b> 미터는 트랜잭션을 안 타므로, 쓰기 전에
     * 세면 그 트랜잭션이 되돌려져도 숫자만 그대로 남는다 — 숫자를 보는 사람은 <b>일어나지
     * 않은 일</b>을 본다. 리뷰가 짚었다.
     *
     * <p>회수는 {@code REQUIRES_NEW} 라 바깥에서 되돌릴 수 없다. 그래서 여기서는
     * <b>커밋 뒤에 센다</b>는 사실만 확인한다 — 회수가 끝난 뒤에야 값이 보이는지.
     */
    @Test
    @DisplayName("회수 지표는 커밋 뒤에 올라온다 — 쓰기 전에 세면 롤백돼도 숫자가 남는다")
    void recoveryMetricsAppearOnlyAfterTheWriteCommitted() {
        repository.save(NotificationOutbox.pending(NOTIFICATION_ID, 1, AttemptTrigger.INITIAL, AT));
        repository.claimBatch(Duration.ofMinutes(1), 1);
        jdbcTemplate.update("UPDATE notification_outbox"
                + " SET claimed_at=TIMESTAMPADD(SECOND,-120,CURRENT_TIMESTAMP(6))"
                + " WHERE notification_id=?", NOTIFICATION_ID);

        double before = retryCount(OutboxRetryReason.LEASE_EXPIRED);

        // 만료시켜 놓기만 하고 아직 회수는 안 했다. 쓰기 전에 세는 구현이면 여기서 오른다.
        assertThat(retryCount(OutboxRetryReason.LEASE_EXPIRED) - before)
                .as("회수 전인데 값이 올랐으면 미터가 실제 쓰기와 무관하게 오른 것이다")
                .isZero();

        repository.claimBatch(Duration.ofSeconds(60), 5);

        assertThat(retryCount(OutboxRetryReason.LEASE_EXPIRED) - before).isEqualTo(1.0);
    }

    /**
     * <b>절대값이 아니라 증분으로 본다.</b> 스프링이 컨텍스트를 재사용하므로 레지스트리도
     * 클래스 안에서 공유된다 — 앞선 테스트가 올려 둔 값이 그대로 남아 있어서, 0 을
     * 기대하면 <b>실행 순서에 따라 통과하고 실패한다.</b>
     */
    private double retryCount(OutboxRetryReason reason) {
        return registry.find(DomainMeterNames.OUTBOX_RETRY)
                .tag(DomainMeterNames.TAG_REASON, reason.tag()).counter().count();
    }

    private double deadCount(OutboxRetryReason reason) {
        return registry.find(DomainMeterNames.OUTBOX_DEAD)
                .tag(DomainMeterNames.TAG_REASON, reason.tag()).counter().count();
    }

    private long delayCount(OutboxRetryReason reason) {
        return registry.find(DomainMeterNames.OUTBOX_RETRY_DELAY)
                .tag(DomainMeterNames.TAG_REASON, reason.tag()).timer().count();
    }

    /** 판정 기준을 앱 시계가 아니라 <b>DB 시계</b>로 잡는다 — 값을 쓴 것도 DB 다. */
    private long dbNowMicros() {
        java.sql.Timestamp now = jdbcTemplate.queryForObject(
                "SELECT CURRENT_TIMESTAMP(6)", java.sql.Timestamp.class);
        return now.getTime() * 1_000L + now.getNanos() % 1_000_000 / 1_000L;
    }
}
