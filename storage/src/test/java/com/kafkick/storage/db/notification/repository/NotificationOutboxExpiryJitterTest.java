package com.kafkick.storage.db.notification.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

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

import com.kafkick.core.notification.NotificationOutboxRepository;
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
        NotificationOutboxExpiryJitterTest.WideBackOff.class})
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

    /** 판정 기준을 앱 시계가 아니라 <b>DB 시계</b>로 잡는다 — 값을 쓴 것도 DB 다. */
    private long dbNowMicros() {
        java.sql.Timestamp now = jdbcTemplate.queryForObject(
                "SELECT CURRENT_TIMESTAMP(6)", java.sql.Timestamp.class);
        return now.getTime() * 1_000L + now.getNanos() % 1_000_000 / 1_000L;
    }
}
