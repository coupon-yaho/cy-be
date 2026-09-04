package com.kafkick.storage.db.notification.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

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
 * <p>그래서 절대 시각이 아니라 <b>지연</b>을 본다 — 읽는 시점 기준의 남은 초로 바꾸면
 * 행마다 공통으로 빠지는 값이 사라진다.
 *
 * <h2>왜 base 를 크게 주나</h2>
 *
 * <p>기본값(200ms)이면 첫 재시도 상한이 400ms 라 전부 같은 초 버킷에 들어간다.
 * 여기서 재려는 것은 <b>값이 흩어지는가</b>이지 기본값이 얼마인가가 아니므로, 흩어짐이
 * 초 단위로 보이도록 넓힌다.
 *
 * <p><b>확률</b> — 20건이 10초 폭에 균등하게 떨어질 때 서로 다른 초 버킷이 2개 이하일
 * 확률은 {@code C(10,2) × 0.2^20 ≈ 5e-13} 이다. 고정 지연이나 값 재사용이면 <b>반드시</b>
 * 1개다.
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
    @DisplayName("동시에 만료된 것들이 서로 다른 지연으로 돌아온다 — 절대 시각이 아니라 지연을 본다")
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

        // 회수는 claimBatch 안에서 돈다. 되돌린 것들은 지연 때문에 이번엔 안 집힌다.
        repository.claimBatch(Duration.ofSeconds(60), ROWS);

        List<Integer> delaySeconds = jdbcTemplate.queryForList(
                "SELECT TIMESTAMPDIFF(SECOND, CURRENT_TIMESTAMP(6), next_attempt_at)"
                        + " FROM notification_outbox WHERE notification_id=?",
                Integer.class, NOTIFICATION_ID);

        assertThat(delaySeconds).hasSize(ROWS);
        assertThat(Set.copyOf(delaySeconds))
                .as("고정 지연이면 1개, 배치에 한 값을 재사용해도 1개다 — 둘 다 여기서 걸린다")
                .hasSizeGreaterThan(2);
    }
}
