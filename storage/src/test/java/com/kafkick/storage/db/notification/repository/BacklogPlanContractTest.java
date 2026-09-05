package com.kafkick.storage.db.notification.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.storage.db.RepositoryTest;

/**
 * <b>백로그 질의가 누적 행 수에 비례해 비싸지지 않는다.</b>
 *
 * <p>15초마다 도는 질의라 표가 커질수록 비싸지면 <b>관측이 사고를 키운다</b> — 볼 수 없는
 * 것보다 나쁘다.
 *
 * <h2>접근 방식 이름이 아니라 읽은 행 수를 본다</h2>
 *
 * <p>한때 이 자리에서 {@code EXPLAIN} 의 {@code type} 을 봤다. {@code type=index} 가
 * 나오길래 <b>"인덱스를 끝까지 훑는다"</b> 로 읽고 질의를 상태별로 쪼갰는데, 리뷰가
 * <b>그 단정이 입증되지 않았다</b>고 짚었다. 재 보니 실제로는 두 구간만 읽는다:
 *
 * <pre>
 *   Covering index range scan over (status='IN_PROGRESS') OR (status='PENDING')
 *   actual rows=5      ← 백로그 크기지 표 크기가 아니다
 * </pre>
 *
 * <p><b>이름은 비용을 말하지 않는다.</b> 그래서 이 테스트는 {@code EXPLAIN ANALYZE} 로
 * <b>실제로 읽은 행</b>을 세고, 그것이 <b>백로그 크기에 비례하는지</b>를 본다 —
 * 표에 백로그 밖 행을 잔뜩 심어 두고서.
 */
@RepositoryTest
@Import(OutboxMeterTestConfig.class)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class BacklogPlanContractTest {

    /** 백로그 밖(이미 나간) 행 수. 이만큼을 안 읽는다는 것이 이 테스트의 요점이다. */
    private static final int PUBLISHED_ROWS = 300;

    /** 백로그 행 수. */
    private static final int PENDING_ROWS = 5;

    private static final long ID_BASE = 800_000L;

    /** {@code EXPLAIN ANALYZE} 트리에서 스캔 노드의 {@code actual … rows=N} 을 뽑는다. */
    private static final Pattern ACTUAL_ROWS =
            Pattern.compile("Covering index[^\\n]*?actual time=[^)]*?rows=(\\d+)");

    @Autowired JdbcTemplate jdbcTemplate;

    @AfterEach
    void clean() {
        jdbcTemplate.update("DELETE FROM notification_outbox WHERE notification_id >= ?", ID_BASE);
    }

    private void seed() {
        clean();
        for (int i = 0; i < PUBLISHED_ROWS; i++) {
            jdbcTemplate.update("INSERT INTO notification_outbox"
                    + " (notification_id, attempt_seq, `trigger`, status, failure_count,"
                    + "  next_attempt_at, created_at, published_at)"
                    + " VALUES (?, 1, 'INITIAL', 'PUBLISHED', 0, NOW(6), NOW(6), NOW(6))",
                    ID_BASE + i);
        }
        for (int i = 0; i < PENDING_ROWS; i++) {
            jdbcTemplate.update("INSERT INTO notification_outbox"
                    + " (notification_id, attempt_seq, `trigger`, status, failure_count,"
                    + "  next_attempt_at, created_at)"
                    + " VALUES (?, 1, 'INITIAL', 'PENDING', 0, NOW(6), NOW(6))",
                    ID_BASE + 10_000 + i);
        }
    }

    @Test
    @DisplayName("백로그 밖 행이 300건 있어도 읽는 행은 백로그 크기뿐이다")
    void readsOnlyTheBacklogRangeNotTheWholeIndex() {
        seed();

        String plan = String.join("\n", jdbcTemplate.queryForList(
                "EXPLAIN ANALYZE " + NotificationOutboxRepositoryImpl.COUNT_BACKLOG,
                String.class));

        Matcher rows = ACTUAL_ROWS.matcher(plan);
        assertThat(rows.find())
                .as("커버링 인덱스 스캔 노드를 못 찾았습니다. 계획:%n%s", plan)
                .isTrue();
        assertThat(Integer.parseInt(rows.group(1)))
                .as("표에 %d건이 더 있는데 그만큼 읽으면 누적될수록 비싸집니다. 계획:%n%s",
                        PUBLISHED_ROWS, plan)
                .isLessThanOrEqualTo(PENDING_ROWS);
    }

    /** 세는 값 자체도 맞아야 한다 — 계획만 보고 결과를 안 보면 반쪽이다. */
    @Test
    @DisplayName("백로그 밖 행을 안 센다")
    void doesNotCountRowsOutsideTheBacklog() {
        seed();

        Long counted = jdbcTemplate.queryForObject(
                NotificationOutboxRepositoryImpl.COUNT_BACKLOG + " AND notification_id >= "
                        + ID_BASE, Long.class);

        assertThat(counted).isEqualTo(PENDING_ROWS);
    }
}
