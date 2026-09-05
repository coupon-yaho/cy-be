package com.kafkick.storage.db.notification.repository;

import static org.assertj.core.api.Assertions.assertThat;

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
 * <p><b>이름은 비용을 말하지 않는다.</b> 그렇다고 {@code EXPLAIN ANALYZE} 의
 * {@code actual rows} 도 답이 아니다 — 그것은 그 노드가 <b>돌려준</b> 행이지 스토리지
 * 엔진에서 <b>읽은</b> 행이 아니다(리뷰가 짚었다). 필터가 뒤에 붙으면 읽은 행이 훨씬
 * 많아도 돌려준 행은 적다.
 *
 * <p>그래서 <b>{@code Handler_read_*} 를 센다.</b> 그것이 스토리지 엔진 호출 수이고,
 * "표가 커질수록 비싸지는가" 가 정확히 그 축이다. 질의 전후로 세션 상태를 읽어 차이를
 * 본다 — 표에 백로그 밖 행을 잔뜩 심어 두고서.
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

    /**
     * <b>스토리지 엔진에서 읽은 행이 백로그 크기에 머문다.</b>
     *
     * <p>여유를 조금 둔다 — 인덱스 구간을 훑을 때 경계 한 칸을 더 읽는 것은 정상이다.
     * 잡으려는 것은 <b>{@link #PUBLISHED_ROWS} 만큼 읽는 상태</b>이지 한두 칸이 아니다.
     */
    @Test
    @DisplayName("백로그 밖 행이 300건 있어도 엔진에서 읽는 행은 백로그 크기뿐이다")
    void readsOnlyTheBacklogRangeNotTheWholeIndex() {
        seed();

        long before = handlerReads();
        Long counted = jdbcTemplate.queryForObject(
                NotificationOutboxRepositoryImpl.COUNT_BACKLOG, Long.class);
        long read = handlerReads() - before;

        assertThat(counted).as("세기는 해야 한다").isNotNull();
        assertThat(read)
                .as("표에 %d건이 더 있는데 그만큼 읽으면 누적될수록 비싸집니다 (읽은 행 %d)",
                        PUBLISHED_ROWS, read)
                .isLessThan(PUBLISHED_ROWS);
    }

    /**
     * 스토리지 엔진 읽기 호출 수.
     *
     * <p>{@code Handler_read_next} 는 인덱스 순서로 다음 행을 읽은 횟수,
     * {@code Handler_read_key} 는 키로 찾은 횟수다 — 인덱스 구간 스캔이 쓰는 둘이다.
     * {@code EXPLAIN} 의 추정치가 아니라 <b>실제로 일어난 호출</b>이라 여기서 쓴다.
     */
    private long handlerReads() {
        return jdbcTemplate.query(
                "SHOW SESSION STATUS WHERE Variable_name IN"
                        + " ('Handler_read_next','Handler_read_key','Handler_read_first',"
                        + "  'Handler_read_rnd_next')",
                (rs, i) -> rs.getLong("Value")).stream().mapToLong(Long::longValue).sum();
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
