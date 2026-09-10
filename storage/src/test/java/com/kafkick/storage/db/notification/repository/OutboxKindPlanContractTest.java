package com.kafkick.storage.db.notification.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.storage.db.RepositoryTest;

/**
 * <b>종류별 선점이 상대 종류의 적체에 비례해 비싸지지 않는다.</b>
 *
 * <p>이것이 {@code ix_notification_outbox_kind} 가 있는 이유 전부다. 없으면
 * {@code `trigger`} 술어를 걸러 낼 데가 없어 <b>due 백로그를 통째로 훑는다</b> —
 * 100ms 마다 도는 경로라, 굶는 것을 고치려다 적체가 커질수록 느려지는 것으로 바뀐다.
 *
 * <h2>이름이 아니라 읽은 호출 수를 본다</h2>
 *
 * <p>형제 {@code BacklogPlanContractTest} 가 같은 자리에서 <b>{@code EXPLAIN} 의
 * {@code type} 으로 비용을 말했다가 반려당했다</b>. 여기서도 같은 기준을 쓴다 —
 * {@code Handler_read_*}(스토리지 엔진 읽기 호출)의 차이를 본다.
 *
 * <p><b>연결을 고정한다.</b> 그 값은 <b>세션 상태</b>라, 풀에서 매번 다른 연결을 받으면
 * 앞뒤 측정이 서로 다른 세션에서 나와 차이가 무의미해진다.
 */
@RepositoryTest
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class OutboxKindPlanContractTest {

    /** 상대 종류의 적체. 이만큼을 안 읽는다는 것이 이 테스트의 요점이다. */
    private static final int OPPOSITE_BACKLOG = 2_000;

    private static final int LIMIT = 32;

    /**
     * <b>대상 종류에 있는 "읽으면 안 되는" 행</b> — 이미 나간 것과 아직 때가 안 된 것.
     *
     * <p>형제 {@code BacklogPlanContractTest} 가 같은 이유로 {@code PUBLISHED} 300건을
     * 심는다. 이것이 없으면 인덱스에서 {@code status} 를 빼거나
     * {@code next_attempt_at} 을 뒤로 보내도 <b>걸러낼 것이 없어 테스트가 초록이다.</b>
     */
    private static final int DECOY_ROWS = 300;

    /** 이 클래스가 쓰는 구간. 형제와 안 겹치게 좁게 잡고 그 구간만 지운다. */
    private static final long ID_BASE = 7_300_000L;
    private static final long ID_MAX = 7_399_999L;

    private static final long TARGET_BLOCK = 50_000L;
    private static final long PUBLISHED_BLOCK = 60_000L;
    private static final long FUTURE_BLOCK = 70_000L;

    @Autowired JdbcTemplate jdbcTemplate;

    @BeforeEach
    @AfterEach
    void clean() {
        jdbcTemplate.update(
                "DELETE FROM notification_outbox WHERE notification_id BETWEEN ? AND ?",
                ID_BASE, ID_MAX);
    }

    /**
     * 상대 종류의 적체와, <b>대상 종류 쪽의 미끼</b>를 함께 심는다.
     *
     * <p>미끼는 인덱스가 걸러 줘야 하는 것들이다 — 이미 {@code PUBLISHED} 인 행과,
     * {@code PENDING} 이지만 {@code next_attempt_at} 이 미래라 아직 집을 수 없는 행.
     */
    private void seedShape(int targetDue) {
        seed(AttemptTrigger.INITIAL, OPPOSITE_BACKLOG, 0, State.DUE);
        seed(AttemptTrigger.MANUAL, targetDue, TARGET_BLOCK, State.DUE);
        seed(AttemptTrigger.MANUAL, DECOY_ROWS, PUBLISHED_BLOCK, State.PUBLISHED);
        seed(AttemptTrigger.MANUAL, DECOY_ROWS, FUTURE_BLOCK, State.NOT_YET_DUE);
    }

    /**
     * 임계는 <b>몫에 비례하는 값</b>이어야 한다. {@code OPPOSITE_BACKLOG} 미만은
     * 인덱스를 통째로 지운 경우만 잡고 <b>부분 퇴행은 전부 통과시킨다</b> —
     * 정상값이 한 자리 수인데 상한이 2,000이면 재는 것이 아니다.
     */
    private static final long READ_BUDGET = 4L * LIMIT;

    @Test
    @DisplayName("상대 적체 2,000건과 대상 종류의 미끼 600건을 안 훑는다")
    void doesNotScaleWithTheOppositeKindsBacklog() {
        seedShape(1);

        Measurement measured = measure(NotificationOutboxRepositoryImpl.SELECT_DUE_BY_KIND, false);

        assertThat(measured.ids())
                .as("계획만 보고 결과를 안 보면 반쪽입니다")
                .hasSize(1);
        assertThat(measured.handlerReads())
                .as("상대 적체 %d + 미끼 %d 건인데 그만큼 읽으면 적체가 커질수록 선점이"
                        + " 느려집니다 (읽은 호출 %d)",
                        OPPOSITE_BACKLOG, 2 * DECOY_ROWS, measured.handlerReads())
                .isLessThan(READ_BUDGET);
    }

    /**
     * 이어받기도 같은 축을 탄다. 커서가 붙으면 술어가 하나 늘어 <b>인덱스 선택이
     * 바뀔 수 있으므로</b> 따로 본다 — 앞 문장만 재고 뒤 문장을 안 재면 남는 자리를
     * 채우는 회차에서만 느려진다.
     *
     * <p>결과도 같이 본다. 안 보면 커서 비교를 <b>거꾸로 뒤집어도</b> 통과한다
     * (0건 반환 → 읽기 호출도 작음).
     */
    @Test
    @DisplayName("이어받기 선점도 상대 적체와 미끼를 안 훑는다")
    void theContinuationDoesNotScaleEither() {
        seedShape(5);

        Measurement measured =
                measure(NotificationOutboxRepositoryImpl.SELECT_DUE_BY_KIND_AFTER, true);

        assertThat(measured.ids())
                .as("커서를 뒤집으면 여기서 0건이 됩니다")
                .hasSize(5);
        assertThat(measured.handlerReads())
                .as("읽은 호출 %d", measured.handlerReads())
                .isLessThan(READ_BUDGET);
    }

    private record Measurement(List<Long> ids, long handlerReads) { }

    /**
     * 고정한 연결에서 한 번 워밍업하고, 그다음 실행의 읽기 호출 차이를 잰다.
     *
     * <p>워밍업이 없으면 첫 실행의 <b>메타데이터·통계 읽기</b>가 차이에 섞인다.
     */
    private Measurement measure(String sql, boolean withCursor) {
        return jdbcTemplate.execute((ConnectionCallback<Measurement>) connection -> {
            run(connection, sql, withCursor);
            long before = handlerReads(connection);
            List<Long> ids = run(connection, sql, withCursor);
            return new Measurement(ids, handlerReads(connection) - before);
        });
    }

    /**
     * 인자 개수는 <b>부르는 쪽이 안다.</b>
     *
     * <p>한때 {@code sql.contains(...)} 로 문장 원문을 보고 갈랐는데, 그러면 공백
     * 하나만 바뀌어도 조용히 틀린 분기로 가서 <b>퇴행이 아니라 포매팅을 검출한다.</b>
     */
    private List<Long> run(Connection connection, String sql, boolean withCursor)
            throws java.sql.SQLException {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, AttemptTrigger.MANUAL.name());
            if (withCursor) {
                ps.setTimestamp(2, new java.sql.Timestamp(0L));
                ps.setLong(3, 0L);
                ps.setInt(4, LIMIT);
            } else {
                ps.setInt(2, LIMIT);
            }
            try (ResultSet rs = ps.executeQuery()) {
                List<Long> ids = new ArrayList<>();
                while (rs.next()) {
                    ids.add(rs.getLong("id"));
                }
                return ids;
            }
        }
    }

    private long handlerReads(Connection connection) throws java.sql.SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                     "SHOW SESSION STATUS WHERE Variable_name IN"
                             + " ('Handler_read_next','Handler_read_key','Handler_read_first',"
                             + "  'Handler_read_rnd_next')")) {
            long sum = 0;
            while (rs.next()) {
                sum += rs.getLong("Value");
            }
            return sum;
        }
    }

    private enum State { DUE, NOT_YET_DUE, PUBLISHED }

    /**
     * {@code NotificationOutboxQuotaTest} 와 같은 형상을 만들되 <b>상태 축</b>을 더한다.
     *
     * <p>{@link State#PUBLISHED} 는 {@code published_at} 이 있어야 한다 —
     * {@code ck_notification_outbox_published_at} 이 그 짝을 강제한다.
     */
    private void seed(AttemptTrigger kind, int count, long idOffset, State state) {
        String sql = state == State.PUBLISHED
                ? """
                  INSERT INTO notification_outbox
                         (notification_id, attempt_seq, `trigger`, status, failure_count,
                          next_attempt_at, created_at, published_at)
                  VALUES (?, 1, ?, 'PUBLISHED', 0,
                          CURRENT_TIMESTAMP(6) - INTERVAL ? SECOND, NOW(6), NOW(6))
                  """
                : """
                  INSERT INTO notification_outbox
                         (notification_id, attempt_seq, `trigger`, status, failure_count,
                          next_attempt_at, created_at)
                  VALUES (?, 1, ?, 'PENDING', 0,
                          CURRENT_TIMESTAMP(6) - INTERVAL ? SECOND, NOW(6))
                  """;
        boolean scattered = idOffset == 0;
        // 미래로 미는 것은 음수 초로 표현한다 — INTERVAL 하나로 두 방향을 다 낸다.
        int sign = state == State.NOT_YET_DUE ? -1 : 1;
        jdbcTemplate.batchUpdate(sql,
                new org.springframework.jdbc.core.BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws java.sql.SQLException {
                ps.setLong(1, ID_BASE + idOffset + i);
                ps.setString(2, kind.name());
                ps.setInt(3, sign * (scattered ? 61 + (int) ((long) i * 7919 % 3540) : 1));
            }

            @Override
            public int getBatchSize() {
                return count;
            }
        });
    }
}
