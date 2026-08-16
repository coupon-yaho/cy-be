// V3·V5 판정 SQL 입니다. asof_state 를 드라이빙 테이블로 잡습니다.
package com.kafkick.storage.verification;

import java.util.List;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import com.kafkick.core.verification.FindingType;
import com.kafkick.core.verification.VerificationFinding;
import com.kafkick.core.verification.VerificationRuleRepository;

/**
 * <b>{@code asof_state.coupon_id} 는 발급건({@code issuances.id})입니다.</b> 레거시 컬럼명이라
 * 회차로 읽으면 조인이 통째로 어긋납니다.
 *
 * <p>{@code target_key} 를 SQL 에서 만들지 않습니다. 형식이 코드와 SQL 두 곳에 생기면
 * 갈라지고, 그러면 개수는 맞는데 키만 달라져 누락과 오탐이 동시에 뜹니다.
 * 여기서는 식별자만 꺼내고 키는 {@link VerificationFinding} 의 팩토리가 만듭니다.
 */
@Repository
public class VerificationRuleJdbcAdapter implements VerificationRuleRepository {

    /**
     * 접은 상태와 저장된 상태가 다른 발급건.
     *
     * <p>{@code asof_state} 를 드라이빙 테이블로 잡는다. 시드는 발급건마다 {@code issued_at} 에
     * ISSUE 이력을 남기고 그 시각은 asOf 이하이므로, 행이 없는 발급건은 생기지 않는다.
     */
    // stored 는 MySQL 예약어다(GENERATED ... STORED). 별칭에 그대로 쓰면 문법 오류가 난다.
    private static final String SELECT_REPLAY_MISMATCH = """
            SELECT a.coupon_id AS issuance_id,
                   a.state     AS replayed_state,
                   i.status    AS stored_status
              FROM asof_state a
              JOIN issuances i ON i.id = a.coupon_id
             WHERE a.run_id = :runId
               AND a.state <> i.status
             ORDER BY a.coupon_id
             LIMIT :limit
            """;

    /**
     * 상태와 활성 사용 건수가 어긋나는 발급건.
     *
     * <p>불변식은 {@code USED} 면 1건, 아니면 0건이다. 한쪽 방향만 보면
     * 이중 사용({@code USED} 인데 2건)을 놓친다.
     */
    private static final String SELECT_USAGE_MISMATCH = """
            SELECT a.coupon_id                                       AS issuance_id,
                   a.active_usage_count                              AS actual_count,
                   CASE WHEN a.state = 'USED' THEN 1 ELSE 0 END      AS expected_count
              FROM asof_state a
             WHERE a.run_id = :runId
               AND a.active_usage_count <> CASE WHEN a.state = 'USED' THEN 1 ELSE 0 END
             ORDER BY a.coupon_id
             LIMIT :limit
            """;

    private static final RowMapper<VerificationFinding> REPLAY_MISMATCH_MAPPER =
            (rs, rowNum) -> VerificationFinding.forIssuance(
                    FindingType.REPLAY_MISMATCH,
                    rs.getLong("issuance_id"),
                    "replay=" + rs.getString("replayed_state"),
                    "issuances.status=" + rs.getString("stored_status"));

    private static final RowMapper<VerificationFinding> USAGE_MISMATCH_MAPPER =
            (rs, rowNum) -> VerificationFinding.forIssuance(
                    FindingType.USAGE_MISMATCH,
                    rs.getLong("issuance_id"),
                    "active_usage=" + rs.getInt("expected_count"),
                    "active_usage=" + rs.getInt("actual_count"));

    private final JdbcClient jdbcClient;

    public VerificationRuleJdbcAdapter(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    @Override
    public List<VerificationFinding> findReplayMismatches(long runId, int limit) {
        return query(SELECT_REPLAY_MISMATCH, runId, limit, REPLAY_MISMATCH_MAPPER);
    }

    @Override
    public List<VerificationFinding> findUsageMismatches(long runId, int limit) {
        return query(SELECT_USAGE_MISMATCH, runId, limit, USAGE_MISMATCH_MAPPER);
    }

    private List<VerificationFinding> query(
            String sql, long runId, int limit, RowMapper<VerificationFinding> mapper) {
        if (limit < 1) {
            throw new IllegalArgumentException("검출 상한은 1 이상이어야 합니다. 값=" + limit);
        }

        return jdbcClient.sql(sql)
                .param("runId", runId)
                .param("limit", limit)
                .query(mapper)
                .list();
    }
}
