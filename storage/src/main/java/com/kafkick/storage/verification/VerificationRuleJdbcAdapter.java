// V3·V5 판정 SQL 입니다. asof_state 를 드라이빙 테이블로 잡습니다.
package com.kafkick.storage.verification;

import java.time.LocalDateTime;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
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
     * <p>{@code asof_state} 를 드라이빙 테이블로 잡는다. {@code issuances} 를 드라이빙으로 잡으면
     * 접힌 상태가 없는 발급건이 {@code state IS NULL} 로 올라와, 기대 매트릭스에 없는 검출을 낸다.
     *
     * <p>{@code i.updated_at <= :asOf} 로 자른다. 접힌 상태는 asOf 로 얼어 있는데 저장 상태는
     * 질의 순간의 현재값이라, 이 조건이 없으면 배치가 도는 동안 런타임이 건드린 발급건이
     * 전부 어긋난 것으로 잡히고 재실행 결과도 달라진다.
     */
    // stored 는 MySQL 예약어다(GENERATED ... STORED). 별칭에 그대로 쓰면 문법 오류가 난다.
    private static final String SELECT_REPLAY_MISMATCH = """
            SELECT /*+ MAX_EXECUTION_TIME(%d) */
                   a.coupon_id AS issuance_id,
                   a.state     AS replayed_state,
                   i.status    AS stored_status
              FROM asof_state a
              JOIN issuances i ON i.id = a.coupon_id
             WHERE a.run_id = :runId
               AND i.updated_at <= :asOf
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
            SELECT /*+ MAX_EXECUTION_TIME(%d) */
                   a.coupon_id                                       AS issuance_id,
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
    private final String replayMismatchSql;
    private final String usageMismatchSql;

    /**
     * @param queryTimeoutMillis 문장 단위 상한. 없으면 폭주한 질의가 커넥션을 물고 늘어져
     *                           배치 풀(기본 4개)과 공유 서버가 함께 막힌다.
     *                           {@code JdbcClient} 에는 queryTimeout API 가 없어 힌트로 건다
     */
    public VerificationRuleJdbcAdapter(
            JdbcClient jdbcClient,
            @Value("${batch.verify.query-timeout-ms:600000}") long queryTimeoutMillis
    ) {
        if (queryTimeoutMillis < 1) {
            throw new IllegalArgumentException(
                    "질의 상한은 1ms 이상이어야 합니다. 값=" + queryTimeoutMillis);
        }

        this.jdbcClient = jdbcClient;
        this.replayMismatchSql = SELECT_REPLAY_MISMATCH.formatted(queryTimeoutMillis);
        this.usageMismatchSql = SELECT_USAGE_MISMATCH.formatted(queryTimeoutMillis);
    }

    @Override
    public List<VerificationFinding> findReplayMismatches(long runId, LocalDateTime asOf, int limit) {
        requireLimit(limit);

        return jdbcClient.sql(replayMismatchSql)
                .param("runId", runId)
                .param("asOf", asOf)
                .param("limit", limit)
                .query(REPLAY_MISMATCH_MAPPER)
                .list();
    }

    @Override
    public List<VerificationFinding> findUsageMismatches(long runId, int limit) {
        requireLimit(limit);

        return jdbcClient.sql(usageMismatchSql)
                .param("runId", runId)
                .param("limit", limit)
                .query(USAGE_MISMATCH_MAPPER)
                .list();
    }

    private static void requireLimit(int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("검출 상한은 1 이상이어야 합니다. 값=" + limit);
        }
    }
}
