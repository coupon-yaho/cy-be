// asof_state 쓰기 어댑터입니다. 300만 행이라 단건 왕복이 아니라 배치로 씁니다.
package com.kafkick.storage.db.verification;

import java.time.LocalDateTime;
import java.util.List;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

import com.kafkick.core.verification.replay.AsOfStateRepository;
import com.kafkick.core.verification.replay.ReplayResult;

/**
 * {@link org.springframework.jdbc.core.simple.JdbcClient} 는 배치 업데이트를 제공하지 않아
 * ({@code update()} 단건뿐) 여기서는 {@link NamedParameterJdbcTemplate} 을 쓴다.
 * 300만 행을 단건으로 쓰면 왕복이 300만 번이다.
 *
 * <p>컨테이너 URL 에 {@code rewriteBatchedStatements=true} 가 걸려 있어 드라이버가
 * 여러 행을 한 문장으로 합친다.
 *
 * <p><b>{@code asof_state.coupon_id} 는 레거시 컬럼명이고 실제로 담기는 값은
 * {@code issuances.id} 다.</b> 이름만 보고 회차 식별자로 읽으면 안 된다.
 */
@Repository
public class AsOfStateJdbcAdapter implements AsOfStateRepository {

    /**
     * 같은 행을 다시 써도 되게 만든다. PK 가 {@code (run_id, coupon_id)} 라 그냥 INSERT 면
     * 같은 발급건이 두 번 나오는 순간 중복키로 죽는데, 그 "두 번" 을 만드는 경로가 여럿이다 —
     * 창을 다시 읽는 재시작(지금은 {@code preventRestart} 로 막혀 있다), 청크 롤백 후 재실행,
     * 그리고 앞으로 INSERT-only 가정을 깨는 변경. 마지막이 조용히 죽는 것을 막는 것이 핵심이다.
     *
     * <p>사용 건수는 여기서 건드리지 않는다. {@link #applyActiveUsageCounts} 가 뒤에 채우므로
     * 덮어쓰면 이미 채운 값을 0 으로 되돌린다.
     */
    private static final String UPSERT = """
            INSERT INTO asof_state
                (run_id, coupon_id, state, last_history_id, last_event_at, active_usage_count)
            VALUES (:runId, :issuanceId, :state, :lastHistoryId, :lastEventAt, 0) AS new
            ON DUPLICATE KEY UPDATE
                state           = new.state,
                last_history_id = new.last_history_id,
                last_event_at   = new.last_event_at
            """;

    private static final String APPLY_USAGE_COUNTS = """
            UPDATE asof_state a
              JOIN (SELECT issuance_id, COUNT(*) AS active_count
                      FROM issuance_usages
                     WHERE id <= :maxUsageId
                       AND used_at <= :asOf
                       AND (canceled_at IS NULL OR canceled_at > :asOf)
                     GROUP BY issuance_id) u
                ON u.issuance_id = a.coupon_id
               SET a.active_usage_count = u.active_count
             WHERE a.run_id = :runId
            """;

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public AsOfStateJdbcAdapter(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void appendAll(long runId, List<ReplayResult> results) {
        if (results.isEmpty()) {
            return;
        }

        SqlParameterSource[] batch = results.stream()
                .map(result -> toParams(runId, result))
                .toArray(SqlParameterSource[]::new);

        jdbcTemplate.batchUpdate(UPSERT, batch);
    }

    /**
     * <b>{@code maxUsageId} 는 이력 축의 {@code maxHistoryId} 와 같은 뜻이다</b> —
     * 리플레이가 {@code h.id <= :maxHistoryId} 로 읽듯, V5 도 얼린 상한까지만 센다.
     * 상한이 없으면 이 UPDATE 가 도는 순간 커밋돼 있는 모든 행을 세어, 같은 {@code asOf}
     * 재실행이 다른 답을 낼 수 있다 — 결정론이 깨지는 자리다.
     * {@code assertFrozenStep} 의 사용 축 가드가 "V5 는 얼린 상한까지 읽었다" 라고
     * 말할 수 있는 근거가 바로 이 한 줄이다.
     */
    @Override
    public void applyActiveUsageCounts(long runId, LocalDateTime asOf, long maxUsageId) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("runId", runId)
                .addValue("asOf", asOf)
                .addValue("maxUsageId", maxUsageId);

        jdbcTemplate.update(APPLY_USAGE_COUNTS, params);
    }

    private static SqlParameterSource toParams(long runId, ReplayResult result) {
        return new MapSqlParameterSource()
                .addValue("runId", runId)
                .addValue("issuanceId", result.issuanceId())
                .addValue("state", result.state().name())
                .addValue("lastHistoryId", result.lastHistoryId())
                .addValue("lastEventAt", result.lastEventAt());
    }
}
