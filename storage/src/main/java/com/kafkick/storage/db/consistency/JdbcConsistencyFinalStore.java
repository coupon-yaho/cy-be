package com.kafkick.storage.db.consistency;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.admin.overview.calculator.ConsistencyActionContext;
import com.kafkick.core.consistency.ConsistencyEvaluation;
import com.kafkick.core.consistency.ConsistencyFinalObservation;
import com.kafkick.core.consistency.ConsistencyFinalStore;
import com.kafkick.core.consistency.ConsistencyGapType;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.consistency.GapValue;
import com.kafkick.core.consistency.Verdict;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;

/** 운영 풀로 FINAL 결과를 쓰고 관측 풀로 캠페인별 최신 결과를 읽습니다. */
@Repository
@ConditionalOnProperty(name = "observation.datasource.enabled", havingValue = "true")
public class JdbcConsistencyFinalStore implements ConsistencyFinalStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcConsistencyFinalStore.class);

    private final JdbcTemplate writeJdbcTemplate;
    private final JdbcTemplate observationJdbcTemplate;

    public JdbcConsistencyFinalStore(
            JdbcTemplate jdbcTemplate,
            @Qualifier("obs") JdbcTemplate observationJdbcTemplate) {
        this.writeJdbcTemplate = jdbcTemplate;
        this.observationJdbcTemplate = observationJdbcTemplate;
    }

    @Override
    public Optional<String> claim(long benchmarkRunId, Duration lease) {
        requireLease(lease);
        String token = UUID.randomUUID().toString();
        int claimed = writeJdbcTemplate.update("""
            UPDATE benchmark_runs
               SET consistency_status = 'IN_PROGRESS', consistency_failure_reason = NULL,
                   consistency_claimed_at = CURRENT_TIMESTAMP(6), consistency_claim_token = ?,
                   consistency_attempt_count = consistency_attempt_count + 1
             WHERE id = ? AND run_status = 'FINALIZED' AND coupon_id IS NOT NULL AND (
                   consistency_status IN ('NONE', 'FAILED')
                   OR (consistency_status = 'IN_PROGRESS'
                       AND consistency_claimed_at < TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(6))))
            """, token, benchmarkRunId, -lease.getSeconds());
        return claimed == 1 ? Optional.of(token) : Optional.empty();
    }

    @Override
    @Transactional
    public boolean complete(long benchmarkRunId, String claimToken, long couponId,
                            EngineVersion engineVersion, Instant evaluatedAt,
                            ConsistencyEvaluation evaluation) {
        if (evaluation.phase() != ConsistencyPhase.FINAL) {
            throw new IllegalArgumentException("FINAL 결과만 저장할 수 있습니다");
        }
        // 소유권 확인과 시도 번호 확보를 한 문장으로 묶는다. 행이 없으면 소유자가 아니다.
        Integer attemptNo = writeJdbcTemplate.query("""
            SELECT consistency_attempt_count FROM benchmark_runs
             WHERE id = ? AND coupon_id = ? AND run_status = 'FINALIZED'
               AND consistency_status = 'IN_PROGRESS' AND consistency_claim_token = ?
             FOR UPDATE
            """, rs -> rs.next() ? rs.getInt(1) : null, benchmarkRunId, couponId, claimToken);
        if (attemptNo == null) return false;

        GapValue active = evaluation.gaps().get(ConsistencyGapType.ACTIVE_DB_GAP);
        GapValue lua = evaluation.gaps().get(ConsistencyGapType.LUA_GAP);
        GapValue persist = evaluation.gaps().get(ConsistencyGapType.PERSIST_GAP);
        GapValue counter = evaluation.gaps().get(ConsistencyGapType.DB_COUNTER_GAP);
        GapValue over = evaluation.overIssued();
        writeJdbcTemplate.update("""
            INSERT INTO consistency_finals (
                run_id, coupon_id, engine_version, evaluated_at, attempt_no, verdict, severity,
                active_db_gap_value, active_db_gap_state, active_db_gap_observed_at,
                lua_gap_value, lua_gap_state, lua_gap_observed_at,
                persist_gap_value, persist_gap_state, persist_gap_observed_at,
                db_counter_gap_value, db_counter_gap_state, db_counter_gap_observed_at,
                over_issued_value, over_issued_state, over_issued_observed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, benchmarkRunId, couponId, engineVersion.name(), timestamp(evaluatedAt), attemptNo,
            evaluation.verdict().name(), evaluation.severity().name(),
            active.value(), active.state().name(), timestamp(active.observedAt()),
            lua.value(), lua.state().name(), timestamp(lua.observedAt()),
            persist.value(), persist.state().name(), timestamp(persist.observedAt()),
            counter.value(), counter.state().name(), timestamp(counter.observedAt()),
            over.value(), over.state().name(), timestamp(over.observedAt()));
        int completed = writeJdbcTemplate.update("""
            UPDATE benchmark_runs
               SET consistency_status = 'DONE', consistency_failure_reason = NULL,
                   consistency_claimed_at = NULL, consistency_claim_token = NULL
             WHERE id = ? AND consistency_status = 'IN_PROGRESS' AND consistency_claim_token = ?
            """, benchmarkRunId, claimToken);
        if (completed != 1) {
            throw new IllegalStateException("FINAL 정합성 claim 소유권을 잃었습니다: benchmarkRunId="
                    + benchmarkRunId);
        }
        return true;
    }

    @Override
    public boolean fail(long benchmarkRunId, String claimToken, String failureReason) {
        return terminate("FAILED", benchmarkRunId, claimToken, failureReason);
    }

    @Override
    public boolean expire(long benchmarkRunId, String claimToken, String failureReason) {
        return terminate("EXPIRED", benchmarkRunId, claimToken, failureReason);
    }

    private boolean terminate(String status, long benchmarkRunId, String claimToken,
                              String failureReason) {
        if (failureReason == null || failureReason.isBlank()) {
            throw new IllegalArgumentException(status + "에는 이유가 필요합니다");
        }
        if (failureReason.length() > FAILURE_REASON_MAX) {
            throw new IllegalArgumentException(
                    status + " 이유는 " + FAILURE_REASON_MAX + "자를 넘을 수 없습니다");
        }
        return writeJdbcTemplate.update("""
            UPDATE benchmark_runs
               SET consistency_status = ?, consistency_failure_reason = ?,
                   consistency_claimed_at = NULL, consistency_claim_token = NULL
             WHERE id = ? AND consistency_status = 'IN_PROGRESS' AND consistency_claim_token = ?
            """, status, failureReason, benchmarkRunId, claimToken) == 1;
    }

    @Override
    public ConsistencyFinalObservation findLatestByCouponId(long couponId) {
        try {
            var rows = observationJdbcTemplate.query("""
                SELECT f.*, c.name AS campaign_name, c.open_at
                  FROM consistency_finals f
                  LEFT JOIN coupons c ON c.id = f.coupon_id
                 WHERE f.coupon_id = ?
                 ORDER BY f.evaluated_at DESC, f.run_id DESC
                 LIMIT 1
                """, (rs, rowNumber) -> context(rs), couponId);
            if (!rows.isEmpty()) {
                return new ConsistencyFinalObservation(SourceStatus.VALID, rows.getFirst());
            }
            Integer exists = observationJdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM coupons WHERE id = ?", Integer.class, couponId);
            return new ConsistencyFinalObservation(
                    exists != null && exists > 0 ? SourceStatus.PENDING : SourceStatus.N_A, null);
        } catch (DataAccessException failure) {
            log.warn("FINAL 정합성 조회에 실패했습니다. couponId={}", couponId, failure);
            return new ConsistencyFinalObservation(SourceStatus.UNAVAILABLE, null);
        } catch (IllegalArgumentException failure) {
            log.error("consistency_finals의 enum 값이 코드와 어긋납니다. couponId={}", couponId, failure);
            return new ConsistencyFinalObservation(SourceStatus.UNAVAILABLE, null);
        }
    }

    private static ConsistencyActionContext context(ResultSet rs) throws SQLException {
        Map<ConsistencyGapType, GapValue> gaps = new EnumMap<>(ConsistencyGapType.class);
        gaps.put(ConsistencyGapType.ACTIVE_DB_GAP, gap(rs, "active_db_gap"));
        gaps.put(ConsistencyGapType.LUA_GAP, gap(rs, "lua_gap"));
        gaps.put(ConsistencyGapType.PERSIST_GAP, gap(rs, "persist_gap"));
        gaps.put(ConsistencyGapType.DB_COUNTER_GAP, gap(rs, "db_counter_gap"));
        ConsistencyEvaluation evaluation = new ConsistencyEvaluation(
                gaps, gap(rs, "over_issued"), ConsistencyPhase.FINAL,
                Verdict.valueOf(rs.getString("verdict")),
                Severity.valueOf(rs.getString("severity")));
        return new ConsistencyActionContext(
                rs.getLong("coupon_id"), rs.getString("campaign_name"),
                instant(rs, "open_at"), instant(rs, "evaluated_at"),
                EngineVersion.valueOf(rs.getString("engine_version")), evaluation);
    }

    private static GapValue gap(ResultSet rs, String prefix) throws SQLException {
        long raw = rs.getLong(prefix + "_value");
        Long value = rs.wasNull() ? null : raw;
        return new GapValue(value, SourceStatus.valueOf(rs.getString(prefix + "_state")),
                instant(rs, prefix + "_observed_at"));
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        Timestamp value = rs.getTimestamp(column);
        return value == null ? null : value.toInstant();
    }

    private static Timestamp timestamp(Instant value) {
        return value == null ? null : Timestamp.from(value);
    }

    private static void requireLease(Duration lease) {
        if (lease == null || lease.compareTo(Duration.ofSeconds(1)) < 0
                || lease.getNano() != 0 || lease.compareTo(Duration.ofDays(365)) > 0) {
            throw new IllegalArgumentException("FINAL 정합성 claim lease는 1초~365일 정수 초여야 합니다");
        }
    }
}
