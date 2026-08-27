package com.kafkick.storage.db.consistency;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
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

/** 운영 풀로 FINAL 결과를 쓰고 관측 풀로 회차별 최신 결과를 읽습니다. */
@Repository
@ConditionalOnProperty(name = "observation.datasource.enabled", havingValue = "true")
public class JdbcConsistencyFinalStore implements ConsistencyFinalStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcConsistencyFinalStore.class);

    private final JdbcTemplate writeJdbcTemplate;
    private final NamedParameterJdbcTemplate observationJdbcTemplate;

    public JdbcConsistencyFinalStore(
            JdbcTemplate jdbcTemplate,
            @Qualifier("obs") JdbcTemplate observationJdbcTemplate) {
        this.writeJdbcTemplate = jdbcTemplate;
        this.observationJdbcTemplate = new NamedParameterJdbcTemplate(observationJdbcTemplate);
    }

    @Override
    @Transactional
    public Optional<Claim> claim(long benchmarkRunId, Duration lease) {
        requireLease(lease);
        // 사유 읽기와 claim 을 같은 트랜잭션의 행 락 안에 묶는다. 따로 읽으면 그 사이
        // 다른 작업자가 남긴 사유를 놓친다.
        String previousReason = writeJdbcTemplate.query(
                "SELECT consistency_failure_reason FROM benchmark_runs WHERE id = ? FOR UPDATE",
                rs -> rs.next() ? rs.getString(1) : null, benchmarkRunId);
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
        return claimed == 1 ? Optional.of(new Claim(token, previousReason)) : Optional.empty();
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
    public Map<Long, ConsistencyFinalObservation> findLatestByCouponIds(List<Long> couponIds) {
        List<Long> requestedIds = requestedIds(couponIds);
        if (requestedIds.isEmpty()) {
            return Map.of();
        }
        LinkedHashMap<Long, ConsistencyFinalObservation> observations = new LinkedHashMap<>();
        for (Long couponId : requestedIds) {
            observations.put(couponId, new ConsistencyFinalObservation(SourceStatus.N_A, null));
        }
        try {
            List<FinalRow> rows = observationJdbcTemplate.query("""
                WITH ranked_finals AS (
                    SELECT f.*,
                           ROW_NUMBER() OVER (
                               PARTITION BY f.coupon_id
                               ORDER BY f.evaluated_at DESC, f.run_id DESC
                           ) AS final_rank
                      FROM consistency_finals f
                      JOIN benchmark_runs r
                        ON r.id = f.run_id AND r.run_status = 'FINALIZED'
                     WHERE f.coupon_id IN (:couponIds)
                ), benchmark_rounds AS (
                    SELECT coupon_id,
                           MAX(CASE WHEN consistency_status <> 'EXPIRED' THEN 1 ELSE 0 END)
                               AS has_non_expired_run
                      FROM benchmark_runs
                     WHERE coupon_id IN (:couponIds)
                     GROUP BY coupon_id
                )
                SELECT b.coupon_id AS requested_coupon_id,
                       b.has_non_expired_run,
                       f.run_id, f.engine_version, f.evaluated_at, f.verdict, f.severity,
                       f.active_db_gap_value, f.active_db_gap_state, f.active_db_gap_observed_at,
                       f.lua_gap_value, f.lua_gap_state, f.lua_gap_observed_at,
                       f.persist_gap_value, f.persist_gap_state, f.persist_gap_observed_at,
                       f.db_counter_gap_value, f.db_counter_gap_state, f.db_counter_gap_observed_at,
                       f.over_issued_value, f.over_issued_state, f.over_issued_observed_at,
                       c.name AS campaign_name, c.open_at
                  FROM benchmark_rounds b
                  LEFT JOIN ranked_finals f
                    ON f.coupon_id = b.coupon_id AND f.final_rank = 1
                  LEFT JOIN coupons c ON c.id = b.coupon_id
                """, new MapSqlParameterSource("couponIds", requestedIds),
                    (rs, rowNumber) -> finalRow(rs));
            for (FinalRow row : rows) {
                // Benchmark 미실행, FINAL 계산 대기, 만료된 회차를 구분합니다.
                if (!row.hasFinal()) {
                    observations.put(row.couponId(),
                            new ConsistencyFinalObservation(
                                    row.hasNonExpiredRun()
                                            ? SourceStatus.PENDING : SourceStatus.UNAVAILABLE,
                                    null));
                    continue;
                }
                try {
                    observations.put(row.couponId(),
                            new ConsistencyFinalObservation(SourceStatus.VALID, context(row)));
                } catch (RuntimeException failure) {
                    log.error("consistency_finals 행을 복구할 수 없습니다. couponId={}",
                            row.couponId(), failure);
                    observations.put(row.couponId(),
                            new ConsistencyFinalObservation(SourceStatus.UNAVAILABLE, null));
                }
            }
        } catch (DataAccessException failure) {
            log.warn("FINAL 정합성 일괄 조회에 실패했습니다. couponIds={}", requestedIds, failure);
            for (Long couponId : requestedIds) {
                observations.put(couponId,
                        new ConsistencyFinalObservation(SourceStatus.UNAVAILABLE, null));
            }
        }
        return Collections.unmodifiableMap(observations);
    }

    private static List<Long> requestedIds(List<Long> couponIds) {
        if (couponIds == null) {
            throw new IllegalArgumentException("FINAL 조회 회차 ID 목록이 필요합니다.");
        }
        Set<Long> distinctIds = new LinkedHashSet<>();
        for (Long couponId : couponIds) {
            if (couponId == null || couponId <= 0L) {
                throw new IllegalArgumentException("FINAL 조회 회차 ID는 양수여야 합니다: " + couponId);
            }
            distinctIds.add(couponId);
        }
        return List.copyOf(distinctIds);
    }

    private static FinalRow finalRow(ResultSet rs) throws SQLException {
        return new FinalRow(
                rs.getLong("requested_coupon_id"), rs.getObject("run_id") != null,
                rs.getBoolean("has_non_expired_run"),
                rs.getString("campaign_name"), instant(rs, "open_at"),
                instant(rs, "evaluated_at"), rs.getString("engine_version"),
                rs.getString("verdict"), rs.getString("severity"),
                rawGap(rs, "active_db_gap"), rawGap(rs, "lua_gap"),
                rawGap(rs, "persist_gap"), rawGap(rs, "db_counter_gap"),
                rawGap(rs, "over_issued"));
    }

    private static RawGap rawGap(ResultSet rs, String prefix) throws SQLException {
        long raw = rs.getLong(prefix + "_value");
        Long value = rs.wasNull() ? null : raw;
        return new RawGap(value, rs.getString(prefix + "_state"),
                instant(rs, prefix + "_observed_at"));
    }

    private static ConsistencyActionContext context(FinalRow row) {
        Map<ConsistencyGapType, GapValue> gaps = new EnumMap<>(ConsistencyGapType.class);
        gaps.put(ConsistencyGapType.ACTIVE_DB_GAP, gap(row.activeDbGap()));
        gaps.put(ConsistencyGapType.LUA_GAP, gap(row.luaGap()));
        gaps.put(ConsistencyGapType.PERSIST_GAP, gap(row.persistGap()));
        gaps.put(ConsistencyGapType.DB_COUNTER_GAP, gap(row.dbCounterGap()));
        ConsistencyEvaluation evaluation = new ConsistencyEvaluation(
                gaps, gap(row.overIssued()), ConsistencyPhase.FINAL,
                Verdict.valueOf(row.verdict()), Severity.valueOf(row.severity()));
        return new ConsistencyActionContext(
                row.couponId(), row.campaignName(), row.opensAt(), row.evaluatedAt(),
                EngineVersion.valueOf(row.engineVersion()), evaluation);
    }

    private static GapValue gap(RawGap gap) {
        return new GapValue(gap.value(), SourceStatus.valueOf(gap.state()), gap.observedAt());
    }

    private record RawGap(Long value, String state, Instant observedAt) { }

    private record FinalRow(
            long couponId,
            boolean hasFinal,
            boolean hasNonExpiredRun,
            String campaignName,
            Instant opensAt,
            Instant evaluatedAt,
            String engineVersion,
            String verdict,
            String severity,
            RawGap activeDbGap,
            RawGap luaGap,
            RawGap persistGap,
            RawGap dbCounterGap,
            RawGap overIssued
    ) { }

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
