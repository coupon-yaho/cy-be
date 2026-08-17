package com.kafkick.storage.verification;

import static com.kafkick.storage.verification.ColumnValues.toEnum;
import static com.kafkick.storage.verification.ColumnValues.toName;

import java.time.LocalDateTime;
import java.util.Optional;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.StatsStatus;
import com.kafkick.core.verification.VerdictType;
import com.kafkick.core.verification.VerificationRun;
import com.kafkick.core.verification.VerificationRunRepository;
import com.kafkick.core.verification.exception.VerificationErrorCode;

/**
 * JPA 엔티티를 만들지 않고 JdbcClient 로 간다.
 *
 * <p>검증 배치는 300만~534만 행을 읽고 결과만 쓴다. 영속성 컨텍스트가 할 일이 없고,
 * 규칙은 전부 집계 SQL 이라 엔티티를 두면 관리 비용만 는다.
 */
@Repository
public class VerificationRunJdbcAdapter implements VerificationRunRepository {

    private static final String INSERT = """
            INSERT INTO verification_runs
                (as_of, from_ts, scope, dataset, attempt, finding_count, started_at)
            VALUES (:asOf, :fromTs, :scope, :dataset, :attempt, :findingCount, :startedAt)
            """;

    private static final String UPDATE = """
            UPDATE verification_runs
               SET verdict             = :verdict,
                   stats_status        = :statsStatus,
                   finding_count       = :findingCount,
                   findings_checksum   = :findingsChecksum,
                   dataset_fingerprint = :datasetFingerprint,
                   finished_at         = :finishedAt
             WHERE id = :id
            """;

    private static final String SELECT_BY_ID = """
            SELECT id, as_of, from_ts, scope, dataset, attempt,
                   verdict, stats_status, finding_count,
                   findings_checksum, dataset_fingerprint, started_at, finished_at
              FROM verification_runs
             WHERE id = :id
            """;

    private static final String SELECT_BY_PARAMS = """
            SELECT id, as_of, from_ts, scope, dataset, attempt,
                   verdict, stats_status, finding_count,
                   findings_checksum, dataset_fingerprint, started_at, finished_at
              FROM verification_runs
             WHERE as_of = :asOf AND dataset = :dataset
               AND scope = :scope AND attempt = :attempt
            """;

    private static final RowMapper<VerificationRun> ROW_MAPPER = (rs, rowNum) -> VerificationRun.restore(
            rs.getLong("id"),
            rs.getObject("as_of", LocalDateTime.class),
            rs.getObject("from_ts", LocalDateTime.class),
            ScopeType.valueOf(rs.getString("scope")),
            DatasetType.valueOf(rs.getString("dataset")),
            rs.getInt("attempt"),
            toEnum(rs.getString("verdict"), VerdictType::valueOf),
            toEnum(rs.getString("stats_status"), StatsStatus::valueOf),
            rs.getInt("finding_count"),
            rs.getString("findings_checksum"),
            rs.getString("dataset_fingerprint"),
            rs.getObject("started_at", LocalDateTime.class),
            rs.getObject("finished_at", LocalDateTime.class)
    );

    private final JdbcClient jdbcClient;

    public VerificationRunJdbcAdapter(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    @Override
    public VerificationRun save(VerificationRun run) {
        KeyHolder keyHolder = new GeneratedKeyHolder();

        jdbcClient.sql(INSERT)
                .param("asOf", run.asOf())
                .param("fromTs", run.fromTs())
                .param("scope", run.scope().name())
                .param("dataset", run.dataset().name())
                .param("attempt", run.attempt())
                .param("findingCount", run.findingCount())
                .param("startedAt", run.startedAt())
                .update(keyHolder);

        Number generated = keyHolder.getKey();
        if (generated == null) {
            // 여기만 BusinessException 이 아니다. 행이 없다는 뜻이 아니라 INSERT 가 성공했는데
            // 드라이버가 생성 키를 안 준 것이라, 업무 조건이 아니라 인프라 계약 위반이다.
            throw new IllegalStateException("검증 실행 ID가 생성되지 않았습니다.");
        }

        return VerificationRun.restore(
                generated.longValue(),
                run.asOf(), run.fromTs(), run.scope(), run.dataset(), run.attempt(),
                run.verdict(), run.statsStatus(), run.findingCount(),
                run.findingsChecksum(), run.datasetFingerprint(),
                run.startedAt(), run.finishedAt()
        );
    }

    @Override
    public void update(VerificationRun run) {
        if (run.id() == null) {
            throw new IllegalArgumentException("갱신하려면 검증 실행 ID가 필요합니다.");
        }

        int updated = jdbcClient.sql(UPDATE)
                .param("verdict", toName(run.verdict()))
                .param("statsStatus", toName(run.statsStatus()))
                .param("findingCount", run.findingCount())
                .param("findingsChecksum", run.findingsChecksum())
                .param("datasetFingerprint", run.datasetFingerprint())
                .param("finishedAt", run.finishedAt())
                .param("id", run.id())
                .update();

        // save() 는 실패를 예외로 알리는데 update() 만 조용히 넘어가면, 정리 배치가 지운 run 을
        // 갱신하려 할 때 판정이 안 써졌는데도 잡이 COMPLETED 로 끝난다.
        if (updated != 1) {
            throw new BusinessException(
                    VerificationErrorCode.RUN_NOT_FOUND,
                    "검증 실행을 갱신하지 못했습니다. id=" + run.id() + " 갱신된 행=" + updated);
        }
    }

    @Override
    public Optional<VerificationRun> findByParams(
            LocalDateTime asOf,
            DatasetType dataset,
            ScopeType scope,
            int attempt
    ) {
        return jdbcClient.sql(SELECT_BY_PARAMS)
                .param("asOf", asOf)
                .param("dataset", dataset.name())
                .param("scope", scope.name())
                .param("attempt", attempt)
                .query(ROW_MAPPER)
                .optional();
    }

    @Override
    public Optional<VerificationRun> findById(long id) {
        return jdbcClient.sql(SELECT_BY_ID)
                .param("id", id)
                .query(ROW_MAPPER)
                .optional();
    }

    @Override
    public void updateStatsStatus(long runId, StatsStatus status) {
        int updated = jdbcClient.sql("""
                        UPDATE verification_runs SET stats_status = :statsStatus WHERE id = :id
                        """)
                .param("statsStatus", status.name())
                .param("id", runId)
                .update();

        if (updated != 1) {
            throw new BusinessException(
                    VerificationErrorCode.RUN_NOT_FOUND,
                    "통계 상태를 갱신하지 못했습니다. runId=" + runId + " 갱신행=" + updated);
        }
    }
}
