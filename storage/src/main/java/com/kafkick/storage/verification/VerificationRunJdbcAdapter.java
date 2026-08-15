package com.kafkick.storage.verification;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.function.Function;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.StatsStatus;
import com.kafkick.core.verification.VerdictType;
import com.kafkick.core.verification.VerificationRun;
import com.kafkick.core.verification.VerificationRunRepository;

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

    private static final RowMapper<VerificationRun> ROW_MAPPER = (rs, rowNum) -> VerificationRun.restore(
            rs.getLong("id"),
            toLocalDateTime(rs.getTimestamp("as_of")),
            toLocalDateTime(rs.getTimestamp("from_ts")),
            ScopeType.valueOf(rs.getString("scope")),
            DatasetType.valueOf(rs.getString("dataset")),
            rs.getInt("attempt"),
            toEnum(rs.getString("verdict"), VerdictType::valueOf),
            toEnum(rs.getString("stats_status"), StatsStatus::valueOf),
            rs.getInt("finding_count"),
            rs.getString("findings_checksum"),
            rs.getString("dataset_fingerprint"),
            toLocalDateTime(rs.getTimestamp("started_at")),
            toLocalDateTime(rs.getTimestamp("finished_at"))
    );

    private final JdbcClient jdbcClient;

    public VerificationRunJdbcAdapter(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    @Override
    public VerificationRun save(VerificationRun run) {
        KeyHolder keyHolder = new GeneratedKeyHolder();

        jdbcClient.sql(INSERT)
                .param("asOf", Timestamp.valueOf(run.asOf()))
                .param("fromTs", toTimestamp(run.fromTs()))
                .param("scope", run.scope().name())
                .param("dataset", run.dataset().name())
                .param("attempt", run.attempt())
                .param("findingCount", run.findingCount())
                .param("startedAt", Timestamp.valueOf(run.startedAt()))
                .update(keyHolder);

        Number generated = keyHolder.getKey();
        if (generated == null) {
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

        jdbcClient.sql(UPDATE)
                .param("verdict", toName(run.verdict()))
                .param("statsStatus", toName(run.statsStatus()))
                .param("findingCount", run.findingCount())
                .param("findingsChecksum", run.findingsChecksum())
                .param("datasetFingerprint", run.datasetFingerprint())
                .param("finishedAt", toTimestamp(run.finishedAt()))
                .param("id", run.id())
                .update();
    }

    @Override
    public Optional<VerificationRun> findById(long id) {
        return jdbcClient.sql(SELECT_BY_ID)
                .param("id", id)
                .query(ROW_MAPPER)
                .optional();
    }

    private static Timestamp toTimestamp(LocalDateTime value) {
        return value == null ? null : Timestamp.valueOf(value);
    }

    private static LocalDateTime toLocalDateTime(Timestamp value) {
        return value == null ? null : value.toLocalDateTime();
    }

    private static String toName(Enum<?> value) {
        return value == null ? null : value.name();
    }

    private static <E> E toEnum(String value, Function<String, E> parser) {
        return value == null ? null : parser.apply(value);
    }
}
