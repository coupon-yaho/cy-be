package com.kafkick.storage.db.benchmark;

import java.sql.Timestamp;
import java.util.List;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.benchmark.RunTimeseriesArchiver.ArchiveStore;
import com.kafkick.core.benchmark.RunTimeseriesArchiver.Sample;

/** run_timeseries 쓰기 어댑터. 한정자 없는 JdbcTemplate은 @Primary 운영 풀이다. */
@Repository
@ConditionalOnProperty(name = "observation.datasource.enabled", havingValue = "true")
public class JdbcRunTimeseriesArchiveStore implements ArchiveStore {

    private final JdbcTemplate writeJdbcTemplate;

    public JdbcRunTimeseriesArchiveStore(JdbcTemplate jdbcTemplate) {
        this.writeJdbcTemplate = jdbcTemplate;
    }

    @Override
    @Transactional
    public void replaceForRun(
        long benchmarkRunId, String claimToken, List<Sample> samples, int chunkSize
    ) {
        List<Long> owned = writeJdbcTemplate.queryForList("""
            SELECT id FROM benchmark_runs
            WHERE id = ? AND archive_status = 'IN_PROGRESS' AND archive_claim_token = ?
            FOR UPDATE
            """, Long.class, benchmarkRunId, claimToken);
        if (owned.size() != 1) {
            throw new IllegalStateException("archive claim 소유권을 잃었다: benchmarkRunId=" + benchmarkRunId);
        }
        writeJdbcTemplate.update("DELETE FROM run_timeseries WHERE benchmark_run_id = ?", benchmarkRunId);
        for (int from = 0; from < samples.size(); from += chunkSize) {
            List<Sample> chunk = samples.subList(
                from, Math.min(from + chunkSize, samples.size()));
            writeJdbcTemplate.batchUpdate("""
                INSERT INTO run_timeseries
                    (benchmark_run_id, metric, snapshot_sequence, observed_at, value, state, source_instance)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """, chunk, chunk.size(), (statement, sample) -> {
                    statement.setLong(1, benchmarkRunId);
                    statement.setString(2, sample.metric().name());
                    statement.setLong(3, sample.sequence());
                    statement.setTimestamp(4, Timestamp.from(sample.observedAt()));
                    if (sample.value() == null) statement.setNull(5, java.sql.Types.DOUBLE);
                    else statement.setDouble(5, sample.value());
                    statement.setString(6, sample.state().name());
                    statement.setString(7, sample.sourceInstance());
                });
        }
        int completed = writeJdbcTemplate.update("""
            UPDATE benchmark_runs
            SET archive_status = 'DONE', archive_failure_reason = NULL,
                archive_claimed_at = NULL, archive_claim_token = NULL
            WHERE id = ? AND archive_status = 'IN_PROGRESS' AND archive_claim_token = ?
            """, benchmarkRunId, claimToken);
        if (completed != 1) {
            throw new IllegalStateException("archive claim 소유권을 잃었다: benchmarkRunId=" + benchmarkRunId);
        }
    }
}
