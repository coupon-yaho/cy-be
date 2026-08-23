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
@ConditionalOnProperty("observation.datasource.enabled")
public class JdbcRunTimeseriesArchiveStore implements ArchiveStore {

    private final JdbcTemplate writeJdbcTemplate;

    public JdbcRunTimeseriesArchiveStore(JdbcTemplate jdbcTemplate) {
        this.writeJdbcTemplate = jdbcTemplate;
    }

    @Override
    @Transactional
    public void deleteForRun(long benchmarkRunId) {
        writeJdbcTemplate.update("DELETE FROM run_timeseries WHERE benchmark_run_id = ?", benchmarkRunId);
    }

    @Override
    @Transactional
    public void insert(long benchmarkRunId, List<Sample> samples) {
        writeJdbcTemplate.batchUpdate("""
                INSERT INTO run_timeseries
                    (benchmark_run_id, metric, snapshot_sequence, observed_at, value, state, source_instance)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """, samples, samples.size(), (statement, sample) -> {
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
}
