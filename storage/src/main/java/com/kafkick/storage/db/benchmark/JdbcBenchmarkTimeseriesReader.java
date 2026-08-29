package com.kafkick.storage.db.benchmark;

import java.sql.Timestamp;
import java.util.List;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.kafkick.core.benchmark.BenchmarkTimeseriesReader;
import com.kafkick.core.benchmark.RunTimeseriesArchiver;

/** run_timeseries archive를 관측 전용 JDBC 풀로 읽는 어댑터입니다. */
@Repository
@ConditionalOnProperty(name = "observation.datasource.enabled", havingValue = "true")
public class JdbcBenchmarkTimeseriesReader implements BenchmarkTimeseriesReader {

    private final JdbcTemplate observationJdbcTemplate;

    /** 관측 전용 템플릿을 주입받습니다. */
    public JdbcBenchmarkTimeseriesReader(@Qualifier("obs") JdbcTemplate observationJdbcTemplate) {
        this.observationJdbcTemplate = observationJdbcTemplate;
    }

    /** 회차 archive를 metric과 snapshot sequence 순서로 읽습니다. */
    @Override
    public List<RunTimeseriesArchiver.Sample> read(long benchmarkRunId) {
        return observationJdbcTemplate.query("""
                SELECT metric, snapshot_sequence, observed_at, value, state, source_instance
                FROM run_timeseries WHERE benchmark_run_id = ?
                ORDER BY metric ASC, snapshot_sequence ASC
                """, (rs, rowNumber) -> {
            Timestamp observedAt = rs.getTimestamp("observed_at");
            return new RunTimeseriesArchiver.Sample(
                    RunTimeseriesArchiver.Metric.valueOf(rs.getString("metric")),
                    rs.getLong("snapshot_sequence"), observedAt.toInstant(),
                    rs.getObject("value", Double.class),
                    RunTimeseriesArchiver.State.valueOf(rs.getString("state")),
                    rs.getString("source_instance"));
        }, benchmarkRunId);
    }
}
