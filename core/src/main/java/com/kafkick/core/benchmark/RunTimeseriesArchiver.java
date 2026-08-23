package com.kafkick.core.benchmark;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.kafkick.core.support.exception.BusinessException;

/** 관측이 끝난 회차의 세 발표용 시계열을 Prometheus에서 MySQL로 복제한다. */
public class RunTimeseriesArchiver {

    static final int WRITE_CHUNK_SIZE = 500;

    private final BenchmarkRunRepository runs;
    private final RangeSource source;
    private final ArchiveStore store;

    public RunTimeseriesArchiver(BenchmarkRunRepository runs, RangeSource source, ArchiveStore store) {
        this.runs = Objects.requireNonNull(runs);
        this.source = Objects.requireNonNull(source);
        this.store = Objects.requireNonNull(store);
    }

    /** FAILED 회차를 포함해 완료된 관측 구간을 다시 복제한다. */
    public void archive(long benchmarkRunId) {
        BenchmarkRun run = runs.findById(benchmarkRunId)
                .orElseThrow(() -> notFound(benchmarkRunId));
        if (run.observationStoppedAt() == null) {
            throw illegalTransition(benchmarkRunId, "observation_stopped_at is null");
        }

        try {
            // 외부 HTTP와 검증은 DB 쓰기보다 먼저 끝낸다. Prometheus 지연 중 커넥션을 잡지 않는다.
            List<Sample> samples = new ArrayList<>();
            for (Metric metric : Metric.values()) {
                List<Sample> metricSamples = source.queryRange(
                        metric, run.startedAt(), run.observationStoppedAt(), 1);
                if (metric != Metric.LATENCY_P99 && metricSamples.isEmpty()) {
                    throw new IllegalStateException("필수 Prometheus 시계열이 비어 있습니다: " + metric);
                }
                validate(metric, run.startedAt(), run.observationStoppedAt(), metricSamples);
                samples.addAll(metricSamples);
            }

            store.deleteForRun(benchmarkRunId);
            for (int from = 0; from < samples.size(); from += WRITE_CHUNK_SIZE) {
                store.insert(benchmarkRunId,
                        samples.subList(from, Math.min(from + WRITE_CHUNK_SIZE, samples.size())));
            }
            runs.updateArchiveStatus(benchmarkRunId, BenchmarkArchiveStatus.DONE, null);
        } catch (RuntimeException failure) {
            String reason = failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage();
            runs.updateArchiveStatus(benchmarkRunId, BenchmarkArchiveStatus.FAILED,
                    reason.substring(0, Math.min(reason.length(), 500)));
            throw failure;
        }
    }

    /** 관리자 재실행 경로. 실패한 archive만 다시 실행한다. */
    public void retry(long benchmarkRunId) {
        BenchmarkRun run = runs.findById(benchmarkRunId)
                .orElseThrow(() -> notFound(benchmarkRunId));
        if (run.archiveStatus() != BenchmarkArchiveStatus.FAILED) {
            throw illegalTransition(benchmarkRunId, "archiveStatus=" + run.archiveStatus());
        }
        if (!runs.claimFailedArchive(benchmarkRunId)) {
            throw illegalTransition(benchmarkRunId, "archive retry is already claimed");
        }
        archive(benchmarkRunId);
    }

    private static void validate(Metric metric, Instant start, Instant end, List<Sample> samples) {
        Objects.requireNonNull(samples, "samples");
        long previous = -1;
        for (Sample sample : samples) {
            if (sample.metric() != metric || sample.sequence() <= previous
                    || sample.observedAt().isBefore(start) || sample.observedAt().isAfter(end)) {
                throw new IllegalArgumentException("Prometheus range 표본이 회차 구간/순서를 벗어났습니다: " + metric);
            }
            if ((sample.state() == State.VALID) != (sample.value() != null)) {
                throw new IllegalArgumentException("표본 값과 상태가 어긋났습니다: " + metric);
            }
            previous = sample.sequence();
        }
    }

    private static BusinessException notFound(long id) {
        return new BusinessException(BenchmarkErrorCode.RUN_NOT_FOUND, "benchmarkRunId=" + id);
    }

    private static BusinessException illegalTransition(long id, String actual) {
        return new BusinessException(BenchmarkErrorCode.ILLEGAL_TRANSITION,
                "benchmarkRunId=" + id + " " + actual);
    }

    public enum Metric { STOCK_REMAINING, LATENCY_P99, DB_POOL_USAGE }
    public enum State { VALID, UNAVAILABLE, N_A }

    public record Sample(Metric metric, long sequence, Instant observedAt, Double value,
                         State state, String sourceInstance) {
        public Sample {
            Objects.requireNonNull(metric);
            Objects.requireNonNull(observedAt);
            Objects.requireNonNull(state);
        }
    }

    public interface RangeSource {
        List<Sample> queryRange(Metric metric, Instant start, Instant end, int stepSeconds);
    }

    /** 구현은 운영 풀에만 쓴다. 각 호출은 짧은 독립 트랜잭션이다. */
    public interface ArchiveStore {
        void deleteForRun(long benchmarkRunId);
        void insert(long benchmarkRunId, List<Sample> samples);
    }
}
