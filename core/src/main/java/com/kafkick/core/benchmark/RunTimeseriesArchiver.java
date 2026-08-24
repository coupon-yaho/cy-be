package com.kafkick.core.benchmark;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.time.Duration;

import com.kafkick.core.support.exception.BusinessException;

/** 관측이 끝난 회차의 세 발표용 시계열을 Prometheus에서 MySQL로 복제한다. */
public class RunTimeseriesArchiver {

    private static final org.slf4j.Logger log =
        org.slf4j.LoggerFactory.getLogger(RunTimeseriesArchiver.class);

    public static final int DEFAULT_WRITE_CHUNK_SIZE = 500;
    static final Duration MAX_CLAIM_LEASE = Duration.ofDays(365);

    private final BenchmarkRunRepository runs;
    private final RangeSource source;
    private final ArchiveStore store;
    private final Duration claimLease;
    private final int maxArchiveSamples;
    private final int writeChunkSize;

    public RunTimeseriesArchiver(
        BenchmarkRunRepository runs, RangeSource source, ArchiveStore store, Duration claimLease,
        int maxArchiveSamples
    ) {
        this(runs, source, store, claimLease, maxArchiveSamples, DEFAULT_WRITE_CHUNK_SIZE);
    }

    public RunTimeseriesArchiver(
        BenchmarkRunRepository runs, RangeSource source, ArchiveStore store, Duration claimLease,
        int maxArchiveSamples, int writeChunkSize
    ) {
        this.runs = Objects.requireNonNull(runs);
        this.source = Objects.requireNonNull(source);
        this.store = Objects.requireNonNull(store);
        this.claimLease = Objects.requireNonNull(claimLease);
        if (claimLease.compareTo(Duration.ofSeconds(1)) < 0) {
            throw new IllegalArgumentException("archive claim lease는 1초 이상이어야 한다");
        }
        if (claimLease.compareTo(MAX_CLAIM_LEASE) > 0) {
            throw new IllegalArgumentException("archive claim lease가 지원 상한을 넘었다");
        }
        if (maxArchiveSamples <= 0) {
            throw new IllegalArgumentException("archive 표본 상한은 양수여야 한다");
        }
        this.maxArchiveSamples = maxArchiveSamples;
        if (writeChunkSize <= 0) {
            throw new IllegalArgumentException("archive 쓰기 청크 크기는 양수여야 한다");
        }
        this.writeChunkSize = writeChunkSize;
    }

    /** FAILED 회차를 포함해 완료된 관측 구간을 다시 복제한다. */
    public void archive(long benchmarkRunId) {
        BenchmarkRun run = runs.findById(benchmarkRunId)
                .orElseThrow(() -> notFound(benchmarkRunId));
        if (run.observationStoppedAt() == null) {
            throw illegalTransition(benchmarkRunId, "observation_stopped_at is null");
        }
        String claimToken = runs.claimArchive(benchmarkRunId, claimLease)
                .orElseThrow(() -> illegalTransition(
                    benchmarkRunId, "archive is already claimed or completed"));

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
                if (samples.size() > maxArchiveSamples) {
                    throw new IllegalStateException(
                        "archive 표본 상한을 넘었습니다: max=" + maxArchiveSamples
                            + " actual=" + samples.size());
                }
            }

            store.replaceForRun(benchmarkRunId, claimToken, samples, writeChunkSize);
        } catch (RuntimeException failure) {
            String message = failure.getMessage();
            String reason = message == null || message.isBlank()
                    ? failure.getClass().getSimpleName() : message;
            try {
                boolean recorded = runs.failArchive(benchmarkRunId, claimToken,
                    reason.substring(0, Math.min(reason.length(), 200)));
                if (!recorded) {
                    log.warn("archive failure status was not recorded: benchmarkRunId={} tokenPrefix={}",
                        benchmarkRunId, claimToken.substring(0, Math.min(8, claimToken.length())));
                }
            } catch (RuntimeException statusFailure) {
                failure.addSuppressed(statusFailure);
            }
            throw failure;
        }
    }

    /** 실패했거나 lease가 만료된 archive를 재실행한다. DONE 원본은 불변이다. */
    public void retry(long benchmarkRunId) {
        BenchmarkRun run = runs.findById(benchmarkRunId)
                .orElseThrow(() -> notFound(benchmarkRunId));
        if (run.archiveStatus() == BenchmarkArchiveStatus.DONE) {
            throw illegalTransition(benchmarkRunId, "archiveStatus=" + run.archiveStatus());
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

    /** 구현은 기존 원본 삭제와 전체 대체 입력을 한 트랜잭션으로 처리한다. */
    public interface ArchiveStore {
        void replaceForRun(long benchmarkRunId, String claimToken, List<Sample> samples, int chunkSize);
    }
}
