package com.kafkick.core.benchmark;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.time.Duration;

import com.kafkick.core.support.exception.BusinessException;

/** 관측이 끝난 회차의 발표용 시계열을 Prometheus에서 MySQL로 복제한다. */
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
        if (claimLease.getNano() != 0) {
            throw new IllegalArgumentException("archive claim lease는 정수 초여야 한다");
        }
        if (claimLease.compareTo(MAX_CLAIM_LEASE) > 0) {
            throw new IllegalArgumentException("archive claim lease가 지원 상한을 넘었다");
        }
        // 축 수가 곧 회차 길이 한계다. 상한은 회차 <전체> 표본 수라 archive 가능한 최대 회차
        // 길이 = maxArchiveSamples ÷ Metric.values().length ÷ step(1초) 다. 축이 셋이던 때
        // 10,000 은 3,333초(≈55분)였고 넷인 지금은 2,500초(≈41분)다. 측정 프로토콜은
        // load-hold 5초 + observation-hold 60초로 ApiTopologyValidator 가 불일치를 거절하므로
        // 실제 회차는 ~65초·4축 264행이고 상한까지 37배 남는다 — 그래서 기본값을 안 올렸다.
        // 프로토콜을 길게 바꾸면 이 나눗셈을 먼저 다시 하라. 넘기면 회차가 끝난 뒤에 터지고
        // Prometheus retention 이 지나면 재수집이 안 된다.
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
                if (metricSamples.isEmpty()) {
                    if (!metric.allowsEmptySamples()) {
                        throw new IllegalStateException("필수 Prometheus 시계열이 비어 있습니다: " + metric);
                    }
                    metricSamples = List.of(emptyAxisMarker(
                            metric, run.startedAt(), run.observationStoppedAt()));
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

    /**
     * 표본이 없는 축에 <b>이유</b>를 한 줄 남긴다.
     *
     * <p>행을 아예 안 쓰면 "그런 일이 한 건도 없었다" 와 "아예 못 쟀다" 가 같은 모양이 된다.
     * archive 는 불변이라 그 구분은 나중에 만들 수 없다 — 회차 비교 화면이 전자를 "깨끗한
     * 회차" 로 읽는데 실은 계측이 죽어 있던 회차일 수 있다.</p>
     *
     * <p>그래서 원천에 한 번 더 묻는다. 미터 자체가 그 구간에 있었으면 축이 빈 것은 사실이므로
     * {@link State#N_A}(해당 없음), 미터가 없었으면 재지 못한 것이므로
     * {@link State#UNAVAILABLE} 이다. 화면 경로가 {@code PENDING} 과 {@code UNAVAILABLE} 을
     * 가르는 것과 같은 판정이고, archive 도 같은 어휘를 쓰게 된다.</p>
     *
     * <p><b>반대 방향 실패</b> — 이 질의가 실패하면 회차 archive 가 통째로 실패한다. 추측해서
     * 둘 중 하나를 적으면 그 거짓이 영구히 남으므로, 모르면 안 쓰는 쪽을 택한다.</p>
     */
    private Sample emptyAxisMarker(Metric metric, Instant startedAt, Instant observationStoppedAt) {
        // 표본을 뜬 것과 같은 구간을 묻는다. 구간이 다르면 "그 구간에 미터가 있었나" 의 답이
        // 이 회차의 답이 아니게 된다.
        State state = source.sourceExists(metric, startedAt, observationStoppedAt)
                ? State.N_A
                : State.UNAVAILABLE;
        // 회차 시작 시각에 한 줄이다. 구간 어디에 두어도 뜻이 같고, 시작이면 uk_run_metric_seq
        // 의 sequence 0 과 짝이 맞아 표본이 생긴 회차와 자리가 겹치지 않는다.
        return new Sample(metric, 0, startedAt, null, state, null);
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

    /**
     * archive 하는 시계열 종류다.
     *
     * <p><b>DB 의 {@code ck_timeseries_metric} 과 같은 집합이어야 한다.</b> 한쪽만 늘리면
     * 컴파일도 되고 단위 테스트도 통과하다가 실제 회차 archive 에서 처음 터지는데, 그때는
     * 회차가 이미 끝나 있어 재수집이 안 된다. 둘을 잇는 계약
     * 테스트는 api 모듈에 있다 — 여기서 이름을 박으면 그쪽이 옮겨져도 컴파일러가 안 잡는다.</p>
     *
     * <p><b>이름을 바꾸지 않는다.</b> 완료 회차의 행은 불변이라 개명하면 과거 행을 UPDATE 해야
     * 하고, 그 순간 과거 회차와 현재 회차의 비교 축이 갈린다. 축은 더하기만 한다 —
     * {@link #LATENCY_P99} 는 OBS-31 이래 <b>성공 경로</b>를 뜻하며 그 뜻은 유지된다.</p>
     */
    public enum Metric {

        STOCK_REMAINING(false),

        /** 성공 경로 지연. 표본이 아직 없는 회차가 있어 비어도 실패로 보지 않는다. */
        LATENCY_P99(true),

        /** 시스템 실패 지연. <b>0건이 정상</b>이라 비는 것이 기대되는 상태다. */
        LATENCY_P99_SYSTEM_FAILURE(true),

        DB_POOL_USAGE(false);

        private final boolean allowsEmptySamples;

        Metric(boolean allowsEmptySamples) {
            this.allowsEmptySamples = allowsEmptySamples;
        }

        /**
         * 표본이 하나도 없어도 회차 archive 를 실패시키지 않는가.
         *
         * <p><b>값 옆에 두는 이유.</b> 목록을 따로 들면 축을 더할 때 그 목록만 안 고쳐지고,
         * 그 축이 0건인 첫 회차가 "필수 시계열이 비었다" 로 죽는다. 여기 두면 값을 더하는
         * 순간 판단을 강제한다 — 세 계약(SeriesKey · CHECK · 이 enum)을 잇는 테스트가
         * 못 보는 <b>네 번째 사본</b>을 아예 만들지 않는 쪽이다.</p>
         *
         * <p>⚠️ true 의 대가는 침묵이다. 원천이 죽으면 질의가 예외로 실패하지만, 살아 있는
         * 원천이 <b>빈 matrix</b> 를 돌려주면 "그런 일이 없었다" 와 구분되지 않은 채 DONE 으로
         * 확정된다. archive 는 불변이라 사후 구분이 안 된다 — 원천 부재와 0건을 갈라 적는 것은
         * 후속 티켓이다.</p>
         */
        public boolean allowsEmptySamples() {
            return allowsEmptySamples;
        }
    }
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

        /**
         * 그 구간에 <b>이 축의 원천 미터 자체</b>가 있었는가.
         *
         * <p>축의 표본이 비었을 때만 묻는다. 축이 비어 있어도 미터가 있었다면 "그런 일이
         * 없었다" 이고, 미터가 없었다면 "재지 못했다" 다 — 둘은 운영자가 취할 행동이 반대다.
         * 축 라벨을 빼고 미터 계열의 존재만 본다.</p>
         *
         * <p><b>기본 구현을 두지 않는다.</b> 두면 구현을 빼먹은 원천이 평소엔 멀쩡하다가
         * 축이 처음 비는 회차에서 터지고, 그때는 회차가 이미 끝나 있다.</p>
         *
         * @throws RuntimeException 물어보지 못했으면 던진다. 추측한 값을 적으면 영구히 남는다
         */
        boolean sourceExists(Metric metric, Instant start, Instant end);
    }

    /** 구현은 기존 원본 삭제와 전체 대체 입력을 한 트랜잭션으로 처리한다. */
    public interface ArchiveStore {
        void replaceForRun(long benchmarkRunId, String claimToken, List<Sample> samples, int chunkSize);
    }
}
