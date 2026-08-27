package com.kafkick.api.admin.benchmark.dto;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import com.kafkick.core.benchmark.BenchmarkArchiveStatus;
import com.kafkick.core.benchmark.BenchmarkRun;
import com.kafkick.core.benchmark.BenchmarkRunStatus;
import com.kafkick.core.benchmark.ClientLoadSummary;
import com.kafkick.core.benchmark.RunTimeseriesArchiver;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;

/** Benchmark 실행 조건, 분리된 회차 시각, archive 상태와 공식 결과를 반환합니다. */
public record BenchmarkDetailResponse(Long benchmarkRunId, EngineVersion engineVersion,
        ReleaseStage releaseStage, QueueMode queueMode, String scenarioCode, BenchmarkRunStatus runStatus,
        BenchmarkArchiveStatus archiveStatus, String archiveFailureReason, Instant startedAt,
        Instant loadStoppedAt, Instant observationStoppedAt, Instant finalizedAt, ClientSummary clientSummary,
        List<ServerSample> serverSamples) {
    /** 상세의 시계열 목록을 불변으로 보호합니다. */
    public BenchmarkDetailResponse { Objects.requireNonNull(serverSamples, "serverSamples"); serverSamples = List.copyOf(serverSamples); }
    /** Core 회차와 archive 표본을 HTTP 상세 응답으로 투영합니다. */
    public static BenchmarkDetailResponse from(BenchmarkRun run, List<RunTimeseriesArchiver.Sample> samples) {
        Objects.requireNonNull(run, "run"); Objects.requireNonNull(samples, "samples");
        // DONE 전에는 빈 시계열을 정상 데이터처럼 해석할 수 없도록 archive 상태를 함께 노출한다.
        List<ServerSample> serverSamples = samples.stream().map(ServerSample::from).toList();
        ClientSummary client = run.client().map(ClientSummary::from).orElse(null);
        return new BenchmarkDetailResponse(run.id(), run.engineVersion(), run.releaseStage(), run.queueMode(),
                run.scenarioCode(), run.runStatus(), run.archiveStatus(), run.archiveFailureReason(), run.startedAt(),
                run.loadStoppedAt(), run.observationStoppedAt(), run.finalizedAt(), client, serverSamples);
    }
    /** 도구 이름과 무관한 부하 생성기 공식 요약입니다. */
    public record ClientSummary(long requestCount, long failureCount, long droppedIterations, double tps,
            double p95Millis, double p99Millis, Instant measuredAt) {
        private static ClientSummary from(ClientLoadSummary summary) { return new ClientSummary(summary.requestCount(), summary.failureCount(), summary.droppedIterations(), summary.tps(), summary.p95Millis(), summary.p99Millis(), summary.measuredAt()); }
    }
    /** archive에 저장된 서버 관측 표본 한 점입니다. */
    public record ServerSample(RunTimeseriesArchiver.Metric metric, long sequence, Instant observedAt,
            Double value, RunTimeseriesArchiver.State state, String sourceInstance) {
        private static ServerSample from(RunTimeseriesArchiver.Sample sample) { return new ServerSample(sample.metric(), sample.sequence(), sample.observedAt(), sample.value(), sample.state(), sample.sourceInstance()); }
    }
}
