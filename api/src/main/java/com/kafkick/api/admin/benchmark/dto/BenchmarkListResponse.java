package com.kafkick.api.admin.benchmark.dto;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import com.kafkick.core.benchmark.BenchmarkArchiveStatus;
import com.kafkick.core.benchmark.BenchmarkRun;
import com.kafkick.core.benchmark.BenchmarkRunStatus;
import com.kafkick.core.observation.EngineVersion;

/** Benchmark 실행을 최신 시작 시각부터 과거 방향으로 반환하는 목록 응답입니다. */
public record BenchmarkListResponse(List<BenchmarkSummary> items, String nextBeforeCursor, boolean hasOlder) {
    /** 목록을 불변으로 보호합니다. */
    public BenchmarkListResponse { Objects.requireNonNull(items, "items"); items = List.copyOf(items); }
    /** @return 다음 페이지가 없는 빈 Benchmark 목록 */
    public static BenchmarkListResponse draft() { return new BenchmarkListResponse(List.of(), null, false); }
    /** Core 회차를 목록에 필요한 현재 상태로 투영합니다. */
    public static BenchmarkSummary from(BenchmarkRun run) {
        Objects.requireNonNull(run, "run");
        return new BenchmarkSummary(run.id(), run.engineVersion(), run.scenarioCode(), run.startedAt(),
                run.runStatus(), run.archiveStatus());
    }
    /** 한 번의 Benchmark 실행을 나타내는 목록 항목입니다. */
    public record BenchmarkSummary(Long benchmarkRunId, EngineVersion engineVersion, String scenarioCode,
                                   Instant startedAt, BenchmarkRunStatus runStatus,
                                   BenchmarkArchiveStatus archiveStatus) { }
}
