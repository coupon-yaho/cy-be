package com.kafkick.api.admin.benchmark.dto;

import java.time.Instant;
import java.util.List;

import com.kafkick.api.admin.support.ObservedValue;
import com.kafkick.core.admin.BenchmarkRunState;
import com.kafkick.core.admin.EngineVersion;
import com.kafkick.core.admin.QueueMode;
import com.kafkick.core.admin.ReleaseStage;
import com.kafkick.core.verification.VerdictType;

/** 벤치마크 실행 조건·공식 결과·서버 시계열을 구분한 상세 응답입니다. */
public record BenchmarkDetailResponse(Long benchmarkRunId, EngineVersion engineVersion,
                                      ReleaseStage releaseStage, QueueMode queueMode, String scenarioCode,
                                      BenchmarkRunState state, VerdictType verdict, Instant startedAt,
                                      Instant finishedAt, K6Summary k6, List<ServerSample> serverSamples) {
    /** k6 종단 관측 공식 비교값입니다. */
    public record K6Summary(Double tps, Double p99Millis, Long failureCount, Double failureRate) { }

    /** 서버 관측 시계열 한 점이며 원천 미수집 값을 0으로 만들지 않습니다. */
    public record ServerSample(Instant observedAt, ObservedValue<Double> serverP99Millis,
                               ObservedValue<Double> dbPoolPercent,
                               ObservedValue<Long> remainingStock) { }
}
