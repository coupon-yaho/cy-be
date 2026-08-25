package com.kafkick.core.benchmark;

import java.util.List;

/** 완료된 Benchmark 회차의 archive 시계열을 읽는 기술 중립 포트입니다. */
public interface BenchmarkTimeseriesReader {

    /**
     * 한 회차에 저장된 archive 표본을 metric과 표본 순서대로 읽습니다.
     *
     * @param benchmarkRunId 조회할 Benchmark 회차 식별자
     * @return archive에 저장된 표본
     */
    List<RunTimeseriesArchiver.Sample> read(long benchmarkRunId);
}
