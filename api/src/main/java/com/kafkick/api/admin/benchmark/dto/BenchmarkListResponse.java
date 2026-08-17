package com.kafkick.api.admin.benchmark.dto;

import java.time.Instant;
import java.util.List;

import com.kafkick.api.admin.support.ObservedValue;
import com.kafkick.core.admin.BenchmarkRunState;
import com.kafkick.core.admin.EngineVersion;
import com.kafkick.core.verification.VerdictType;

/**
 * Benchmark 실행을 최신 시작 시각부터 과거 방향으로 반환하는 목록 응답 초안입니다.
 *
 * <p>{@code nextBeforeCursor}는 다음 과거 페이지가 있을 때만 존재하고, {@code hasOlder}는 더 오래된 실행의
 * 존재 여부를 나타냅니다. 실행 중인 항목은 {@code finishedAt}과 {@code verdict}가 null일 수 있습니다.
 * 엔진 버전과 실행 상태는 공용 enum으로 직렬화됩니다.</p>
 *
 * @param items 최신 실행부터 과거 실행 순서로 정렬된 목록
 * @param nextBeforeCursor 다음 과거 페이지 조회에 사용할 cursor; 다음 페이지가 없으면 null
 * @param hasOlder 더 오래된 실행의 존재 여부
 */
public record BenchmarkListResponse(List<BenchmarkSummary> items, String nextBeforeCursor, boolean hasOlder) {
    /**
     * 선구축 단계의 JSON 필드 계약을 검증하기 위한 빈 목록 예시를 만듭니다.
     *
     * @return 다음 페이지가 없는 빈 Benchmark 목록
     */
    public static BenchmarkListResponse draft() { return new BenchmarkListResponse(List.of(), null, false); }

    /**
     * 한 번의 Benchmark 실행과 비교 화면에 필요한 대표 관측값을 나타냅니다.
     * 각 성능 값은 원천별 수집 실패를 0으로 숨기지 않도록 {@link ObservedValue}로 감쌉니다.
     *
     * @param benchmarkRunId Benchmark 실행 식별자
     * @param engineVersion 실행에 사용한 엔진 버전
     * @param scenarioCode 실행한 부하 시나리오 코드
     * @param startedAt 실행 시작 시각
     * @param finishedAt 실행 종료 시각; 실행 중이면 null
     * @param state 실행 상태
     * @param verdict 최종 PASS/FAIL 판정; 실행 중이면 null
     * @param issueAttemptRps 초당 발급 시도 수
     * @param successP99Millis 성공 요청의 p99 지연 시간(ms)
     * @param systemFailureRate 시스템 실패 비율
     * @param overIssuedCount 초과 발급 건수
     */
    public record BenchmarkSummary(
            Long benchmarkRunId,
            EngineVersion engineVersion,
            String scenarioCode,
            Instant startedAt,
            Instant finishedAt,
            BenchmarkRunState state,
            VerdictType verdict,
            ObservedValue<Double> issueAttemptRps,
            ObservedValue<Double> successP99Millis,
            ObservedValue<Double> systemFailureRate,
            ObservedValue<Long> overIssuedCount
    ) { }
}
