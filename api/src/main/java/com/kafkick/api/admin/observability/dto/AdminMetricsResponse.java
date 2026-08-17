package com.kafkick.api.admin.observability.dto;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import com.kafkick.api.admin.support.ObservedValue;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.consistency.Verdict;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;

/**
 * 특정 관측 범위와 집계 구간의 정합성·트래픽·지연·의존성 상태를 한 snapshot으로 반환하는 응답 초안입니다.
 *
 * <p>각 원천은 독립적으로 실패할 수 있으므로 수집 실패를 0으로 바꾸지 않고 {@link ObservedValue}의
 * 상태와 관측 시각을 유지합니다. 정합성 {@code phase}가 LIVE이면 {@code verdict}는 null이고, FINAL이면
 * PASS 또는 FAIL이어야 합니다. 범위·구간·단계·심각도와 세부 지표는 모두 명시적 enum·record 계약입니다.</p>
 *
 * @param scope GLOBAL, COUPON, BENCHMARK_RUN 중 실제 관측 범위
 * @param snapshotAt 응답 지표의 기준 시각
 * @param window 집계 구간
 * @param consistency LIVE/FINAL 정합성 판정과 독립 gap
 * @param traffic 발급 경로별 처리량
 * @param latency 결과 분류별 지연 분포
 * @param dependencies Redis·HikariCP·Kafka 의존성 지표
 * @param persistence 비동기 영속화 지연 관측값
 * @param circuitBreakers 회로 차단기별 상태 목록
 */
public record AdminMetricsResponse(
        MetricsScope scope,
        Instant snapshotAt,
        MetricsWindow window,
        ConsistencyResponse consistency,
        TrafficMetrics traffic,
        LatencyMetrics latency,
        DependencyMetrics dependencies,
        ObservedValue<PersistenceLagSummary> persistence,
        List<CircuitBreakerSummary> circuitBreakers
) {

    /**
     * 모든 세부 지표가 아직 수집되지 않은 관측 응답 예시를 만듭니다.
     *
     * @param window 예시에 사용할 집계 구간
     * @return GLOBAL/LIVE 범위이고 관측값이 PENDING/null인 응답
     */
    public static AdminMetricsResponse draft(MetricsWindow window) {
        // 직렬화 초안에서는 아직 수집되지 않은 비율을 0이 아닌 PENDING/null로 표현합니다.
        ObservedValue<Double> pendingRate = new ObservedValue<>(null, SourceStatus.PENDING, null);
        return new AdminMetricsResponse(
                new MetricsScope(MetricsScopeType.GLOBAL, null, null),
                Instant.EPOCH,
                window,
                new ConsistencyResponse(ConsistencyPhase.LIVE, null, Severity.NONE, null, null, null, null, null),
                new TrafficMetrics(pendingRate, pendingRate, pendingRate, pendingRate, pendingRate),
                new LatencyMetrics(null, null, null),
                new DependencyMetrics(null, null, null),
                new ObservedValue<>(null, SourceStatus.PENDING, null),
                List.of()
        );
    }

    /**
     * 지표의 관측 범위를 나타냅니다. GLOBAL은 두 ID가 모두 null이고, COUPON과 BENCHMARK_RUN은
     * 각각 대응하는 식별자 하나만 가집니다.
     *
     * @param type 관측 범위 유형
     * @param couponId COUPON 범위의 쿠폰 식별자; 다른 범위이면 null
     * @param benchmarkRunId BENCHMARK_RUN 범위의 실행 식별자; 다른 범위이면 null
     */
    public record MetricsScope(MetricsScopeType type, Long couponId, Long benchmarkRunId) {

        /**
         * 범위 유형마다 허용되는 식별자 하나만 보유하도록 생성 시점에 검증합니다.
         *
         * @throws NullPointerException type이 null인 경우
         * @throws IllegalArgumentException 범위에 필요한 식별자가 없거나 불필요한 식별자가 지정된 경우
         */
        public MetricsScope {
            Objects.requireNonNull(type, "type");
            switch (type) {
                case GLOBAL -> {
                    if (couponId != null || benchmarkRunId != null) {
                        throw new IllegalArgumentException("GLOBAL 범위에는 식별자를 지정할 수 없습니다.");
                    }
                }
                case COUPON -> {
                    if (couponId == null || couponId <= 0 || benchmarkRunId != null) {
                        throw new IllegalArgumentException(
                                "COUPON 범위에는 양수 couponId만 지정해야 합니다.");
                    }
                }
                case BENCHMARK_RUN -> {
                    if (benchmarkRunId == null || benchmarkRunId <= 0 || couponId != null) {
                        throw new IllegalArgumentException(
                                "BENCHMARK_RUN 범위에는 양수 benchmarkRunId만 지정해야 합니다.");
                    }
                }
            }
        }
    }

    /** 관리 지표가 표현하는 논리 범위입니다. */
    public enum MetricsScopeType { GLOBAL, COUPON, BENCHMARK_RUN }

    /**
     * LIVE 또는 FINAL 정합성 판정과 서로 독립적인 gap 관측값을 묶습니다.
     * 각 gap은 다른 원천의 실패에 영향받지 않고 자체 상태를 가져야 합니다.
     *
     * <p>Mapper는 {@code ConsistencyEvaluation.gaps()}의 네 항목을 다음과 같이 펼칩니다.
     * {@code LUA_GAP -> luaGap}, {@code ACTIVE_DB_GAP -> activeDbGap},
     * {@code DB_COUNTER_GAP -> dbCounterGap}, {@code PERSIST_GAP -> persistGap}입니다.
     * {@code overIssued}는 Evaluation의 동명 필드에서 별도로 전달합니다. 현재는 Calculator나
     * Mapper를 호출하지 않고 HTTP 필드 계약만 고정합니다.</p>
     *
     * @param phase LIVE 또는 FINAL 판정 단계
     * @param verdict FINAL 단계의 PASS/FAIL 판정; LIVE이면 null
     * @param severity 정합성 위험 심각도
     * @param overIssued 허용 수량 대비 초과 발급 gap
     * @param luaGap Lua/Redis 처리 결과 gap
     * @param activeDbGap 활성 상태 DB 집계 gap
     * @param dbCounterGap DB counter 집계 gap
     * @param persistGap 실시간 처리와 영속화 결과 간 gap
     */
    public record ConsistencyResponse(
            ConsistencyPhase phase,
            Verdict verdict,
            Severity severity,
            ObservedValue<Long> overIssued,
            ObservedValue<Long> luaGap,
            ObservedValue<Long> activeDbGap,
            ObservedValue<Long> dbCounterGap,
            ObservedValue<Long> persistGap
    ) {

        /**
         * LIVE 단계에는 최종 판정을 허용하지 않고 FINAL 단계에는 판정과 심각도를 모두 요구합니다.
         *
         * @throws NullPointerException phase가 null인 경우
         * @throws IllegalArgumentException 단계와 verdict 또는 severity 조합이 유효하지 않은 경우
         */
        public ConsistencyResponse {
            Objects.requireNonNull(phase, "phase");
            if (phase == ConsistencyPhase.LIVE && verdict != null) {
                throw new IllegalArgumentException("LIVE 단계에는 verdict를 지정할 수 없습니다.");
            }
            if (phase == ConsistencyPhase.FINAL && verdict == null) {
                throw new IllegalArgumentException("FINAL 단계에는 verdict가 필요합니다.");
            }
            if (phase == ConsistencyPhase.FINAL && severity == null) {
                throw new IllegalArgumentException("FINAL 단계에는 severity가 필요합니다.");
            }
        }
    }

    /**
     * 발급 시도·성공·대기 수락·정책 거절·시스템 실패 처리량을 독립 관측값으로 제공합니다.
     *
     * @param issueAttemptRps 초당 발급 시도 수
     * @param issueSuccessTps 초당 발급 성공 수
     * @param queueAcceptedRps 초당 대기열 수락 수
     * @param policyRejectRps 초당 정책 거절 수
     * @param systemFailureRps 초당 시스템 실패 수
     */
    public record TrafficMetrics(
            ObservedValue<Double> issueAttemptRps,
            ObservedValue<Double> issueSuccessTps,
            ObservedValue<Double> queueAcceptedRps,
            ObservedValue<Double> policyRejectRps,
            ObservedValue<Double> systemFailureRps
    ) { }

    /**
     * 성공·정책 거절·시스템 실패 경로의 지연 분포를 분리합니다.
     *
     * @param success 성공 경로 지연 분포
     * @param policyReject 정책 거절 경로 지연 분포
     * @param systemFailure 시스템 실패 경로 지연 분포
     */
    public record LatencyMetrics(
            ObservedValue<LatencyPercentiles> success,
            ObservedValue<LatencyPercentiles> policyReject,
            ObservedValue<LatencyPercentiles> systemFailure
    ) { }

    /**
     * Redis·HikariCP·Kafka 의존성 상태와 지연을 원천별로 분리합니다.
     *
     * @param redis Redis 지연·오류 관측값
     * @param hikari HikariCP 연결 풀 관측값
     * @param kafka Kafka 송수신 관측값
     */
    public record DependencyMetrics(
            ObservedValue<DependencySnapshot> redis,
            ObservedValue<DependencySnapshot> hikari,
            ObservedValue<DependencySnapshot> kafka
    ) { }

    /**
     * 한 처리 결과 그룹의 대표 지연 백분위입니다.
     *
     * @param p50Millis p50 지연 시간(ms)
     * @param p95Millis p95 지연 시간(ms)
     * @param p99Millis p99 지연 시간(ms)
     */
    public record LatencyPercentiles(double p50Millis, double p95Millis, double p99Millis) { }

    /**
     * 외부 의존성의 대표 지연과 오류 비율입니다.
     *
     * @param p95Millis p95 지연 시간(ms)
     * @param p99Millis p99 지연 시간(ms)
     * @param errorRate 0 이상 1 이하의 오류 비율
     */
    public record DependencySnapshot(double p95Millis, double p99Millis, double errorRate) { }

    /**
     * 실시간 처리 결과가 영속 저장소에 반영되는 속도와 지연을 나타냅니다.
     *
     * @param lagTotal 아직 영속화되지 않은 전체 건수
     * @param partitionMax 파티션별 지연 중 최댓값
     * @param arrivalRate 초당 유입 건수
     * @param consumeRate 초당 처리 건수
     * @param netDrainRate 초당 순감소 건수
     * @param drainEtaMillis 현재 속도 기준 예상 해소 시간(ms); 계산 불가 시 null
     */
    public record PersistenceLagSummary(long lagTotal, long partitionMax, double arrivalRate, double consumeRate,
                                        double netDrainRate, Long drainEtaMillis) { }

    /** 회로 차단기의 공개 상태입니다. */
    public enum CircuitBreakerState { CLOSED, OPEN, HALF_OPEN }

    /**
     * 하나의 회로 차단기 상태를 노출합니다.
     *
     * @param name 회로 차단기 식별 이름
     * @param state 현재 상태
     * @param openedAt 마지막 OPEN 전환 시각; 열린 적이 없으면 null
     */
    public record CircuitBreakerSummary(String name, CircuitBreakerState state, Instant openedAt) { }
}
