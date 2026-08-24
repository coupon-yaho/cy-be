package com.kafkick.api.admin.observability.dto;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonValue;

import com.kafkick.api.admin.support.ObservedValue;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.consistency.Verdict;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;

/**
 * 특정 관측 범위와 집계 구간의 정합성·트래픽·지연·의존성 상태를 한 snapshot으로 반환하는 응답 초안입니다.
 *
 * <p><b>응답 전용입니다.</b> 이 타입으로 역직렬화하지 마십시오 — 키 enum 이 {@code @JsonValue} 로
 * 상수 이름과 다른 camelCase 를 내보내고 짝이 되는 {@code @JsonCreator} 가 없습니다.</p>
 *
 * <p>각 원천은 독립적으로 실패할 수 있으므로 수집 실패를 0으로 바꾸지 않고 {@link ObservedValue}의
 * 상태와 관측 시각을 유지합니다. 정합성 {@code phase}가 LIVE이면 {@code verdict}는 null이고, FINAL이면
 * PASS 또는 FAIL이어야 합니다. 범위·구간·단계·심각도와 세부 지표는 모두 명시적 enum·record 계약입니다.</p>
 *
 * @param meta 원천 상태와 무관하게 항상 채워지는 스냅샷 메타데이터
 * @param scope GLOBAL, COUPON, BENCHMARK_RUN 중 실제 관측 범위
 * @param snapshotAt 응답 지표의 기준 시각
 * @param window 집계 구간
 * @param consistency LIVE/FINAL 정합성 판정과 독립 gap
 * @param traffic 발급 경로별 처리량
 * @param latency 결과 분류별 지연 분포
 * @param dependencies Redis·HikariCP·Kafka 의존성 지표
 * @param persistence 비동기 영속화 지연 관측값
 * @param circuitBreakers 회로 차단기별 상태 목록
 * @param errors 실패 분류별 비율과 실패 원인 Top N
 */
public record AdminMetricsResponse(
        Meta meta,
        MetricsScope scope,
        Instant snapshotAt,
        MetricsWindow window,
        ConsistencyResponse consistency,
        TrafficMetrics traffic,
        LatencyMetrics latency,
        DependencyMetrics dependencies,
        ObservedValue<PersistenceLagSummary> persistence,
        List<CircuitBreakerSummary> circuitBreakers,
        ErrorMetrics errors
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
                Meta.of(Instant.EPOCH, window, 0L),
                new MetricsScope(MetricsScopeType.GLOBAL, null, null),
                Instant.EPOCH,
                window,
                new ConsistencyResponse(ConsistencyPhase.LIVE, null, Severity.NONE, null, null, null, null, null),
                new TrafficMetrics(pendingRate, pendingRate, pendingRate, pendingRate, pendingRate),
                new LatencyMetrics(null, null, null),
                new DependencyMetrics(null, null, null),
                new ObservedValue<>(null, SourceStatus.PENDING, null),
                List.of(),
                ErrorMetrics.draft()
        );
    }


    /**
     * 스냅샷 자체에 대한 사실입니다. 원천이 실패해도 이 값들은 항상 알 수 있으므로
     * {@link ObservedValue}로 감싸지 않습니다 — 감싸면 화면이 여기서도 상태 분기를 타야 합니다.
     *
     * @param schemaVersion 화면 계약이 리터럴 1로 고정한 응답 스키마 판
     * @param snapshotAt 응답 지표의 기준 시각
     * @param windowStart 집계 창의 시작 시각
     * @param windowEnd 집계 창의 끝 시각
     * @param collectionDurationMs 조립에 실제로 걸린 시간(ms). 응답 예산에 얼마나 근접했는지가
     *        이 값으로만 보이므로 상수로 채우지 않습니다
     * @param sources 원천별 상태 자리. 상태는 값마다 붙는 {@code ObservedValue.state}가 정본이라
     *        같은 사실을 두 곳에 두지 않기 위해 비워 둡니다
     */
    public record Meta(
            int schemaVersion,
            Instant snapshotAt,
            Instant windowStart,
            Instant windowEnd,
            long collectionDurationMs,
            Map<String, SourceStatus> sources
    ) {

        /** 화면 타입이 {@code number}가 아니라 리터럴 {@code 1}입니다. */
        public static final int SCHEMA_VERSION = 1;

        /**
         * 기준 시각과 집계 창으로 메타데이터를 만듭니다.
         *
         * @param snapshotAt 응답 지표의 기준 시각
         * @param window 집계 창
         * @param collectionDurationMs 조립에 걸린 시간(ms)
         * @return 창 경계가 계산된 메타데이터
         */
        public static Meta of(Instant snapshotAt, MetricsWindow window, long collectionDurationMs) {
            Objects.requireNonNull(snapshotAt, "snapshotAt");
            Objects.requireNonNull(window, "window");
            return new Meta(
                    SCHEMA_VERSION,
                    snapshotAt,
                    snapshotAt.minus(window.duration()),
                    snapshotAt,
                    collectionDurationMs,
                    // null 이면 default-property-inclusion: non_null 이 키를 지워 화면이 undefined 를 읽는다.
                    Map.of());
        }
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

    /**
     * 발급 시도 대비 실패 비율과 실패 사유별 발생률입니다.
     *
     * <p><b>분모는 {@code issueAttemptRps} 하나로 고정합니다.</b> 패널마다 분모를 고르면 같은
     * 화면의 두 숫자가 다른 모집단을 가리킵니다. 어느 값을 분모로 썼는지는 화면이 문자열을
     * 박지 않도록 {@code denominator} 로 함께 내려보냅니다.</p>
     *
     * <p><b>정책 거절과 클라이언트 요청 오류는 분자에서 뺍니다.</b> 403·409 는 설계된 동작이고
     * 400·422·429 는 요청 계약 위반이라 서버 실패가 아닙니다. 분자에 넣으면 재고 소진 구간에서
     * 실패율이 100% 에 붙어 경보가 아무 의미도 갖지 못합니다. 다만 표에서 지우지는 않습니다 —
     * 안 보이면 그 트래픽이 어디로 갔는지 아무도 설명하지 못합니다. {@code excludedFromNumerator}
     * 가 그 구분을 싣습니다.</p>
     *
     * <p><b>{@code clientObservedFailure} 키는 없습니다.</b> 브라우저·부하 생성기 쪽 사건이라
     * 서버가 볼 수 있는 원천이 아예 없습니다. 0 으로 실으면 "클라이언트 실패 없음" 이라는 거짓
     * 신호가 되므로 키 자체를 내보내지 않습니다.</p>
     *
     * @param denominator 비율의 분모로 쓴 처리량 값의 키
     * @param classes 실패 분류별 비율. 분자 제외 여부를 값마다 함께 싣습니다
     * @param topReasons 실패 사유별 발생률. <b>서버는 상위 N 개로 자르지 않습니다</b> — 몇 줄까지
     *        보여줄지는 화면이 정합니다. "실패가 없어 빈 표"(VALID + 빈 목록)와 "아직 못 물어봐서"
     *        빈 표"(PENDING·UNAVAILABLE)는 운영자가 취할 행동이 정반대라 목록을 상태로 감쌉니다
     */
    public record ErrorMetrics(
            TrafficKey denominator,
            List<ErrorClass> classes,
            ObservedValue<List<TopReason>> topReasons
    ) {

        /**
         * 분모 키와 분류 목록을 고정 검증합니다.
         *
         * @throws NullPointerException 인자가 null 인 경우
         */
        public ErrorMetrics {
            Objects.requireNonNull(denominator, "denominator");
            classes = List.copyOf(Objects.requireNonNull(classes, "classes"));
            Objects.requireNonNull(topReasons, "topReasons");
        }

        /** @return 분류 자리는 있으나 아직 아무것도 수집되지 않은 초안 */
        public static ErrorMetrics draft() {
            return new ErrorMetrics(
                    TrafficKey.ISSUE_ATTEMPT_RPS,
                    List.of(),
                    new ObservedValue<>(null, SourceStatus.PENDING, null));
        }
    }

    /**
     * 실패 비율 한 줄입니다.
     *
     * <p>{@code label} 과 {@code definition} 을 서버가 싣습니다 — 판정식이 바뀌었는데 화면 문구가
     * 그대로면 숫자가 조용히 다른 것을 뜻하게 됩니다. 반대로 "지금 이 값을 믿지 마라" 같은 한시적
     * 경고는 사실이 아니라 상황이므로 여기 섞지 않습니다.</p>
     *
     * @param key 결과 분류 키
     * @param label 화면에 그대로 쓰는 짧은 이름
     * @param definition 이 분류를 가르는 판정식
     * @param excludedFromNumerator 실패율 분자에서 제외되는 분류이면 true
     * @param rate 분모 대비 백분율(0~100). 분모가 0 이면 비율이 정의되지 않아 N_A 입니다
     */
    public record ErrorClass(
            ErrorClassKey key,
            String label,
            String definition,
            boolean excludedFromNumerator,
            ObservedValue<Double> rate
    ) {

        /**
         * 분류 한 줄의 필수 항목을 검증합니다.
         *
         * @throws NullPointerException 인자가 null 인 경우
         */
        public ErrorClass {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(label, "label");
            Objects.requireNonNull(definition, "definition");
            Objects.requireNonNull(rate, "rate");
        }
    }

    /**
     * 실패 분류 키입니다. {@code ResultClassifier.ResultClass} 의 <b>성공이 아닌 네 분류</b>와
     * 일대일로 대응합니다 — 대응이 깨지면 화면의 분류 표가 원천에 없는 것을 그리거나 원천에 있는
     * 것을 빠뜨립니다. 그 대응은 계약 테스트가 잡습니다.
     */
    public enum ErrorClassKey {

        DEPENDENCY_FAILURE("dependencyFailure"),
        APPLICATION_FAILURE("applicationFailure"),
        CLIENT_INVALID("clientInvalid"),
        POLICY_REJECT("policyReject");

        private final String jsonValue;

        ErrorClassKey(String jsonValue) {
            this.jsonValue = jsonValue;
        }

        /** @return 화면 계약이 쓰는 camelCase 키 */
        @JsonValue
        public String jsonValue() {
            return jsonValue;
        }
    }

    /**
     * 처리량 값의 키입니다. 비율의 분모가 무엇인지 화면에 알려주는 데 씁니다.
     *
     * <p>이름과 순서가 {@link TrafficMetrics} 의 항목과 정확히 같아야 합니다 — 어긋나면 화면이
     * 존재하지 않는 값을 분모라고 표시합니다. 계약 테스트가 둘을 잇습니다.</p>
     */
    public enum TrafficKey {

        ISSUE_ATTEMPT_RPS("issueAttemptRps"),
        ISSUE_SUCCESS_TPS("issueSuccessTps"),
        QUEUE_ACCEPTED_RPS("queueAcceptedRps"),
        POLICY_REJECT_RPS("policyRejectRps"),
        SYSTEM_FAILURE_RPS("systemFailureRps");

        private final String jsonValue;

        TrafficKey(String jsonValue) {
            this.jsonValue = jsonValue;
        }

        /** @return 화면 계약이 쓰는 camelCase 키 */
        @JsonValue
        public String jsonValue() {
            return jsonValue;
        }
    }

    /**
     * 실패 원인 한 줄입니다.
     *
     * <p><b>0 인 행을 서버가 거르지 않습니다.</b> 무엇이 0 인지 보이는 것과 사라지는 것 중
     * 무엇이 나은지는 패널이 정할 문제입니다. 같은 이유로 상위 N 개로 자르지도 않습니다 —
     * 사유는 {@code ReasonCode} 로 이미 저카디널리티라 자를 만큼 길어지지 않습니다.</p>
     *
     * <p>HTTP 상태는 싣지 않습니다. 원천({@code app_issuance_outcome_total})에 그 라벨이 없어
     * 서버가 지어내야 하고, 지어낸 값은 {@code ErrorCode} 가 상태를 바꾸는 순간 조용히
     * 어긋납니다.</p>
     *
     * <p><b>사유 코드만 대문자로 나갑니다.</b> {@code ErrorClassKey}·{@code TrafficKey} 는 화면이
     * 정한 키라 camelCase 지만, 이것은 키가 아니라 원천의 {@code outcome} 라벨 값을 그대로 옮긴
     * 것입니다. 표기를 맞추겠다고 여기서 바꾸면 화면에 찍힌 문자열로 Prometheus 를 되짚을 수
     * 없게 됩니다.</p>
     *
     * @param reasonCode 저카디널리티 사유 코드. 원천 라벨과 같은 대문자 표기다
     * @param rps 초당 발생 건수
     */
    public record TopReason(ReasonCode reasonCode, double rps) {

        /**
         * 사유 코드를 검증합니다.
         *
         * @throws NullPointerException reasonCode 가 null 인 경우
         */
        public TopReason {
            Objects.requireNonNull(reasonCode, "reasonCode");
        }
    }
}
