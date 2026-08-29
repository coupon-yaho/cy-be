package com.kafkick.api.admin.observability;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalDouble;

import com.kafkick.api.observation.MeterNames;
import com.kafkick.core.observation.DomainMeterNames;

/**
 * 지표를 인스턴스 여러 대에서 하나로 줄이는 규칙입니다. <b>이 표가 계약입니다.</b>
 *
 * <p>규칙이 패널마다 달라지면 화면의 숫자가 무슨 의미인지 아무도 설명하지 못합니다. 그래서
 * 패널이 집계를 고르지 못하게 지표 이름으로만 규칙을 찾고, 표에 없는 지표는 예외로 막습니다.</p>
 *
 * <pre>
 * 지표                    집계     근거
 * ────────────────────────────────────────────────────────────
 * 응답시간 p50/p95/p99    max     인스턴스별 값이라 병합 불가(DEC-02).
 *                                 화면 라벨에 '인스턴스 최댓값' 필수.
 * 응답 결과 rate          sum     전체 처리량
 * 발급 결과 사유 rate     sum     전체 처리량
 * in-flight               sum     전역 동시 처리 수
 * DB 풀 active/pending    sum     절대 개수
 * DB 풀 사용률(%)         max     sum 하면 200% 가 나온다
 * tomcat busy/정원        sum     절대 스레드 수
 * tomcat 사용률(%)        max     위 둘과 같은 이유
 * CPU · heap              max     가장 위험한 인스턴스
 * heap 사용률(%)          max     영역 합산은 인스턴스 안에서 먼저 한다
 * up                      sum     살아 있는 인스턴스 수
 * 대기열 길이             단일     batch 한 곳에서만 나온다
 * Kafka lag               sum     파티션 합
 * 정합성 gap              단일     batch 한 곳에서만 나온다
 * </pre>
 *
 * <p><b>모든 지연·처리량 지표는 uri 그룹으로 먼저 쪼갠 뒤 집계합니다.</b> 그룹을 합친 전체 p99 는
 * 만들지 않습니다 — 1 초 폴링에 지배당해 아무 의미가 없습니다. 이 enum 은 그룹을 나눈
 * <b>뒤에</b> 같은 그룹 안의 인스턴스들을 줄이는 단계만 담당합니다.</p>
 */
public enum MetricAggregation {

    /** 절대 개수·처리량. 인스턴스 값을 더한다. */
    SUM,

    /**
     * 인스턴스별로 계산된 값이라 통계적으로 합칠 수 없는 지표. 최댓값을 쓴다(DEC-02).
     *
     * <p>DEC-02 는 최댓값 사용을 허용하되 <b>'인스턴스 최댓값' 표식</b>을 조건으로 단다. 응답
     * 계약({@code LatencyPercentiles.p99Millis})에는 그 표식을 실을 자리가 없어 지금은 화면이
     * 라벨로 붙여야 한다 — 계약에 자리를 만드는 것은 A 와 합의할 항목이다.
     */
    MAX,

    /**
     * 원천이 한 곳뿐인 지표. 표본이 둘 이상이면 배선이 잘못된 것이므로 조용히 고르지 않고 던진다.
     *
     * <p><b>호출자는 이 예외를 값 단위로 잡아야 한다.</b> 밖으로 올리면 scrape 대상을 늘린 순간
     * 응답 전체가 500 이 된다 — "원천 하나가 죽어도 화면 전체가 죽지 않는다" 는 계약을 깬다.
     */
    SINGLE;

    // ── Prometheus 이름. 미터 이름 상수에서 만들어 쓴다 — 문자열을 옮겨 적지 않는다 ──────
    public static final String HTTP_LATENCY_SECONDS = promName(MeterNames.HTTP_LATENCY) + "_seconds";
    public static final String HTTP_RESULT_TOTAL = promName(MeterNames.HTTP_RESULT) + "_total";
    public static final String HTTP_IN_FLIGHT = promName(MeterNames.IN_FLIGHT);

    /**
     * 발급 결과 사유별 Counter. HTTP 결과 분류({@link #HTTP_RESULT_TOTAL})와 <b>원천이 다릅니다</b> —
     * 저쪽은 응답 상태로 나눈 여섯 분류이고 이쪽은 업무 사유({@code ReasonCode})입니다. 실패
     * <b>비율</b>은 저쪽에서, 실패 <b>원인</b>은 이쪽에서 나옵니다.
     */
    public static final String ISSUANCE_OUTCOME_TOTAL = promName(MeterNames.ISSUANCE_OUTCOME) + "_total";
    public static final String HIKARI_ACTIVE = promName(MeterNames.HIKARI_ACTIVE);
    public static final String HIKARI_PENDING = promName(MeterNames.HIKARI_PENDING);
    public static final String CPU_USAGE = promName(MeterNames.CPU_USAGE);
    public static final String JVM_MEMORY_USED = promName(MeterNames.JVM_MEMORY_USED) + "_bytes";
    public static final String HIKARI_MAX = promName(MeterNames.HIKARI_MAX);
    public static final String JVM_MEMORY_MAX = promName(MeterNames.JVM_MEMORY_MAX) + "_bytes";

    /**
     * Tomcat worker 스레드. <b>Micrometer 가 이름 뒤에 base unit(threads)을 붙인다</b> — 실측한
     * 이름이 {@code tomcat_threads_busy_threads} 여서 {@link #promName(String)} 만으로는 나오지
     * 않는다. 접미사를 빼면 예외 없이 표본이 0 개고 두 행이 영원히 빈다.
     *
     * <p>{@code server.tomcat.mbeanregistry.enabled=true} 일 때만 등록된다. 그 스위치는
     * {@code observation.yml} 이 쥐고 있고, 둘을 잇는 것은 {@code TomcatMeterSwitchContractTest} 다.
     */
    public static final String TOMCAT_BUSY = promName(MeterNames.TOMCAT_BUSY) + "_threads";
    public static final String TOMCAT_MAX = promName(MeterNames.TOMCAT_MAX) + "_threads";

    /**
     * 스크레이프 대상이 살아 있는지. 미터가 아니라 Prometheus 가 직접 만드는 시계열이라 미터
     * 이름 상수가 없다. 값을 더하면 살아 있는 인스턴스 수가 된다 — 죽은 인스턴스의 마지막
     * 값이 남아 SUM 을 부풀리는 것을 화면이 "활성 n대" 로 드러내는 근거다.
     */
    public static final String UP = "up";

    public static final String QUEUE_LENGTH = promName(DomainMeterNames.QUEUE_LENGTH);
    public static final String QUEUE_LENGTH_STATE = promName(DomainMeterNames.QUEUE_LENGTH_STATE);
    public static final String OBSERVED_COUPON_ID = promName(DomainMeterNames.OBSERVED_COUPON_ID);

    public static final String CONSISTENCY_GAP = promName(DomainMeterNames.CONSISTENCY_GAP);
    public static final String CONSISTENCY_GAP_STATE = promName(DomainMeterNames.CONSISTENCY_GAP_STATE);
    public static final String OVER_ISSUED = promName(DomainMeterNames.OVER_ISSUED);
    public static final String OVER_ISSUED_STATE = promName(DomainMeterNames.OVER_ISSUED_STATE);
    public static final String CONSISTENCY_SEVERITY = promName(DomainMeterNames.CONSISTENCY_SEVERITY);
    public static final String CONSISTENCY_SEVERITY_STATE =
            promName(DomainMeterNames.CONSISTENCY_SEVERITY_STATE);
    public static final String CONSISTENCY_COUPON_ID = promName(DomainMeterNames.CONSISTENCY_COUPON_ID);

    /**
     * 수집 경로별 마지막 성공 시각. <b>정합성 값들의 진짜 관측 시각</b>이다 — instant query 가
     * 표본에 붙여 주는 시각은 질의 평가 시각이라 언제나 "지금" 이고, 그것으로는 STALE 을 낼 수 없다.
     */
    public static final String COLLECT_LAST_SUCCESS_EPOCH =
            promName(DomainMeterNames.COLLECT_LAST_SUCCESS_EPOCH);


    /**
     * HTTP 미터가 마지막으로 스크레이프된 뒤 흐른 시간(초). 미터가 아니라 {@code timestamp()} 로
     * 계산하는 파생값이라 이름이 Prometheus 에 존재하지 않는다. 인스턴스 하나가 스크레이프에서
     * 빠졌을 때 그것이 드러나야 하므로 <b>가장 오래된</b> 나이를 쓴다.
     */
    public static final String HTTP_FRESHNESS_AGE_SECONDS = "app.http.freshness.age.seconds";

    /**
     * DB 풀 사용률. 미터가 아니라 {@link #HIKARI_ACTIVE} 를 풀 크기로 나눈 파생값이라 이름이
     * Prometheus 에 존재하지 않는다. 규칙표에서 빠지면 패널이 sum 을 골라 200% 를 그린다.
     */
    public static final String HIKARI_POOL_UTILIZATION = "hikaricp.connections.utilization";

    /**
     * Tomcat worker 사용률. {@link #TOMCAT_BUSY} 를 {@link #TOMCAT_MAX} 로 나눈 파생값이라 이름이
     * Prometheus 에 존재하지 않는다. 나눗셈은 <b>인스턴스 안에서</b> 먼저 하고 그 결과들을 이
     * 규칙으로 줄인다 — 인스턴스를 먼저 합치면 정원이 다른 대가 섞여 사용률이 희석된다.
     */
    public static final String TOMCAT_THREAD_UTILIZATION = "tomcat.threads.utilization";

    /**
     * 힙 사용률. {@link #JVM_MEMORY_USED} 의 {@code area="heap"} 영역들을 인스턴스 안에서 더해
     * {@link #JVM_MEMORY_MAX} 로 나눈 파생값이다.
     *
     * <p><b>생표본에 MAX 를 그대로 걸면 안 된다.</b> 이 미터는 {@code area}·{@code id} 로 여덟
     * 갈래라 최댓값이 heap 이 아니라 Metaspace(nonheap)로 잡힌다(실측 96MB).
     */
    public static final String JVM_HEAP_UTILIZATION = "jvm.memory.heap.utilization";

    /**
     * 인스턴스 하나가 쥐고 있는 in-flight 최댓값. {@link #HTTP_IN_FLIGHT} 와 <b>같은 미터의 다른
     * 축약</b>이라 이름이 따로 필요하다 — 전역 합만 보면 한 대에 쏠린 것을 못 본다.
     */
    public static final String HTTP_IN_FLIGHT_INSTANCE_MAX = "app.http.inflight.instance.max";

    // TODO(OBS-17): Kafka persist lag 미터가 열리면 SUM(파티션 합)으로 이 표에 추가한다.
    //   지금 이름을 지어 넣으면 등록하는 쪽과 어긋나도 아무도 모른다 — 미터가 생긴 뒤에 넣는다.

    private static final Map<String, MetricAggregation> RULES = rules();

    /**
     * 지표 이름에 확정된 집계 규칙을 찾습니다.
     *
     * @param metricName Prometheus 지표 이름 또는 위 파생 키
     * @return 확정된 집계 규칙
     * @throws IllegalArgumentException 규칙표에 없는 지표인 경우
     */
    public static MetricAggregation of(String metricName) {
        MetricAggregation rule = RULES.get(metricName);
        if (rule == null) {
            throw new IllegalArgumentException(
                    "집계 규칙이 정해지지 않은 지표입니다. 규칙표에 먼저 추가하세요: " + metricName);
        }
        return rule;
    }

    /** @return 규칙표 전체. 계약 테스트가 표의 크기와 내용을 함께 본다. */
    public static Map<String, MetricAggregation> rulesView() {
        return RULES;
    }

    /**
     * 같은 그룹 안의 인스턴스 표본들을 규칙대로 하나로 줄입니다.
     *
     * <p>NaN 표본은 값이 아니라 "이유는 상태 미터가 낸다" 는 표시이므로 셈에서 뺍니다. 숫자
     * 표본이 하나도 없으면 0 이 아니라 빈 값입니다 — 0 을 내보내면 화면이 거짓을 그립니다.</p>
     *
     * @param samples 같은 지표·같은 라벨 그룹의 표본들
     * @return 집계값; 숫자 표본이 없으면 빈 값
     * @throws IllegalStateException SINGLE 규칙인데 숫자 표본이 둘 이상인 경우
     */
    public OptionalDouble reduce(Collection<PromSample> samples) {
        List<PromSample> numeric = samples.stream().filter(PromSample::hasNumericValue).toList();
        if (numeric.isEmpty()) {
            return OptionalDouble.empty();
        }
        return switch (this) {
            case SUM -> OptionalDouble.of(numeric.stream().mapToDouble(PromSample::value).sum());
            case MAX -> numeric.stream().mapToDouble(PromSample::value).max();
            case SINGLE -> {
                if (numeric.size() > 1) {
                    throw new IllegalStateException(
                            "원천이 하나여야 하는 지표에 표본이 " + numeric.size() + "개입니다.");
                }
                yield OptionalDouble.of(numeric.get(0).value());
            }
        };
    }

    /**
     * Micrometer 미터 이름을 Prometheus 지표 이름으로 바꿉니다.
     *
     * @param meterName 점 표기 미터 이름
     * @return 밑줄 표기 지표 이름
     */
    public static String promName(String meterName) {
        return meterName.replace('.', '_').replace('-', '_').toLowerCase(Locale.ROOT);
    }

    private static Map<String, MetricAggregation> rules() {
        Map<String, MetricAggregation> table = new LinkedHashMap<>();
        table.put(HTTP_LATENCY_SECONDS, MAX);
        table.put(HTTP_RESULT_TOTAL, SUM);
        table.put(HTTP_IN_FLIGHT, SUM);
        table.put(HTTP_IN_FLIGHT_INSTANCE_MAX, MAX);
        table.put(ISSUANCE_OUTCOME_TOTAL, SUM);
        table.put(HIKARI_ACTIVE, SUM);
        table.put(HIKARI_PENDING, SUM);
        table.put(HIKARI_MAX, SUM);
        table.put(HIKARI_POOL_UTILIZATION, MAX);
        table.put(TOMCAT_BUSY, SUM);
        table.put(TOMCAT_MAX, SUM);
        table.put(TOMCAT_THREAD_UTILIZATION, MAX);
        table.put(JVM_MEMORY_MAX, MAX);
        table.put(JVM_HEAP_UTILIZATION, MAX);
        table.put(UP, SUM);
        table.put(QUEUE_LENGTH, SINGLE);
        table.put(QUEUE_LENGTH_STATE, SINGLE);
        table.put(OBSERVED_COUPON_ID, SINGLE);
        table.put(CPU_USAGE, MAX);
        table.put(JVM_MEMORY_USED, MAX);
        table.put(CONSISTENCY_GAP, SINGLE);
        table.put(CONSISTENCY_GAP_STATE, SINGLE);
        table.put(OVER_ISSUED, SINGLE);
        table.put(OVER_ISSUED_STATE, SINGLE);
        table.put(CONSISTENCY_SEVERITY, SINGLE);
        table.put(CONSISTENCY_SEVERITY_STATE, SINGLE);
        table.put(CONSISTENCY_COUPON_ID, SINGLE);
        table.put(COLLECT_LAST_SUCCESS_EPOCH, SINGLE);
        table.put(HTTP_FRESHNESS_AGE_SECONDS, MAX);
        return Map.copyOf(table);
    }
}
