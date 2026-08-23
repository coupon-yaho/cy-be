package com.kafkick.api.admin.observability;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tools.jackson.databind.JsonNode;

import com.kafkick.core.benchmark.RunTimeseriesArchiver;
import com.kafkick.core.benchmark.RunTimeseriesArchiver.Metric;
import com.kafkick.core.benchmark.RunTimeseriesArchiver.Sample;
import com.kafkick.core.benchmark.RunTimeseriesArchiver.State;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.observation.SourceStatusCode;

/**
 * Prometheus instant query와 완료 회차 range query를 호출해 표본 목록으로 바꿉니다.
 *
 * <p>D2 조회는 {@code /api/v1/query} 한 시점 스냅샷이고 화면이 1초 폴링으로 누적합니다.
 * archive는 회차가 끝난 뒤 {@code /api/v1/query_range}로 과거 표본 자체의 시각을 읽습니다.
 * 두 경로의 응답 크기와 시간 예산이 다르므로 설정에서 서로 다른 RestClient 인스턴스로 배선합니다.</p>
 *
 * <p>실패는 예외로 나갑니다. HTTP 500 으로 번역하지 말고 조립하는 쪽이 잡아 해당 값만
 * {@code UNAVAILABLE} 로 내려보냅니다.</p>
 */
public class PromQueryClient implements PromQuery, PromTimeQuery, RunTimeseriesArchiver.RangeSource {

    private static final Logger log = LoggerFactory.getLogger(PromQueryClient.class);

    private static final String QUERY_PATH = "/api/v1/query";
    private static final String RANGE_QUERY_PATH = "/api/v1/query_range";
    private static final String NAME_LABEL = "__name__";
    private static final String QUERY_PARAM = "query";

    /**
     * 표본 시각의 허용 범위(epoch 초). Prometheus 표본 시각은 스크레이프 시각이라 epoch 이전일 수
     * 없고, 먼 미래 값은 원천이 망가진 것이지 관측이 아니다.
     *
     * <p>범위를 안 보면 조용히 통과한다 — {@code Math.round} 는 범위를 넘길 때 예외 대신
     * {@code Long.MAX_VALUE} 로 포화되고 {@code Instant.ofEpochMilli} 도 그 값을 받아
     * {@code +292278994-08-17} 같은 시각을 만든다(실측). 그 표본이 가장 큰 값이라
     * {@code snapshotAt} 으로 뽑히면 화면이 그것을 현재 시각으로 읽는다.
     */
    private static final double MIN_EPOCH_SECONDS = 0d;

    /** 2100-01-01T00:00:00Z. 이 프로젝트 수명 밖의 시각은 관측이 아니라 고장이다. */
    private static final double MAX_EPOCH_SECONDS = 4102444800d;

    /** Prometheus JSON 의 무한대 표기. 자바는 "Infinity" 만 받아 그대로 파싱하면 예외가 난다. */
    private static final String POSITIVE_INFINITY = "+Inf";
    private static final String NEGATIVE_INFINITY = "-Inf";

    private final RestClient restClient;

    public PromQueryClient(RestClient restClient) {
        this.restClient = Objects.requireNonNull(restClient, "restClient");
    }

    /**
     * instant query 하나를 실행합니다.
     *
     * @param promQl 실행할 PromQL
     * @return 벡터 결과의 표본 목록; 일치하는 시계열이 없으면 빈 목록
     * @throws PromQueryException 호출이 실패했거나 Prometheus 가 success 가 아닌 상태를 돌려준 경우
     */
    @Override
    public List<PromSample> query(String promQl) {
        JsonNode body;
        try {
            body = restClient.get()
                    // ⚠️ PromQL 을 queryParam 값으로 직접 넣지 말 것. 셀렉터의 중괄호를 RestClient 가
                    //    URI 템플릿 변수로 읽어 "Not enough variable values available to expand" 로
                    //    죽는다. 질의 넷 중 셋이 중괄호를 쓰므로 운영에서 그대로 터진다.
                    //    값을 변수로 넘겨야 템플릿 확장 대상에서 빠지고 인코딩도 함께 된다.
                    .uri(uriBuilder -> uriBuilder.path(QUERY_PATH)
                            .queryParam(QUERY_PARAM, "{" + QUERY_PARAM + "}")
                            .build(promQl))
                    .retrieve()
                    .body(JsonNode.class);
        } catch (RestClientException failure) {
            throw new PromQueryException("Prometheus 질의에 실패했습니다: " + promQl, failure);
        }
        return parse(body, promQl);
    }

    /**
     * instant query를 명시한 평가 시각에 실행합니다.
     *
     * <p>기존 {@link #query(String)} 사용자의 서버 현재 시각 동작은 유지하고, 재현 가능한 snapshot이
     * 필요한 Overview 호출만 Prometheus HTTP API의 {@code time} 파라미터를 사용합니다.</p>
     *
     * @param promQl 실행할 PromQL
     * @param evaluationAt Prometheus가 expression을 평가할 시각
     * @return 벡터 결과의 표본 목록; 일치하는 시계열이 없으면 빈 목록
     * @throws PromQueryException 호출이 실패했거나 Prometheus가 success가 아닌 상태를 돌려준 경우
     */
    @Override
    public List<PromSample> query(String promQl, Instant evaluationAt) {
        Objects.requireNonNull(evaluationAt, "evaluationAt");
        JsonNode body;
        try {
            body = restClient.get()
                    .uri(uriBuilder -> uriBuilder.path(QUERY_PATH)
                            .queryParam(QUERY_PARAM, "{" + QUERY_PARAM + "}")
                            .queryParam("time", "{time}")
                            .build(promQl, evaluationAt.toString()))
                    .retrieve()
                    .body(JsonNode.class);
        } catch (RestClientException failure) {
            throw new PromQueryException("Prometheus 시각 고정 질의에 실패했습니다: " + promQl, failure);
        }
        return parseStrict(body, promQl);
    }

    @Override
    public List<Sample> queryRange(Metric metric, Instant start, Instant end, int stepSeconds) {
        String promQl = switch (metric) {
            case STOCK_REMAINING -> "{__name__=~\"app_coupon_stock_remaining|app_coupon_stock_remaining_state\"}";
            // publishPercentiles만 있으므로 bucket 합산은 할 수 없다. topk가 최댓값의 instance 라벨을 보존한다.
            case LATENCY_P99 -> "topk(1, app_http_latency_seconds{quantile=\"0.99\"}) * 1000";
            case DB_POOL_USAGE -> "sum(hikaricp_connections_active{job=\"api\",pool!=\"obs-pool\"})"
                    + " / sum(hikaricp_connections_max{job=\"api\",pool!=\"obs-pool\"})";
        };
        JsonNode body;
        try {
            body = restClient.get()
                    .uri(uriBuilder -> uriBuilder.path(RANGE_QUERY_PATH)
                            .queryParam(QUERY_PARAM, "{" + QUERY_PARAM + "}")
                            .queryParam("start", "{start}")
                            .queryParam("end", "{end}")
                            .queryParam("step", "{step}")
                            .build(promQl, epochSeconds(start), epochSeconds(end), stepSeconds))
                    .retrieve().body(JsonNode.class);
        } catch (RestClientException failure) {
            throw new PromQueryException("Prometheus range 질의에 실패했습니다: " + promQl, failure);
        }
        return parseRange(body, metric, start, promQl);
    }

    private static String epochSeconds(Instant instant) {
        return BigDecimal.valueOf(instant.getEpochSecond())
                .add(BigDecimal.valueOf(instant.getNano(), 9))
                .stripTrailingZeros()
                .toPlainString();
    }

    private static List<Sample> parseRange(JsonNode body, Metric metric, Instant start, String promQl) {
        if (body == null || !"success".equals(body.path("status").asString(""))) {
            throw new PromQueryException("Prometheus range 응답이 실패했습니다: " + promQl);
        }
        JsonNode data = body.path("data");
        if (!"matrix".equals(data.path("resultType").asString(""))) {
            throw new PromQueryException("matrix 가 아닌 range 결과입니다: " + promQl);
        }

        TreeMap<Instant, MutableRangeSample> byTime = new TreeMap<>();
        for (JsonNode series : data.path("result")) {
            JsonNode labels = series.path("metric");
            String name = labels.path(NAME_LABEL).asString("");
            String instance = labels.path("instance").asString(null);
            Map<String, String> seriesLabels = labelsWithoutName(labels);
            boolean stateSeries = name.endsWith("_state");
            for (JsonNode pair : series.path("values")) {
                if (!pair.isArray() || pair.size() != 2) {
                    continue;
                }
                Instant observedAt = instantOfEpochSeconds(pair.get(0));
                double value = parseValue(pair.get(1).asString(""));
                MutableRangeSample sample = byTime.computeIfAbsent(observedAt, ignored -> new MutableRangeSample());
                if (stateSeries) {
                    if (sample.stateSeen) {
                        throw new PromQueryException("같은 시각의 상태 시리즈가 중복됐습니다: " + metric
                                + " at " + observedAt);
                    }
                    sample.stateSeen = true;
                    sample.stateLabels = seriesLabels;
                    sample.state = archiveState(value);
                } else {
                    if (sample.valueSeen) {
                        throw new PromQueryException("같은 시각의 값 시리즈가 중복됐습니다: " + metric
                                + " at " + observedAt);
                    }
                    sample.valueSeen = true;
                    sample.valueLabels = seriesLabels;
                    if (Double.isFinite(value)) {
                        sample.value = value;
                        sample.instance = instance;
                    }
                }
                if (sample.valueLabels != null && sample.stateLabels != null
                        && !sample.valueLabels.equals(sample.stateLabels)) {
                    throw new PromQueryException("값과 상태 시리즈의 라벨이 다릅니다: " + metric
                            + " at " + observedAt);
                }
            }
        }

        List<Sample> samples = new ArrayList<>();
        for (Map.Entry<Instant, MutableRangeSample> entry : byTime.entrySet()) {
            MutableRangeSample raw = entry.getValue();
            State state = raw.state == null ? (raw.value == null ? State.UNAVAILABLE : State.VALID) : raw.state;
            Double value = state == State.VALID ? raw.value : null;
            // 값 시계열이 비었는데 상태만 VALID인 표본을 정상 0으로 만들지 않는다.
            if (value == null && state == State.VALID) {
                state = State.UNAVAILABLE;
            }
            samples.add(new Sample(metric, entry.getKey().getEpochSecond() - start.getEpochSecond(),
                    entry.getKey(), value, state, raw.instance));
        }
        return List.copyOf(samples);
    }

    private static Map<String, String> labelsWithoutName(JsonNode metric) {
        Map<String, String> labels = new TreeMap<>();
        for (Map.Entry<String, JsonNode> field : metric.properties()) {
            if (!NAME_LABEL.equals(field.getKey())) {
                labels.put(field.getKey(), field.getValue().asString(""));
            }
        }
        return Map.copyOf(labels);
    }

    private static Instant instantOfEpochSeconds(JsonNode timestamp) {
        epochSecondsOf(timestamp);
        BigDecimal decimal = timestamp.decimalValue();
        BigDecimal wholeSeconds = decimal.setScale(0, RoundingMode.FLOOR);
        long seconds = wholeSeconds.longValueExact();
        int nanos = decimal.subtract(wholeSeconds).movePointRight(9)
                .setScale(0, RoundingMode.HALF_UP).intValueExact();
        if (nanos == 1_000_000_000) {
            return Instant.ofEpochSecond(Math.addExact(seconds, 1));
        }
        return Instant.ofEpochSecond(seconds, nanos);
    }

    private static State archiveState(double code) {
        if (!Double.isFinite(code) || code != Math.rint(code)) {
            return State.UNAVAILABLE;
        }
        try {
            SourceStatus status = SourceStatusCode.statusOf((int) code);
            return switch (status) {
                case N_A -> State.N_A;
                case VALID, WARMING_UP, STALE, NO_TRAFFIC -> State.VALID;
                case PENDING, UNAVAILABLE -> State.UNAVAILABLE;
            };
        } catch (IllegalArgumentException unknown) {
            return State.UNAVAILABLE;
        }
    }

    private static final class MutableRangeSample {
        private Double value;
        private State state;
        private String instance;
        private boolean valueSeen;
        private boolean stateSeen;
        private Map<String, String> valueLabels;
        private Map<String, String> stateLabels;
    }

    /** 기존 no-time 사용자를 위해 손상 멤버만 건너뛰는 tolerant vector parser입니다. */
    private static List<PromSample> parse(JsonNode body, String promQl) {
        if (body == null) {
            throw new PromQueryException("Prometheus 응답이 비어 있습니다: " + promQl);
        }
        String status = body.path("status").asString("");
        if (!"success".equals(status)) {
            throw new PromQueryException(
                    "Prometheus 가 실패를 반환했습니다: " + body.path("error").asString(status));
        }
        JsonNode data = body.path("data");
        String resultType = data.path("resultType").asString("");
        // scalar·matrix 를 표본 목록으로 바꾸면 라벨이 사라져 집계 규칙을 적용할 수 없다.
        if (!"vector".equals(resultType)) {
            throw new PromQueryException("vector 가 아닌 결과 타입입니다: " + resultType);
        }

        List<PromSample> samples = new ArrayList<>();
        for (JsonNode entry : data.path("result")) {
            // 표본 하나가 이상해도 질의 전체를 죽이지 않는다. 정합성 질의는 gap 4종·overIssued·
            // severity 를 한 번에 담으므로 시계열 하나 때문에 여섯 개가 같이 UNAVAILABLE 이 된다.
            try {
                samples.add(toSample(entry, promQl));
            } catch (PromQueryException malformed) {
                log.warn("해석할 수 없는 표본을 건너뜁니다: {} ({})", promQl, malformed.getMessage());
                log.debug("해석할 수 없는 표본 상세: {}", promQl, malformed);
            }
        }
        return List.copyOf(samples);
    }

    /** Overview timed 질의의 손상 provenance를 잃지 않도록 전체 vector를 엄격히 파싱합니다. */
    private static List<PromSample> parseStrict(JsonNode body, String promQl) {
        if (body == null || !body.isObject()) {
            throw new PromQueryException("Prometheus 응답이 비어 있거나 객체가 아닙니다: " + promQl);
        }
        String status = body.path("status").asString("");
        if (!"success".equals(status)) {
            throw new PromQueryException(
                    "Prometheus 가 실패를 반환했습니다: " + body.path("error").asString(status));
        }
        JsonNode data = body.path("data");
        if (!data.isObject()) {
            throw new PromQueryException("Prometheus data가 객체가 아닙니다: " + promQl);
        }
        String resultType = data.path("resultType").asString("");
        if (!"vector".equals(resultType)) {
            throw new PromQueryException("vector 가 아닌 결과 타입입니다: " + resultType);
        }
        JsonNode result = data.path("result");
        if (!result.isArray()) {
            throw new PromQueryException("vector result가 배열이 아닙니다: " + promQl);
        }
        List<PromSample> samples = new ArrayList<>();
        for (JsonNode entry : result) {
            validateStrictSampleShape(entry, promQl);
            samples.add(toSample(entry, promQl));
        }
        return List.copyOf(samples);
    }

    /** timed vector 멤버의 객체·라벨·값 프로토콜 형식을 하나라도 유실 없이 검증합니다. */
    private static void validateStrictSampleShape(JsonNode entry, String promQl) {
        JsonNode metric = entry.path("metric");
        JsonNode value = entry.path("value");
        if (!entry.isObject() || !metric.isObject() || !value.isArray() || value.size() != 2
                || !value.get(1).isString()) {
            throw new PromQueryException("vector 표본 형식이 아닙니다: " + promQl);
        }
        for (Map.Entry<String, JsonNode> field : metric.properties()) {
            if (!field.getValue().isString()) {
                throw new PromQueryException("vector 라벨 값이 문자열이 아닙니다: " + promQl);
            }
        }
    }

    /** vector 항목 하나를 불변 표본으로 변환합니다. */
    private static PromSample toSample(JsonNode entry, String promQl) {
        JsonNode value = entry.path("value");
        if (!value.isArray() || value.size() != 2) {
            throw new PromQueryException("표본 형식이 [timestamp, value] 가 아닙니다: " + promQl);
        }

        Map<String, String> labels = new HashMap<>();
        String metricName = "";
        JsonNode metric = entry.path("metric");
        for (Map.Entry<String, JsonNode> field : metric.properties()) {
            if (NAME_LABEL.equals(field.getKey())) {
                metricName = field.getValue().asString("");
            } else {
                labels.put(field.getKey(), field.getValue().asString(""));
            }
        }

        double epochSeconds = epochSecondsOf(value.get(0));
        // NaN·Inf 는 그대로 싣는다. 0 으로 바꾸면 "정상인데 0" 과 구분되지 않는다.
        double sampleValue = parseValue(value.get(1).asString(""));
        return new PromSample(
                metricName, labels, sampleValue, Instant.ofEpochMilli(Math.round(epochSeconds * 1000)));
    }

    /**
     * 표본 시각을 읽습니다. 숫자가 아니거나 관측일 수 없는 값이면 그 표본을 버립니다.
     *
     * @param timestamp 표본의 시각 노드
     * @return epoch 초
     * @throws PromQueryException 숫자가 아니거나 허용 범위 밖인 경우
     */
    private static double epochSecondsOf(JsonNode timestamp) {
        // asDouble() 은 숫자가 아니면 조용히 0.0 을 준다 — 그러면 관측 시각이 1970 년이 된다.
        if (!timestamp.isNumber() || !Double.isFinite(timestamp.doubleValue())) {
            throw new PromQueryException("표본 시각이 숫자가 아닙니다: " + timestamp.asString(""));
        }
        double epochSeconds = timestamp.doubleValue();
        if (epochSeconds < MIN_EPOCH_SECONDS || epochSeconds > MAX_EPOCH_SECONDS) {
            throw new PromQueryException("표본 시각이 관측일 수 없는 값입니다: " + epochSeconds);
        }
        return epochSeconds;
    }

    private static double parseValue(String raw) {
        // Prometheus 는 무한대를 "+Inf" · "-Inf" 로 직렬화하는데 Double.parseDouble 은 "Infinity"
        // 만 받는다. 매핑하지 않으면 나눗셈이 한 번 발산하는 순간 그 질의가 통째로 죽는다.
        if (POSITIVE_INFINITY.equals(raw)) {
            return Double.POSITIVE_INFINITY;
        }
        if (NEGATIVE_INFINITY.equals(raw)) {
            return Double.NEGATIVE_INFINITY;
        }
        try {
            return Double.parseDouble(raw);
        } catch (NumberFormatException notANumber) {
            throw new PromQueryException("표본 값을 해석할 수 없습니다: " + raw, notANumber);
        }
    }
}
