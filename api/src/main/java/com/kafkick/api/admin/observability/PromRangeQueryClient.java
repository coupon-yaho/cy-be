package com.kafkick.api.admin.observability;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import tools.jackson.databind.JsonNode;

/** Prometheus {@code query_range} HTTP API를 호출하고 matrix 결과를 파싱합니다. */
public class PromRangeQueryClient implements PromRangeQuery {

    private static final String QUERY_PATH = "/api/v1/query_range";
    private static final double MIN_EPOCH_SECONDS = 0d;
    private static final double MAX_EPOCH_SECONDS = 4102444800d;
    private static final String POSITIVE_INFINITY = "+Inf";
    private static final String NEGATIVE_INFINITY = "-Inf";

    private final RestClient restClient;
    private final Duration maxRange;
    private final int maxPoints;

    /** Overview 기본 상한인 1시간·1,000 평가점을 사용하는 클라이언트를 생성합니다. */
    public PromRangeQueryClient(RestClient restClient) {
        this(restClient, OverviewPrometheusProperties.defaults().maxRange(),
                OverviewPrometheusProperties.defaults().maxPoints());
    }

    /**
     * 명시한 요청 상한을 사용하는 클라이언트를 생성합니다.
     *
     * @param restClient Prometheus 전용 HTTP 클라이언트
     * @param maxRange 허용할 최대 조회 구간
     * @param maxPoints 한 요청에서 허용할 최대 평가점 수
     */
    public PromRangeQueryClient(RestClient restClient, Duration maxRange, int maxPoints) {
        this.restClient = Objects.requireNonNull(restClient, "restClient");
        this.maxRange = Objects.requireNonNull(maxRange, "maxRange");
        if (maxRange.isZero() || maxRange.isNegative()) {
            throw new IllegalArgumentException("maxRange는 양수여야 합니다.");
        }
        if (maxPoints <= 0) {
            throw new IllegalArgumentException("maxPoints는 양수여야 합니다.");
        }
        this.maxPoints = maxPoints;
    }

    /** {@inheritDoc} */
    @Override
    public List<PromRangeSeries> query(String promQl, Instant start, Instant end, Duration step) {
        validateRequest(promQl, start, end, step);
        JsonNode body;
        try {
            body = restClient.get()
                    // PromQL 셀렉터의 중괄호를 URI 템플릿으로 오인하지 않도록 값은 변수로 넘깁니다.
                    .uri(uriBuilder -> uriBuilder.path(QUERY_PATH)
                            .queryParam("query", "{query}")
                            .queryParam("start", "{start}")
                            .queryParam("end", "{end}")
                            .queryParam("step", "{step}")
                            .build(promQl, start.toString(), end.toString(), secondsText(step)))
                    .retrieve()
                    .body(JsonNode.class);
        } catch (RestClientException failure) {
            throw new PromQueryException("Prometheus 구간 질의에 실패했습니다: " + promQl, failure);
        }
        return parse(body, promQl);
    }

    /** 요청 구간과 예상 평가점 수가 클라이언트 상한 안인지 HTTP 호출 전에 검증합니다. */
    private void validateRequest(String promQl, Instant start, Instant end, Duration step) {
        if (promQl == null || promQl.isBlank()) {
            throw new IllegalArgumentException("promQl은 비어 있을 수 없습니다.");
        }
        Objects.requireNonNull(start, "start");
        Objects.requireNonNull(end, "end");
        Objects.requireNonNull(step, "step");
        if (!end.isAfter(start)) {
            throw new IllegalArgumentException("end는 start 이후여야 합니다.");
        }
        if (step.isZero() || step.isNegative()) {
            throw new IllegalArgumentException("step은 양수여야 합니다.");
        }
        Duration range = Duration.between(start, end);
        if (range.compareTo(maxRange) > 0) {
            throw new PromQueryException("Prometheus 구간 질의 범위가 상한을 넘습니다: " + range);
        }
        long expectedPoints = expectedPointCount(range, step);
        if (expectedPoints > maxPoints) {
            throw new PromQueryException("Prometheus 구간 질의 평가점이 상한을 넘습니다: " + expectedPoints);
        }
    }

    /** Prometheus의 {@code start + n * step <= end} grid 평가점 수를 계산합니다. */
    private static long expectedPointCount(Duration range, Duration step) {
        try {
            return Math.addExact(range.dividedBy(step), 1L);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    /** Prometheus step 파라미터가 받는 소수 epoch 초 문자열로 변환합니다. */
    private static String secondsText(Duration step) {
        BigDecimal seconds = BigDecimal.valueOf(step.getSeconds())
                .add(BigDecimal.valueOf(step.getNano(), 9));
        return seconds.stripTrailingZeros().toPlainString();
    }

    /** 성공한 matrix 응답만 불변 시계열 목록으로 변환합니다. */
    private static List<PromRangeSeries> parse(JsonNode body, String promQl) {
        if (body == null || !body.isObject()) {
            throw new PromQueryException("Prometheus 구간 응답이 비어 있거나 객체가 아닙니다: " + promQl);
        }
        String status = body.path("status").asString("");
        if (!"success".equals(status)) {
            throw new PromQueryException(
                    "Prometheus 가 실패를 반환했습니다: " + body.path("error").asString(status));
        }
        JsonNode data = body.path("data");
        String resultType = data.path("resultType").asString("");
        if (!"matrix".equals(resultType)) {
            throw new PromQueryException("matrix 가 아닌 결과 타입입니다: " + resultType);
        }
        JsonNode result = data.path("result");
        if (!result.isArray()) {
            throw new PromQueryException("matrix result가 배열이 아닙니다: " + promQl);
        }

        List<PromRangeSeries> series = new ArrayList<>();
        for (JsonNode entry : result) {
            series.add(toSeries(entry, promQl));
        }
        return List.copyOf(series);
    }

    /** matrix 항목 하나의 라벨과 해석 가능한 점을 변환합니다. */
    private static PromRangeSeries toSeries(JsonNode entry, String promQl) {
        if (!entry.isObject() || !entry.path("metric").isObject() || !entry.path("values").isArray()) {
            throw new PromQueryException("matrix 시계열 형식이 아닙니다: " + promQl);
        }
        Map<String, String> labels = new LinkedHashMap<>();
        for (Map.Entry<String, JsonNode> field : entry.path("metric").properties()) {
            labels.put(field.getKey(), field.getValue().asString(""));
        }
        List<PromRangePoint> points = new ArrayList<>();
        for (JsonNode value : entry.path("values")) {
            points.add(toPoint(value));
        }
        if (points.isEmpty()) {
            throw new PromQueryException("matrix 시계열에 표본이 없습니다: " + promQl);
        }
        return new PromRangeSeries(labels, points);
    }

    /** Prometheus의 {@code [timestamp, value]} 한 점을 변환합니다. */
    private static PromRangePoint toPoint(JsonNode value) {
        if (!value.isArray() || value.size() != 2) {
            throw new PromQueryException("matrix 표본 형식이 [timestamp, value]가 아닙니다.");
        }
        double epochSeconds = epochSecondsOf(value.get(0));
        double sampleValue = parseValue(value.get(1).asString(""));
        return new PromRangePoint(
                Instant.ofEpochMilli(Math.round(epochSeconds * 1000d)), sampleValue);
    }

    /** 유한하고 관측 가능한 범위의 epoch 초를 읽습니다. */
    private static double epochSecondsOf(JsonNode timestamp) {
        if (!timestamp.isNumber() || !Double.isFinite(timestamp.doubleValue())) {
            throw new PromQueryException("matrix 표본 시각이 숫자가 아닙니다.");
        }
        double epochSeconds = timestamp.doubleValue();
        if (epochSeconds < MIN_EPOCH_SECONDS || epochSeconds > MAX_EPOCH_SECONDS) {
            throw new PromQueryException("matrix 표본 시각이 허용 범위 밖입니다: " + epochSeconds);
        }
        return epochSeconds;
    }

    /** NaN·무한대를 보존하면서 Prometheus 문자열 값을 읽습니다. */
    private static double parseValue(String raw) {
        if (POSITIVE_INFINITY.equals(raw)) {
            return Double.POSITIVE_INFINITY;
        }
        if (NEGATIVE_INFINITY.equals(raw)) {
            return Double.NEGATIVE_INFINITY;
        }
        try {
            return Double.parseDouble(raw);
        } catch (NumberFormatException malformed) {
            throw new PromQueryException("matrix 표본 값을 해석할 수 없습니다: " + raw, malformed);
        }
    }

}
