package com.kafkick.api.admin.observability;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tools.jackson.databind.JsonNode;

/**
 * Prometheus 의 instant query({@code /api/v1/query})를 호출하고 벡터 결과를 표본 목록으로 바꿉니다.
 *
 * <p><b>{@code query_range} 를 쓰지 않습니다.</b> 응답은 한 시점 스냅샷이고 차트의 과거 구간은
 * 화면이 1초 폴링으로 누적합니다. 비율·백분위의 집계 창은 질의 문자열 안의 구간 표기
 * ({@code rate(...[60s])})로 들어갑니다.</p>
 *
 * <p>실패는 예외로 나갑니다. HTTP 500 으로 번역하지 말고 조립하는 쪽이 잡아 해당 값만
 * {@code UNAVAILABLE} 로 내려보냅니다.</p>
 */
public class PromQueryClient {

    private static final Logger log = LoggerFactory.getLogger(PromQueryClient.class);

    private static final String QUERY_PATH = "/api/v1/query";
    private static final String NAME_LABEL = "__name__";

    private final RestClient restClient;

    public PromQueryClient(RestClient restClient) {
        this.restClient = restClient;
    }

    /**
     * instant query 하나를 실행합니다.
     *
     * @param promQl 실행할 PromQL
     * @return 벡터 결과의 표본 목록; 일치하는 시계열이 없으면 빈 목록
     * @throws PromQueryException 호출이 실패했거나 Prometheus 가 success 가 아닌 상태를 돌려준 경우
     */
    public List<PromSample> query(String promQl) {
        JsonNode body;
        try {
            body = restClient.get()
                    .uri(uriBuilder -> uriBuilder.path(QUERY_PATH).queryParam("query", promQl).build())
                    .retrieve()
                    .body(JsonNode.class);
        } catch (RestClientException failure) {
            throw new PromQueryException("Prometheus 질의에 실패했습니다: " + promQl, failure);
        }
        return parse(body, promQl);
    }

    /** Prometheus JSON 의 무한대 표기. 자바는 "Infinity" 만 받아 그대로 파싱하면 예외가 난다. */
    private static final String POSITIVE_INFINITY = "+Inf";
    private static final String NEGATIVE_INFINITY = "-Inf";

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

        double epochSeconds = value.get(0).asDouble();
        // NaN·Inf 는 그대로 싣는다. 0 으로 바꾸면 "정상인데 0" 과 구분되지 않는다.
        double sampleValue = parseValue(value.get(1).asString(""));
        return new PromSample(
                metricName, labels, sampleValue, Instant.ofEpochMilli(Math.round(epochSeconds * 1000)));
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
