package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withServerError;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

/** Prometheus range query 호출과 matrix 파싱 계약을 검증합니다. */
class PromRangeQueryClientTest {

    /** URI 변수로 전달한 셀렉터와 구간을 보존하고 matrix를 시간순 표본으로 읽습니다. */
    @Test
    @DisplayName("query_range URI 변수를 전달하고 matrix를 파싱한다")
    void sendsRangeVariablesAndParsesMatrix() {
        RestClient.Builder builder = RestClient.builder().baseUrl("http://prometheus:9090");
        MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
        String promQl = "sum by (coupon_id) (increase(app_issuance_flow_total{stage=\"success\"}[1m]))";
        server.expect(requestTo(Matchers.containsString("/api/v1/query_range")))
                .andExpect(request -> {
                    assertThat(request.getURI().getQuery()).isEqualTo(
                            "query=" + promQl
                                    + "&start=2026-08-23T00:00:00Z"
                                    + "&end=2026-08-23T00:10:00Z&step=60");
                    assertThat(request.getURI().getRawQuery())
                            .contains("%7B", "%22", "%20")
                            .doesNotContain("{", "\"");
                })
                .andRespond(withSuccess("""
                        {"status":"success","data":{"resultType":"matrix","result":[
                          {"metric":{"coupon_id":"101","stage":"success"},"values":[
                            [1787443200,"2"],[1787443260.5,"3"]]}]}}
                        """, MediaType.APPLICATION_JSON));

        List<PromRangeSeries> series = new PromRangeQueryClient(builder.build()).query(
                promQl,
                Instant.parse("2026-08-23T00:00:00Z"),
                Instant.parse("2026-08-23T00:10:00Z"),
                Duration.ofMinutes(1));

        assertThat(series).containsExactly(new PromRangeSeries(
                java.util.Map.of("coupon_id", "101", "stage", "success"),
                List.of(
                        new PromRangePoint(Instant.ofEpochSecond(1787443200L), 2d),
                        new PromRangePoint(Instant.ofEpochMilli(1787443260500L), 3d))));
        server.verify();
    }

    /** 실패 응답과 vector를 matrix로 오인하면 구간 추세를 잘못 조립하게 됩니다. */
    @Test
    @DisplayName("실패 상태와 matrix가 아닌 결과 타입을 거부한다")
    void rejectsServerFailureAndWrongResultType() {
        assertThatThrownBy(() -> clientFor("""
                {"status":"error","error":"bad query"}
                """).query("x", start(), end(), Duration.ofMinutes(1)))
                .isInstanceOf(PromQueryException.class)
                .hasMessageContaining("bad query");
        assertThatThrownBy(() -> clientFor("""
                {"status":"success","data":{"resultType":"vector","result":[]}}
                """).query("x", start(), end(), Duration.ofMinutes(1)))
                .isInstanceOf(PromQueryException.class)
                .hasMessageContaining("vector");

        RestClient.Builder builder = RestClient.builder().baseUrl("http://prometheus:9090");
        MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
        server.expect(requestTo(Matchers.containsString("/api/v1/query_range")))
                .andRespond(withServerError());
        assertThatThrownBy(() -> new PromRangeQueryClient(builder.build())
                .query("x", start(), end(), Duration.ofMinutes(1)))
                .isInstanceOf(PromQueryException.class);
    }

    /** 최상위 JSON이 객체가 아니면 빈 matrix로 오인하지 않고 질의 실패로 올립니다. */
    @Test
    @DisplayName("malformed top-level range 응답을 거부한다")
    void rejectsMalformedTopLevelResponse() {
        assertThatThrownBy(() -> clientFor("[]")
                .query("x", start(), end(), Duration.ofMinutes(1)))
                .isInstanceOf(PromQueryException.class)
                .hasMessageContaining("응답");
    }

    /** 일부라도 손상되면 부분 matrix를 정상 결과로 위장할 수 없으므로 전체 질의를 실패시킵니다. */
    @Test
    @DisplayName("정상 점과 섞인 손상 point 또는 series도 range 질의를 실패시킨다")
    void rejectsAnyMalformedPointOrSeries() {
        assertThatThrownBy(() -> clientFor("""
                {"status":"success","data":{"resultType":"matrix","result":[
                  {"metric":{"coupon_id":"101"},"values":[
                    ["not-a-time","1"],[1787443200,"nonsense"],[1787443260,"4"]]}]}}
                """).query("x", start(), end(), Duration.ofMinutes(1)))
                .isInstanceOf(PromQueryException.class)
                .hasMessageContaining("시각");
        assertThatThrownBy(() -> clientFor("""
                {"status":"success","data":{"resultType":"matrix","result":[
                  {"metric":{"coupon_id":"101"},"values":[[1787443260,"4"]]},
                  {"metric":"broken","values":[[1787443260,"4"]]}]}}
                """).query("x", start(), end(), Duration.ofMinutes(1)))
                .isInstanceOf(PromQueryException.class)
                .hasMessageContaining("시계열");
        assertThatThrownBy(() -> clientFor("""
                {"status":"success","data":{"resultType":"matrix","result":[
                  {"metric":{"coupon_id":"101"},"values":[[99999999999,"1"]]}]}}
                """).query("x", start(), end(), Duration.ofMinutes(1)))
                .isInstanceOf(PromQueryException.class)
                .hasMessageContaining("범위");
    }

    /** HTTP 전에 막지 않으면 실수 한 번이 Prometheus에 과도한 평가 부하를 줍니다. */
    @Test
    @DisplayName("최대 구간과 최대 평가점 수를 HTTP 호출 전에 검증한다")
    void rejectsExcessiveRangeAndPointBudget() {
        PromRangeQueryClient client = new PromRangeQueryClient(
                RestClient.builder().baseUrl("http://prometheus:9090").build(),
                Duration.ofHours(1), 10);

        assertThatThrownBy(() -> client.query(
                "x", start(), start().plus(Duration.ofHours(1)).plusSeconds(1), Duration.ofMinutes(1)))
                .isInstanceOf(PromQueryException.class)
                .hasMessageContaining("범위");
        assertThatThrownBy(() -> client.query(
                "x", start(), end(), Duration.ofMinutes(1)))
                .isInstanceOf(PromQueryException.class)
                .hasMessageContaining("평가점");
        assertThatThrownBy(() -> client.query("x", start(), end(), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("step");
    }

    /** Prometheus는 off-grid end를 덧붙이지 않으므로 floor(range/step)+1만 평가합니다. */
    @Test
    @DisplayName("평가점 예산은 exact·non-divisible·equal·greater step 경계에서 floor grid를 따른다")
    void countsOnlyPrometheusGridPointsAtEveryStepBoundary() {
        Instant start = start();

        assertThat(clientFor("""
                {"status":"success","data":{"resultType":"matrix","result":[]}}
                """, 3).query("x", start, start.plusSeconds(12), Duration.ofSeconds(6))).isEmpty();
        assertThatThrownBy(() -> clientWithoutRequest(2).query(
                "x", start, start.plusSeconds(12), Duration.ofSeconds(6)))
                .isInstanceOf(PromQueryException.class)
                .hasMessageContaining("평가점");
        assertThat(clientFor("""
                {"status":"success","data":{"resultType":"matrix","result":[]}}
                """, 2).query("x", start, start.plusSeconds(10), Duration.ofSeconds(6))).isEmpty();
        assertThat(clientFor("""
                {"status":"success","data":{"resultType":"matrix","result":[]}}
                """, 2).query("x", start, start.plusSeconds(10), Duration.ofSeconds(10))).isEmpty();
        assertThat(clientFor("""
                {"status":"success","data":{"resultType":"matrix","result":[]}}
                """, 1).query("x", start, start.plusSeconds(10), Duration.ofSeconds(11))).isEmpty();
    }

    /** 호출자가 원본 컬렉션이나 반환 컬렉션을 바꿔 과거 결과를 변조하면 안 됩니다. */
    @Test
    @DisplayName("matrix 라벨과 점 목록은 불변이고 방어 복사된다")
    void matrixLabelsAndPointsAreImmutable() {
        Map<String, String> labels = new LinkedHashMap<>(Map.of("coupon_id", "101"));
        List<PromRangePoint> points = new ArrayList<>(List.of(
                new PromRangePoint(Instant.ofEpochSecond(1L), 1d)));
        PromRangeSeries series = new PromRangeSeries(labels, points);

        labels.put("coupon_id", "999");
        points.clear();

        assertThat(series.label("coupon_id")).isEqualTo("101");
        assertThat(series.points()).hasSize(1);
        assertThatThrownBy(() -> series.labels().put("new", "value"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> series.points().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /** NaN과 Inf를 0으로 바꾸면 미관측이 실제 무트래픽으로 바뀝니다. */
    @Test
    @DisplayName("NaN과 무한대를 0으로 바꾸지 않고 비숫자 값으로 보존한다")
    void preservesNonFiniteValues() {
        List<PromRangePoint> points = clientFor("""
                {"status":"success","data":{"resultType":"matrix","result":[
                  {"metric":{},"values":[
                    [1787443200,"NaN"],[1787443260,"+Inf"],[1787443320,"-Inf"]]}]}}
                """).query("x", start(), end(), Duration.ofMinutes(1)).get(0).points();

        assertThat(points.get(0).value()).isNaN();
        assertThat(points.get(1).value()).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(points.get(2).value()).isEqualTo(Double.NEGATIVE_INFINITY);
        assertThat(points).noneMatch(PromRangePoint::hasNumericValue);
        assertThat(points).noneMatch(point -> point.value() == 0d);
    }

    /** 성공 JSON 하나를 반환하는 실제 RestClient 기반 경계를 만듭니다. */
    private static PromRangeQueryClient clientFor(String body) {
        return clientFor(body, 1_000);
    }

    /** 성공 JSON과 지정 평가점 상한을 가진 실제 RestClient 경계를 만듭니다. */
    private static PromRangeQueryClient clientFor(String body, int maxPoints) {
        RestClient.Builder builder = RestClient.builder().baseUrl("http://prometheus:9090");
        MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
        server.expect(requestTo(Matchers.containsString("/api/v1/query_range")))
                .andRespond(withSuccess(body, MediaType.APPLICATION_JSON));
        return new PromRangeQueryClient(builder.build(), Duration.ofHours(1), maxPoints);
    }

    /** HTTP 전에 평가점 상한에 걸려야 하는 요청용 클라이언트를 만듭니다. */
    private static PromRangeQueryClient clientWithoutRequest(int maxPoints) {
        return new PromRangeQueryClient(
                RestClient.builder().baseUrl("http://prometheus:9090").build(),
                Duration.ofHours(1), maxPoints);
    }

    /** 테스트 구간 시작 시각입니다. */
    private static Instant start() {
        return Instant.parse("2026-08-23T00:00:00Z");
    }

    /** 테스트 구간 종료 시각입니다. */
    private static Instant end() {
        return Instant.parse("2026-08-23T00:10:00Z");
    }
}
