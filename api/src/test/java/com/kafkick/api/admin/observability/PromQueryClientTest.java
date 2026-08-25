package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.queryParam;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withServerError;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import java.time.Instant;
import java.util.List;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

/** Prometheus instant query 호출과 벡터 파싱 계약을 검증합니다. */
class PromQueryClientTest {

    private MockRestServiceServer server;
    private PromQueryClient client;

    private PromQueryClient bind(String body, boolean serverError) {
        RestClient.Builder builder = RestClient.builder().baseUrl("http://prometheus:9090");
        server = MockRestServiceServer.bindTo(builder).build();
        server.expect(requestTo(Matchers.containsString("/api/v1/query")))
                .andExpect(queryParam("query", Matchers.notNullValue()))
                .andRespond(serverError
                        ? withServerError()
                        : withSuccess(body, MediaType.APPLICATION_JSON));
        client = new PromQueryClient(builder.build());
        return client;
    }

    /** instant query 를 쓴다. query_range 를 쓰면 화면이 쓰지도 않을 시계열을 받는다. */
    @Test
    @DisplayName("벡터 결과를 라벨·값·시각이 붙은 표본으로 파싱한다")
    void parsesVector() {
        List<PromSample> samples = bind("""
                {"status":"success","data":{"resultType":"vector","result":[
                  {"metric":{"__name__":"app_http_inflight","instance":"api:9090"},
                   "value":[1755000000.5,"12"]}]}}
                """, false).query("app_http_inflight");

        assertThat(samples).hasSize(1);
        PromSample sample = samples.get(0);
        assertThat(sample.metricName()).isEqualTo("app_http_inflight");
        assertThat(sample.label("instance")).isEqualTo("api:9090");
        assertThat(sample.value()).isEqualTo(12d);
        assertThat(sample.evaluatedAt()).isEqualTo(Instant.ofEpochMilli(1755000000500L));
        server.verify();
    }

    /** Overview snapshot 질의는 서버 현재 시각이 아니라 요청 기준 시각에서 평가되어야 합니다. */
    @Test
    @DisplayName("명시한 시각을 Prometheus instant query 평가 시각으로 전달한다")
    void sendsExplicitInstantEvaluationTime() {
        Instant evaluationAt = Instant.parse("2026-08-23T03:00:00Z");
        RestClient.Builder builder = RestClient.builder().baseUrl("http://prometheus:9090");
        MockRestServiceServer expecting = MockRestServiceServer.bindTo(builder).build();
        expecting.expect(requestTo(Matchers.containsString("/api/v1/query")))
                .andExpect(request -> {
                    assertThat(request.getURI().getQuery()).isEqualTo(
                            "query=sum(increase(metric{stage=\"success\"}[1m]))"
                                    + "&time=2026-08-23T03:00:00Z");
                    assertThat(request.getURI().getRawQuery())
                            .contains("%7B", "%22")
                            .doesNotContain("{", "\"");
                })
                .andRespond(withSuccess("""
                        {"status":"success","data":{"resultType":"vector","result":[]}}
                        """, MediaType.APPLICATION_JSON));

        assertThat(new PromQueryClient(builder.build()).query(
                "sum(increase(metric{stage=\"success\"}[1m]))", evaluationAt)).isEmpty();
        expecting.verify();
    }

    /** Overview timed p99는 손상된 인스턴스를 버리고 낮은 max를 게시하면 안 됩니다. */
    @Test
    @DisplayName("시각 고정 p99 질의는 valid와 malformed 표본이 섞이면 전체를 거부한다")
    void timedQueryRejectsMixedValidAndMalformedP99Members() {
        PromQueryClient timed = bind("""
                {"status":"success","data":{"resultType":"vector","result":[
                  {"metric":{"instance":"api-1"},"value":[1755000000,"0.2"]},
                  {"metric":{"instance":"api-2"},"value":[1755000000,"broken"]}]}}
                """, false);

        assertThatThrownBy(() -> timed.query(
                OverviewPrometheusContract.successfulP99(),
                Instant.parse("2026-08-23T03:00:00Z")))
                .isInstanceOf(PromQueryException.class)
                .hasMessageContaining("표본 값을 해석할 수 없습니다");
    }

    /** malformed unknown outcome을 건너뛰면 정확히 14개 known 표본만 남아 거짓 VALID가 됩니다. */
    @Test
    @DisplayName("시각 고정 O3 질의는 14 known와 malformed unknown 표본이 섞이면 전체를 거부한다")
    void timedQueryRejectsFourteenKnownAndMalformedUnknownOutcomeMember() {
        PromQueryClient timed = bind("""
                {"status":"success","data":{"resultType":"vector","result":[
                  {"metric":{"outcome":"ISSUED"},"value":[1755000000,"1"]},
                  {"metric":{"outcome":"QUEUED"},"value":[1755000000,"1"]},
                  {"metric":{"outcome":"QUEUE_REQUIRED"},"value":[1755000000,"1"]},
                  {"metric":{"outcome":"ALREADY_ISSUED"},"value":[1755000000,"1"]},
                  {"metric":{"outcome":"STOCK_EXHAUSTED"},"value":[1755000000,"1"]},
                  {"metric":{"outcome":"NOT_OPENED"},"value":[1755000000,"1"]},
                  {"metric":{"outcome":"CAMPAIGN_CLOSED"},"value":[1755000000,"1"]},
                  {"metric":{"outcome":"GRADE_NOT_ELIGIBLE"},"value":[1755000000,"1"]},
                  {"metric":{"outcome":"NO_ENTRY_TOKEN"},"value":[1755000000,"1"]},
                  {"metric":{"outcome":"ENTRY_TOKEN_EXPIRED"},"value":[1755000000,"1"]},
                  {"metric":{"outcome":"INVALID_TRANSITION"},"value":[1755000000,"1"]},
                  {"metric":{"outcome":"TEMPORARILY_UNAVAILABLE"},"value":[1755000000,"1"]},
                  {"metric":{"outcome":"INTERNAL_ERROR"},"value":[1755000000,"1"]},
                  {"metric":{"outcome":"UNMAPPED"},"value":[1755000000,"1"]},
                  {"metric":{"outcome":"NEW_RESULT"},"value":[1755000000,"broken"]}]}}
                """, false);

        assertThatThrownBy(() -> timed.query(
                OverviewPrometheusContract.outcomes(),
                Instant.parse("2026-08-23T03:00:00Z")))
                .isInstanceOf(PromQueryException.class)
                .hasMessageContaining("표본 값을 해석할 수 없습니다");
    }

    /** missing/non-array result를 빈 vector로 보면 손상과 미배포를 구분할 수 없습니다. */
    @Test
    @DisplayName("시각 고정 질의는 result container가 array가 아니면 거부한다")
    void timedQueryRejectsMalformedResultContainer() {
        PromQueryClient timed = bind("""
                {"status":"success","data":{"resultType":"vector","result":{}}}
                """, false);

        assertThatThrownBy(() -> timed.query("metric", Instant.parse("2026-08-23T03:00:00Z")))
                .isInstanceOf(PromQueryException.class)
                .hasMessageContaining("result");
    }

    /** batch 는 값이 없을 때 NaN 을 싣고 이유는 상태 미터가 낸다. 0 으로 바꾸면 이유가 사라진다. */
    @Test
    @DisplayName("NaN 표본을 0으로 바꾸지 않고 그대로 보존한다")
    void keepsNaN() {
        List<PromSample> samples = bind("""
                {"status":"success","data":{"resultType":"vector","result":[
                  {"metric":{"__name__":"app_consistency_gap","type":"lua"},
                   "value":[1755000000,"NaN"]}]}}
                """, false).query("app_consistency_gap");

        assertThat(samples.get(0).value()).isNaN();
        assertThat(samples.get(0).hasNumericValue()).isFalse();
    }

    /** 일치하는 시계열이 없는 것은 실패가 아니다 — 빈 목록이고 조립하는 쪽이 PENDING 으로 만든다. */
    @Test
    @DisplayName("일치하는 시계열이 없으면 예외가 아니라 빈 목록이다")
    void emptyVectorIsNotFailure() {
        assertThat(bind("""
                {"status":"success","data":{"resultType":"vector","result":[]}}
                """, false).query("app_http_inflight")).isEmpty();
    }

    @Test
    @DisplayName("Prometheus가 실패 상태를 돌려주면 PromQueryException 이다")
    void failsOnErrorStatus() {
        PromQueryClient failing = bind("""
                {"status":"error","errorType":"bad_data","error":"parse error"}
                """, false);
        assertThatThrownBy(() -> failing.query("junk{"))
                .isInstanceOf(PromQueryException.class)
                .hasMessageContaining("parse error");
    }

    /** scalar·matrix 를 표본으로 접으면 라벨이 사라져 집계 규칙을 적용할 수 없다. */
    @Test
    @DisplayName("vector가 아닌 결과 타입은 거부한다")
    void rejectsNonVector() {
        PromQueryClient matrixOnly = bind("""
                {"status":"success","data":{"resultType":"matrix","result":[]}}
                """, false);
        assertThatThrownBy(() -> matrixOnly.query("x"))
                .isInstanceOf(PromQueryException.class)
                .hasMessageContaining("matrix");
    }

    /** 표본 하나가 이상해도 같은 질의의 나머지 지표를 함께 죽이면 안 된다. */
    @Test
    @DisplayName("무한대 표기를 해석하고, 해석 불가 표본은 질의를 죽이지 않고 건너뛴다")
    void parsesInfinityAndSkipsMalformedSamples() {
        List<PromSample> samples = bind("""
                {"status":"success","data":{"resultType":"vector","result":[
                  {"metric":{"__name__":"a"},"value":[1755000000,"+Inf"]},
                  {"metric":{"__name__":"b"},"value":[1755000000,"-Inf"]},
                  {"metric":{"__name__":"c"},"value":[1755000000,"nonsense"]},
                  {"metric":{"__name__":"d"},"value":[1755000000,"7"]}]}}
                """, false).query("x");

        assertThat(samples).hasSize(3);
        assertThat(samples.get(0).value()).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(samples.get(1).value()).isEqualTo(Double.NEGATIVE_INFINITY);
        // 무한대는 파싱은 되지만 값이 아니다. 통과시키면 Math.round 가 Long.MAX_VALUE 를 만든다.
        assertThat(samples.get(0).hasNumericValue()).isFalse();
        assertThat(samples.get(1).hasNumericValue()).isFalse();
        // 해석 불가 표본만 빠지고 뒤에 오는 정상 표본은 살아남는다.
        assertThat(samples.get(2).value()).isEqualTo(7d);
    }

    /**
     * 셀렉터의 중괄호가 URI 템플릿 변수로 해석되면 안 된다.
     *
     * <p>실측 — {@code queryParam("query", promQl)} 로 넣으면 RestClient 가
     * {@code {quantile!=""}} 를 템플릿 변수로 읽어 "Not enough variable values available to
     * expand" 로 죽는다. 조립기가 보내는 질의 넷 중 셋이 중괄호를 쓰므로 운영에서 그대로
     * 터지는데, 대역을 쓰는 조립기 테스트도 중괄호 없는 질의만 쓰던 이 파일도 못 잡았다.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "app_http_latency_seconds{quantile!=\"\"}",
            "max(time() - timestamp({__name__=~\"app_http_result_total|app_http_latency_seconds\"}))",
            "{__name__=~\"app_consistency_gap|app_consistency_gap_state\"}"
    })
    @DisplayName("셀렉터 중괄호가 든 질의도 그대로 전달된다")
    void passesSelectorsWithBracesUnchanged(String promQl) {
        RestClient.Builder builder = RestClient.builder().baseUrl("http://prometheus:9090");
        MockRestServiceServer expecting = MockRestServiceServer.bindTo(builder).build();
        expecting.expect(requestTo(Matchers.containsString("/api/v1/query")))
                // URI#getQuery 는 퍼센트 인코딩을 되돌린 값을 준다. 인코딩되어 나가되 서버가
                // 읽는 값은 원본 그대로여야 한다.
                .andExpect(request -> assertThat(request.getURI().getQuery())
                        .isEqualTo("query=" + promQl))
                .andRespond(withSuccess("""
                        {"status":"success","data":{"resultType":"vector","result":[]}}
                        """, MediaType.APPLICATION_JSON));

        assertThat(new PromQueryClient(builder.build()).query(promQl)).isEmpty();
        expecting.verify();
    }

    /** 시각이 숫자가 아니면 asDouble() 이 0.0 을 줘 관측 시각이 1970 년이 된다. */
    @Test
    @DisplayName("표본 시각이 숫자가 아니면 그 표본을 버린다")
    void dropsSampleWithNonNumericTimestamp() {
        List<PromSample> samples = bind("""
                {"status":"success","data":{"resultType":"vector","result":[
                  {"metric":{"__name__":"a"},"value":["not-a-time","1"]},
                  {"metric":{"__name__":"b"},"value":[1755000000,"2"]}]}}
                """, false).query("x");

        assertThat(samples).hasSize(1);
        assertThat(samples.get(0).metricName()).isEqualTo("b");
    }

    /**
     * 유한한 숫자여도 관측일 수 없는 시각은 버린다.
     *
     * <p>실측 — {@code Math.round} 는 범위를 넘길 때 예외 대신 {@code Long.MAX_VALUE} 로 포화되고
     * {@code Instant.ofEpochMilli} 도 그 값을 받아 {@code +292278994-08-17} 을 만든다. 조립기가
     * 가장 큰 시각을 {@code snapshotAt} 으로 뽑으므로 표본 하나가 응답 전체의 기준 시각을 오염시킨다.</p>
     */
    @ParameterizedTest
    @ValueSource(strings = {"1e18", "-1e18", "-1", "99999999999"})
    @DisplayName("허용 범위 밖의 시각을 가진 표본은 버린다")
    void dropsSampleWithOutOfRangeTimestamp(String epochSeconds) {
        List<PromSample> samples = bind("""
                {"status":"success","data":{"resultType":"vector","result":[
                  {"metric":{"__name__":"a"},"value":[%s,"1"]},
                  {"metric":{"__name__":"b"},"value":[1755000000,"2"]}]}}
                """.formatted(epochSeconds), false).query("x");

        assertThat(samples).hasSize(1);
        assertThat(samples.get(0).metricName()).isEqualTo("b");
        assertThat(samples.get(0).evaluatedAt()).isEqualTo(Instant.ofEpochSecond(1755000000L));
    }

    /** 전송 실패도 예외로 나가야 조립하는 쪽이 UNAVAILABLE 로 바꿀 수 있다. */
    @Test
    @DisplayName("HTTP 오류는 PromQueryException 으로 바뀐다")
    void wrapsTransportFailure() {
        PromQueryClient broken = bind("", true);
        assertThatThrownBy(() -> broken.query("x")).isInstanceOf(PromQueryException.class);
    }
}
