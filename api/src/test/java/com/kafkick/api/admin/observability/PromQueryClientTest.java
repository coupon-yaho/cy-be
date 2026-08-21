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
        // 해석 불가 표본만 빠지고 뒤에 오는 정상 표본은 살아남는다.
        assertThat(samples.get(2).value()).isEqualTo(7d);
    }

    /** 전송 실패도 예외로 나가야 조립하는 쪽이 UNAVAILABLE 로 바꿀 수 있다. */
    @Test
    @DisplayName("HTTP 오류는 PromQueryException 으로 바뀐다")
    void wrapsTransportFailure() {
        PromQueryClient broken = bind("", true);
        assertThatThrownBy(() -> broken.query("x")).isInstanceOf(PromQueryException.class);
    }
}
