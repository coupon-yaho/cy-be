package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import com.kafkick.core.benchmark.RunTimeseriesArchiver.Metric;

/** MockRestRequestMatcher가 아니라 실제 TCP/URI 생성을 거쳐 중괄호 PromQL을 태운다. */
class PromQueryClientRangeTest {

    @Test
    void sendsBracePromQlOverHttpAndUsesMatrixTimestamps() throws Exception {
        AtomicReference<String> rawQuery = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/query_range", exchange -> {
            rawQuery.set(exchange.getRequestURI().getRawQuery());
            byte[] response = """
                    {"status":"success","data":{"resultType":"matrix","result":[
                      {"metric":{"__name__":"app_http_latency_seconds","instance":"api-2:9090"},
                       "values":[[1787414400,"0.123"],[1787414401,"NaN"]]}]}}
                    """.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        try {
            PromQueryClient client = new PromQueryClient(RestClient.builder()
                    .baseUrl("http://127.0.0.1:" + server.getAddress().getPort()).build());
            var samples = client.queryRange(Metric.LATENCY_P99,
                    Instant.ofEpochSecond(1787414400), Instant.ofEpochSecond(1787414401), 1);

            String decoded = URLDecoder.decode(rawQuery.get(), StandardCharsets.UTF_8);
            // 이 테스트가 지키는 것은 질의의 '내용' 이 아니라 '전선 위에서 그대로인가' 다
            // (CY-264: RestClient 가 중괄호를 URI 템플릿 변수로 읽어 첫 요청부터 죽었다).
            // 그래서 문자열을 여기 옮겨 적지 않고 조립기에서 가져온다 — 내용은
            // LatencySeriesOutcomeContractTest 가 따로 고정한다. 라벨이 늘수록 중괄호 안
            // 항목이 늘어 이 경로의 위험도 같이 는다.
            assertThat(decoded)
                    .contains("query=" + PromQueryClient.rangeQueryFor(Metric.LATENCY_P99));
            assertThat(decoded)
                    .as("중괄호 안 라벨이 둘 이상일 때도 전선에서 살아남아야 합니다")
                    .contains("{quantile=\"0.99\",outcome=\"success\"}");
            assertThat(samples).hasSize(2);
            assertThat(samples.get(0).observedAt()).isEqualTo(Instant.ofEpochSecond(1787414400));
            assertThat(samples.get(0).value()).isEqualTo(0.123);
            assertThat(samples.get(0).sourceInstance()).isEqualTo("api-2:9090");
            assertThat(samples.get(1).value()).isNull();
        } finally {
            server.stop(0);
        }
    }

    @Test
    void preservesFractionalRangeBoundaries() throws Exception {
        AtomicReference<String> rawQuery = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/query_range", exchange -> {
            rawQuery.set(exchange.getRequestURI().getRawQuery());
            byte[] response = """
                    {"status":"success","data":{"resultType":"matrix","result":[]}}
                    """.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        try {
            PromQueryClient client = new PromQueryClient(RestClient.builder()
                    .baseUrl("http://127.0.0.1:" + server.getAddress().getPort()).build());
            client.queryRange(Metric.DB_POOL_USAGE,
                    Instant.parse("2026-08-23T00:00:00.123456Z"),
                    Instant.parse("2026-08-23T00:01:05.654321Z"), 1);

            String decoded = URLDecoder.decode(rawQuery.get(), StandardCharsets.UTF_8);
            assertThat(decoded).contains("start=1787443200.123456");
            assertThat(decoded).contains("end=1787443265.654321");
            assertThat(decoded).contains("hikaricp_connections_active{job=\"api\",pool!=\"obs-pool\"}");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void rejectsMultipleValueSeriesAtTheSameTimestamp() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/query_range", exchange -> {
            byte[] response = """
                    {"status":"success","data":{"resultType":"matrix","result":[
                      {"metric":{"__name__":"app_coupon_stock_remaining","instance":"api-1"},
                       "values":[[1787414400,"100"]]},
                      {"metric":{"__name__":"app_coupon_stock_remaining","instance":"api-2"},
                       "values":[[1787414400,"7"]]}
                    ]}}
                    """.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        try {
            PromQueryClient client = new PromQueryClient(RestClient.builder()
                    .baseUrl("http://127.0.0.1:" + server.getAddress().getPort()).build());

            assertThatThrownBy(() -> client.queryRange(Metric.STOCK_REMAINING,
                    Instant.ofEpochSecond(1787414400), Instant.ofEpochSecond(1787414401), 1))
                    .isInstanceOf(PromQueryException.class)
                    .hasMessageContaining("중복");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void preservesFractionalMatrixTimestamp() throws Exception {
        HttpServer server = rangeServer("""
                {"status":"success","data":{"resultType":"matrix","result":[
                  {"metric":{"__name__":"app_http_latency_seconds","instance":"api-1"},
                   "values":[[1787414400.123456,"1"]]}
                ]}}
                """);
        try {
            PromQueryClient client = client(server);

            assertThat(client.queryRange(Metric.LATENCY_P99,
                    Instant.parse("2026-08-22T16:00:00.123456Z"),
                    Instant.parse("2026-08-22T16:00:01.123456Z"), 1).get(0).observedAt())
                    .isEqualTo(Instant.parse("2026-08-22T16:00:00.123456Z"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void rejectsValueAndStateSeriesWithDifferentLabels() throws Exception {
        HttpServer server = rangeServer("""
                {"status":"success","data":{"resultType":"matrix","result":[
                  {"metric":{"__name__":"app_coupon_stock_remaining","instance":"api-1"},
                   "values":[[1787414400,"100"]]},
                  {"metric":{"__name__":"app_coupon_stock_remaining_state","instance":"api-2"},
                   "values":[[1787414400,"1"]]}
                ]}}
                """);
        try {
            PromQueryClient client = client(server);

            assertThatThrownBy(() -> client.queryRange(Metric.STOCK_REMAINING,
                    Instant.ofEpochSecond(1787414400), Instant.ofEpochSecond(1787414401), 1))
                    .isInstanceOf(PromQueryException.class)
                    .hasMessageContaining("라벨");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void preservesNotApplicableStockAsNullValue() throws Exception {
        HttpServer server = rangeServer("""
                {"status":"success","data":{"resultType":"matrix","result":[
                  {"metric":{"__name__":"app_coupon_stock_remaining_state"},
                   "values":[[1787414400,"7"],[1787414401,"7"]]}
                ]}}
                """);
        try {
            var samples = client(server).queryRange(Metric.STOCK_REMAINING,
                Instant.ofEpochSecond(1787414400), Instant.ofEpochSecond(1787414401), 1);

            assertThat(samples).extracting("state")
                .containsOnly(com.kafkick.core.benchmark.RunTimeseriesArchiver.State.N_A);
            assertThat(samples).extracting("value").containsOnlyNulls();
        } finally {
            server.stop(0);
        }
    }

    private static HttpServer rangeServer(String body) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/query_range", exchange -> {
            byte[] response = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        return server;
    }

    private static PromQueryClient client(HttpServer server) {
        return new PromQueryClient(RestClient.builder()
                .baseUrl("http://127.0.0.1:" + server.getAddress().getPort()).build());
    }
}
