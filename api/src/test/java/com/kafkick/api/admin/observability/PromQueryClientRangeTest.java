package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;

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
            assertThat(decoded).contains("query=topk(1, app_http_latency_seconds{quantile=\"0.99\"})");
            assertThat(samples).hasSize(2);
            assertThat(samples.get(0).observedAt()).isEqualTo(Instant.ofEpochSecond(1787414400));
            assertThat(samples.get(0).value()).isEqualTo(0.123);
            assertThat(samples.get(0).sourceInstance()).isEqualTo("api-2:9090");
            assertThat(samples.get(1).value()).isNull();
        } finally {
            server.stop(0);
        }
    }
}
