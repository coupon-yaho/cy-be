package com.kafkick.api.admin.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import com.kafkick.core.consistency.ConsistencyGapType;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.exception.BusinessException;

class HttpBatchConsistencyFinalClientTest {
    @Test
    void realRestClientPreservesFinalEvaluationAndRequestParameters() throws IOException {
        AtomicReference<String> query = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/internal/v1/benchmarks/consistency/final", exchange -> {
            query.set(exchange.getRequestURI().getQuery());
            byte[] body = ("""
                {"evaluation":{"gaps":{
                  "ACTIVE_DB_GAP":{"value":0,"state":"VALID","observedAt":"2026-08-25T00:00:00Z"},
                  "LUA_GAP":{"value":1,"state":"STALE","observedAt":"2026-08-25T00:00:00Z"},
                  "PERSIST_GAP":{"value":0,"state":"VALID","observedAt":"2026-08-25T00:00:00Z"},
                  "DB_COUNTER_GAP":{"value":0,"state":"VALID","observedAt":"2026-08-25T00:00:00Z"}},
                 "overIssued":{"value":0,"state":"VALID","observedAt":"2026-08-25T00:00:00Z"},
                 "phase":"FINAL","verdict":"FAIL","severity":"CRITICAL"},"violations":[]}
                """).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try {
            var client = new HttpBatchConsistencyFinalClient(
                    "http://127.0.0.1:" + server.getAddress().getPort(),
                    Duration.ofMillis(100), Duration.ofSeconds(1));
            var result = client.evaluate(11L, EngineVersion.V3, Instant.parse("2026-08-26T00:00:00Z"));

            assertThat(result.phase()).isEqualTo(ConsistencyPhase.FINAL);
            assertThat(result.gaps().get(ConsistencyGapType.LUA_GAP).state())
                    .isEqualTo(SourceStatus.STALE);
            assertThat(query.get()).contains("couponId=11", "engineVersion=V3",
                    "runFinalizedAt=2026-08-26T00");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void conflictBodyIsCarriedIntoTheExceptionSoFailureReasonIsActionable() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/internal/v1/benchmarks/consistency/final", exchange -> {
            byte[] body = ("""
                {"violations":[{"key":"observation.domain-gauge.coupon-id",
                  "expected":"11","actual":"12",
                  "reason":"batch 관측 대상과 FINAL 회차 couponId가 다릅니다"}]}
                """).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(409, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try {
            var client = new HttpBatchConsistencyFinalClient(
                    "http://127.0.0.1:" + server.getAddress().getPort(),
                    Duration.ofMillis(100), Duration.ofSeconds(1));

            // 상태코드만 남으면 consistency_failure_reason 이 재실행 판단 근거가 되지 못한다.
            assertThatThrownBy(() -> client.evaluate(11L, EngineVersion.V3, Instant.parse("2026-08-26T00:00:00Z")))
                    .isInstanceOf(BusinessException.class)
                    .hasMessageContaining("409")
                    .hasMessageContaining("coupon-id");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void unreachableBatchIsReportedWithItsCauseInsteadOfANakedStackTrace() {
        var client = new HttpBatchConsistencyFinalClient(
                "http://127.0.0.1:1", Duration.ofMillis(100), Duration.ofMillis(200));
        assertThatThrownBy(() -> client.evaluate(11L, EngineVersion.V3, Instant.parse("2026-08-26T00:00:00Z")))
                .isInstanceOf(BusinessException.class)
                .hasMessageContaining("batch FINAL 계산 호출 실패");
    }
}
