package com.kafkick.api.admin.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

class HttpBatchTopologyPreflightTest {

    @Test
    void malformedResponseBecomesViolationInsteadOfFiveHundred() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/internal/v1/benchmarks/preflight", exchange -> {
            byte[] body = "{\"valid\":true,\"violations\":null}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try {
            HttpBatchTopologyPreflight preflight = new HttpBatchTopologyPreflight(
                "http://127.0.0.1:" + server.getAddress().getPort(),
                Duration.ofMillis(50), Duration.ofMillis(50));

            BatchTopologyPreflight.Result result = preflight.validate(10L);

            assertThat(result.valid()).isFalse();
            assertThat(result.violations()).extracting("key")
                .containsExactly("batch.preflight.response");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void readTimeoutBecomesViolationInsteadOfHoldingStartWorker() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        server.setExecutor(executor);
        server.createContext("/internal/v1/benchmarks/preflight", exchange -> {
            try {
                Thread.sleep(500);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            exchange.close();
        });
        server.start();
        try {
            HttpBatchTopologyPreflight preflight = new HttpBatchTopologyPreflight(
                "http://127.0.0.1:" + server.getAddress().getPort(),
                Duration.ofMillis(50), Duration.ofMillis(50));

            BatchTopologyPreflight.Result result = preflight.validate(10L);

            assertThat(result.valid()).isFalse();
            assertThat(result.violations()).extracting("key", "actual")
                .containsExactly(org.assertj.core.groups.Tuple.tuple(
                    "batch.preflight", "unavailable"));
        } finally {
            server.stop(0);
            executor.shutdownNow();
        }
    }

    @Test
    void validResponsePreservesViolationContract() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/internal/v1/benchmarks/preflight", exchange -> {
            byte[] body = ("{\"valid\":false,\"violations\":[{\"key\":\"batch.scheduling.enabled\","
                + "\"expected\":\"false\",\"actual\":\"true\",\"reason\":\"must be off\"}]}")
                .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(409, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try {
            var preflight = new HttpBatchTopologyPreflight(
                "http://127.0.0.1:" + server.getAddress().getPort(),
                Duration.ofMillis(50), Duration.ofMillis(50));

            var result = preflight.validate(10L);

            assertThat(result.valid()).isFalse();
            assertThat(result.violations()).singleElement().satisfies(violation -> {
                assertThat(violation.key()).isEqualTo("batch.scheduling.enabled");
                assertThat(violation.actual()).isEqualTo("true");
            });
        } finally {
            server.stop(0);
        }
    }

    @Test
    void serverErrorPreservesTheHttpStatusInsteadOfPretendingToBeATimeout() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/internal/v1/benchmarks/preflight", exchange -> {
            exchange.sendResponseHeaders(500, -1);
            exchange.close();
        });
        server.start();
        try {
            var preflight = new HttpBatchTopologyPreflight(
                "http://127.0.0.1:" + server.getAddress().getPort(),
                Duration.ofMillis(50), Duration.ofMillis(50));

            var result = preflight.validate(10L);

            assertThat(result.violations()).extracting("key", "expected", "actual")
                .containsExactly(org.assertj.core.groups.Tuple.tuple(
                    "batch.preflight", "HTTP 200 or 409", "HTTP 500"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void malformedConflictResponseIsRejectedAfterThe409StatusIsAccepted() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/internal/v1/benchmarks/preflight", exchange -> {
            byte[] body = "{\"valid\":false,\"violations\":null}"
                .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(409, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try {
            var preflight = new HttpBatchTopologyPreflight(
                "http://127.0.0.1:" + server.getAddress().getPort(),
                Duration.ofMillis(50), Duration.ofMillis(50));

            var result = preflight.validate(10L);

            assertThat(result.valid()).isFalse();
            assertThat(result.violations()).extracting("key")
                .containsExactly("batch.preflight.response");
        } finally {
            server.stop(0);
        }
    }
}
