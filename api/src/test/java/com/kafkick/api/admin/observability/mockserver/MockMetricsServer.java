package com.kafkick.api.admin.observability.mockserver;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import tools.jackson.databind.json.JsonMapper;

import com.kafkick.api.admin.observability.PromMetricsAssembler;
import com.kafkick.api.admin.observability.dto.MetricsQuery;
import com.kafkick.api.admin.support.AdminApiErrorCode;
import com.kafkick.api.support.ErrorResponse;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.support.TimeProvider;

public final class MockMetricsServer {

    private static final String METRICS_PATH = "/api/v1/admin/metrics";
    private static final String EVENTS_PATH = "/api/v1/admin/events";
    private static final Set<String> SCENARIOS =
            Set.of("", "loaded", "idle", "stale", "promDown", "budget");
    private static final Duration STALE_AFTER = Duration.ofSeconds(120);
    private static final Duration RESPONSE_BUDGET = Duration.ofMillis(80);

    private final JsonMapper jsonMapper = MockJsonMapper.create();
    private final long startedAtNanos = System.nanoTime();

    private MockMetricsServer() {
    }

    public static void main(String[] args) throws IOException {
        int port = resolvePort(args);
        MockMetricsServer application = new MockMetricsServer();
        HttpServer server = HttpServer.create(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 0);
        server.createContext("/", application::handle);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        System.out.println("Mock metrics server listening on http://localhost:" + port);
    }

    private void handle(HttpExchange exchange) throws IOException {
        addCors(exchange);
        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
            return;
        }

        String path = exchange.getRequestURI().getPath();
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendFailure(exchange, 404, "MOCK-404", "요청 경로를 찾을 수 없습니다.");
            return;
        }
        if (METRICS_PATH.equals(path)) {
            handleMetrics(exchange);
            return;
        }
        if (EVENTS_PATH.equals(path)) {
            ErrorResponse error = ErrorResponse.of(
                    AdminApiErrorCode.NOT_IMPLEMENTED, requestId(exchange), Instant.now());
            send(exchange, 501, ResponseEnvelope.fail(error));
            return;
        }
        sendFailure(exchange, 404, "MOCK-404", "요청 경로를 찾을 수 없습니다.");
    }

    private void handleMetrics(HttpExchange exchange) throws IOException {
        Map<String, String> parameters = queryParameters(exchange.getRequestURI());
        String windowValue = parameters.get("window");
        if (windowValue == null || windowValue.isBlank()) {
            sendFailure(exchange, 400, "MOCK-400", "window는 필수입니다.");
            return;
        }

        MetricsWindow window = parseWindow(windowValue);
        if (window == null) {
            sendFailure(exchange, 400, "MOCK-400", "지원하지 않는 window입니다: " + windowValue);
            return;
        }

        Long couponId = parsePositiveLong(parameters.get("couponId"));
        Long benchmarkRunId = parsePositiveLong(parameters.get("benchmarkRunId"));
        if ((parameters.containsKey("couponId") && couponId == null)
                || (parameters.containsKey("benchmarkRunId") && benchmarkRunId == null)) {
            sendFailure(exchange, 400, "MOCK-400", "조회 식별자는 양수여야 합니다.");
            return;
        }
        if (couponId != null && benchmarkRunId != null) {
            sendFailure(exchange, 400, "MOCK-400",
                    "couponId와 benchmarkRunId는 동시에 지정할 수 없습니다.");
            return;
        }

        String scenario = parameters.getOrDefault("scenario", "");
        if (!SCENARIOS.contains(scenario)) {
            sendFailure(exchange, 400, "MOCK-400", "지원하지 않는 scenario입니다: " + scenario);
            return;
        }

        MockPromQuery promQuery = new MockPromQuery(scenario, startedAtNanos);
        PromMetricsAssembler assembler = new PromMetricsAssembler(
                promQuery,
                new TimeProvider(Clock.systemUTC()),
                STALE_AFTER,
                RESPONSE_BUDGET);
        send(exchange, 200, ResponseEnvelope.success(
                assembler.assemble(new MetricsQuery(window, couponId, benchmarkRunId))));
    }

    private void sendFailure(HttpExchange exchange, int status, String code, String message)
            throws IOException {
        ErrorResponse error = new ErrorResponse(
                status, code, message, null, requestId(exchange), Instant.now());
        send(exchange, status, ResponseEnvelope.fail(error));
    }

    private void send(HttpExchange exchange, int status, Object body) throws IOException {
        byte[] bytes = jsonMapper.writeValueAsBytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private static void addCors(HttpExchange exchange) {
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, OPTIONS");
        exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type, X-User-Id");
    }

    private static Map<String, String> queryParameters(URI uri) {
        Map<String, String> parameters = new HashMap<>();
        String rawQuery = uri.getRawQuery();
        if (rawQuery == null || rawQuery.isBlank()) {
            return parameters;
        }
        for (String pair : rawQuery.split("&")) {
            String[] parts = pair.split("=", 2);
            String key = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            String value = parts.length == 2
                    ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8)
                    : "";
            parameters.put(key, value);
        }
        return parameters;
    }

    private static MetricsWindow parseWindow(String value) {
        return switch (value) {
            case "1m", "ONE_MINUTE" -> MetricsWindow.ONE_MINUTE;
            case "5m", "FIVE_MINUTES" -> MetricsWindow.FIVE_MINUTES;
            case "15m", "FIFTEEN_MINUTES" -> MetricsWindow.FIFTEEN_MINUTES;
            default -> null;
        };
    }

    private static Long parsePositiveLong(String value) {
        if (value == null) {
            return null;
        }
        try {
            long parsed = Long.parseLong(value);
            return parsed > 0 ? parsed : null;
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static String requestId(HttpExchange exchange) {
        return "mock-" + Integer.toUnsignedString(System.identityHashCode(exchange));
    }

    private static int resolvePort(String[] args) {
        String value = System.getenv("MOCK_PORT");
        for (int index = 0; index < args.length; index++) {
            if ("--port".equals(args[index]) && index + 1 < args.length) {
                value = args[++index];
            } else if (args[index].startsWith("--port=")) {
                value = args[index].substring("--port=".length());
            }
        }
        if (value == null || value.isBlank()) {
            return 18080;
        }
        int port = Integer.parseInt(value);
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("port는 1~65535 범위여야 합니다: " + port);
        }
        return port;
    }
}
