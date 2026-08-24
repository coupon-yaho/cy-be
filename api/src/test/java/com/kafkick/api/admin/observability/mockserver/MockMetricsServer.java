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
import com.kafkick.core.support.exception.CommonErrorCode;

public final class MockMetricsServer {

    private static final String METRICS_PATH = "/api/v1/admin/metrics";
    private static final String EVENTS_PATH = "/api/v1/admin/events";
    private static final Set<String> SCENARIOS =
            Set.of("", "loaded", "idle", "stale", "promDown", "budget");
    private static final Duration STALE_AFTER = Duration.ofSeconds(120);
    private static final Duration RESPONSE_BUDGET = Duration.ofMillis(500);

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
        try {
            addCors(exchange);
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
                return;
            }

            String path = exchange.getRequestURI().getPath();
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendFailure(exchange, CommonErrorCode.METHOD_NOT_ALLOWED,
                        CommonErrorCode.METHOD_NOT_ALLOWED.getMessage());
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
            sendFailure(exchange, CommonErrorCode.NOT_FOUND, CommonErrorCode.NOT_FOUND.getMessage());
        } catch (Exception failure) {
            try {
                sendFailure(exchange, CommonErrorCode.INTERNAL_ERROR,
                        CommonErrorCode.INTERNAL_ERROR.getMessage());
            } catch (IOException ignored) {
            }
        }
    }

    private void handleMetrics(HttpExchange exchange) throws IOException {
        Map<String, String> parameters;
        try {
            parameters = queryParameters(exchange.getRequestURI());
        } catch (IllegalArgumentException invalidQuery) {
            sendFailure(exchange, CommonErrorCode.INVALID_INPUT, "잘못된 쿼리 파라미터입니다.");
            return;
        }
        String windowValue = parameters.get("window");
        if (windowValue == null || windowValue.isBlank()) {
            sendFailure(exchange, CommonErrorCode.INVALID_INPUT, "window는 필수입니다.");
            return;
        }

        MetricsWindow window = parseWindow(windowValue);
        if (window == null) {
            sendFailure(exchange, CommonErrorCode.INVALID_INPUT, "지원하지 않는 window입니다: " + windowValue);
            return;
        }

        Long couponId = parsePositiveLong(parameters.get("couponId"));
        Long benchmarkRunId = parsePositiveLong(parameters.get("benchmarkRunId"));
        if ((parameters.containsKey("couponId") && couponId == null)
                || (parameters.containsKey("benchmarkRunId") && benchmarkRunId == null)) {
            sendFailure(exchange, CommonErrorCode.INVALID_INPUT, "조회 식별자는 양수여야 합니다.");
            return;
        }
        if (couponId != null && benchmarkRunId != null) {
            sendFailure(exchange, CommonErrorCode.INVALID_INPUT,
                    "couponId와 benchmarkRunId는 동시에 지정할 수 없습니다.");
            return;
        }

        String scenario = parameters.getOrDefault("scenario", "");
        if (!SCENARIOS.contains(scenario)) {
            sendFailure(exchange, CommonErrorCode.INVALID_INPUT, "지원하지 않는 scenario입니다: " + scenario);
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

    private void sendFailure(HttpExchange exchange, CommonErrorCode errorCode, String message)
            throws IOException {
        ErrorResponse error = new ErrorResponse(
                errorCode.getStatus(), errorCode.getCode(), message, null, null,
                requestId(exchange), Instant.now());
        send(exchange, errorCode.getStatus(), ResponseEnvelope.fail(error));
    }

    private void send(HttpExchange exchange, int status, Object body) throws IOException {
        try (exchange) {
            byte[] bytes = jsonMapper.writeValueAsBytes(body);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
        }
    }

    private static void addCors(HttpExchange exchange) {
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, OPTIONS");
        // 실서버의 CallerFilter 는 X-User-Id 만 읽고 나머지 헤더는 무시한다 — 거부하지 않는다.
        // 목이 허용 목록을 심사하면 실서버보다 엄격해져, 화면이 헤더를 하나 더 붙일 때마다 막힌다.
        String requested = exchange.getRequestHeaders().getFirst("Access-Control-Request-Headers");
        exchange.getResponseHeaders().set(
                "Access-Control-Allow-Headers",
                requested == null || requested.isBlank() ? "Content-Type, X-User-Id" : requested);
        exchange.getResponseHeaders().set("Access-Control-Max-Age", "600");
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
