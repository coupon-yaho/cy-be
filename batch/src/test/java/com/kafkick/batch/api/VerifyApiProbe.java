// 업무 포트의 검증 API 에 HTTP 로 물어보는 도구입니다.
package com.kafkick.batch.api;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.function.BooleanSupplier;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * {@code ActuatorProbe} 와 같은 이유로 JDK 클라이언트다 — Boot 4 에는
 * {@code TestRestTemplate} 이 없고, 이 검사가 보려는 것이 프레임워크가 아니라 <b>HTTP 표면</b>이다.
 *
 * <p>{@code WebTestClient} 를 쓰지 않는 것은 batch 에 webflux 의존이 없기 때문이다.
 * 테스트 하나를 위해 모듈 의존을 늘리면 <b>운영 클래스패스에도 그것이 올라간다.</b>
 */
final class VerifyApiProbe {

    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    // Boot 4 는 Jackson 3(tools.jackson)이다. java.time 은 코어에 들어와 모듈 등록이 없다.
    private static final JsonMapper MAPPER = JsonMapper.builder()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private final int port;

    VerifyApiProbe(int port) {
        this.port = port;
    }

    HttpResponse<String> post(String path) throws IOException, InterruptedException {
        return CLIENT.send(request(path).POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    HttpResponse<String> get(String path) throws IOException, InterruptedException {
        return CLIENT.send(request(path).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    static JsonNode json(HttpResponse<String> response) {
        return MAPPER.readTree(response.body());
    }

    static <T> T body(HttpResponse<String> response, Class<T> type) {
        return MAPPER.readValue(response.body(), type);
    }

    /**
     * <b>잡이 비동기라 기다려야 한다.</b> awaitility 를 안 쓰는 것도 의존을 안 늘리기
     * 위해서다. 조건이 참이 되면 즉시 빠지고, 안 되면 {@code AssertionError} 로 끝난다 —
     * <b>조용히 통과하지 않는다.</b>
     */
    static void awaitUntil(Duration timeout, String what, BooleanSupplier condition)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError(timeout.toSeconds() + "초 안에 " + what);
    }

    private HttpRequest.Builder request(String path) {
        return HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(30));
    }
}
