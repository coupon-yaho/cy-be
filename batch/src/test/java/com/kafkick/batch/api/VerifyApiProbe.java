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

    /**
     * <b>봉투를 벗겨 {@code data} 를 준다.</b> 저장소 규약이 모든 응답을
     * {@code ResponseEnvelope} 로 감싸므로, 테스트가 매번 그 경로를 적지 않게 여기서 푼다.
     */
    static <T> T body(HttpResponse<String> response, Class<T> type) {
        return MAPPER.treeToValue(json(response).path("data"), type);
    }

    /**
     * <b>봉투를 벗겨 {@code data} 를 노드로 준다.</b> 위 {@code body} 는 타입으로 받는데,
     * 리포트처럼 <b>필드가 실렸는지 자체</b>를 재는 경우에는 타입 변환이 그 사실을 지운다 —
     * 없는 필드가 기본값으로 채워지기 때문이다.
     */
    static JsonNode data(HttpResponse<String> response) {
        return json(response).path("data");
    }

    /** 에러 응답의 {@code error} 노드. 규약상 {@code status}·{@code code}·{@code message} 가 있다. */
    static JsonNode error(HttpResponse<String> response) {
        return json(response).path("error");
    }

    /**
     * <b>잡이 비동기라 기다려야 한다.</b> awaitility 를 안 쓰는 것도 의존을 안 늘리기
     * 위해서다. 조건이 참이 되면 즉시 빠지고, 안 되면 {@code AssertionError} 로 끝난다 —
     * <b>조용히 통과하지 않는다.</b>
     */
    static void awaitUntil(Duration timeout, String what, BooleanSupplier condition)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            if (condition.getAsBoolean()) {
                return;
            }
            // 데드라인 검사를 sleep **뒤**에 둔다. 앞에 두면 마지막 200ms 창에서 조건이
            // 참이 되어도 재평가 없이 실패한다 — CI 부하 시 간헐 실패의 전형이다.
            if (System.nanoTime() >= deadline) {
                throw new AssertionError(timeout.toSeconds() + "초 안에 " + what);
            }
            Thread.sleep(200);
        }
    }

    private HttpRequest.Builder request(String path) {
        return HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(30));
    }
}
