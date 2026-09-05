package com.kafkick.infra.mq.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.notification.NotificationSendException;
import com.kafkick.core.notification.domain.Notification;
import com.kafkick.core.notification.domain.NotificationStatus;
import com.kafkick.core.notification.domain.NotifyFailureReason;

/**
 * <b>HTTP 응답이 재시도 가능 여부로 옮겨지는지</b>를 진짜 서버에 태워 확인한다.
 *
 * <p>이 매핑이 틀리는 방향은 전부 <b>조용하다</b> — 재시도 불가를 가능으로 읽으면 같은
 * 실패를 열 번 반복하다 {@code DEAD} 로 가고, 반대면 <b>잠깐 흔들린 것을 영영 포기한다.</b>
 * 둘 다 예외가 안 나고 지표에서만 이상하게 보인다.
 *
 * <p><b>대역이 아니라 실제 서버다.</b> 타임아웃은 대역으로 못 잰다 — 클라이언트가 정말
 * 그 시간에 끊는지는 <b>느리게 답하는 상대</b>가 있어야 안다.
 */
class HttpNotificationSenderTest {

    private HttpServer server;
    private final AtomicInteger status = new AtomicInteger(200);
    private final AtomicInteger delayMillis = new AtomicInteger(0);
    private final List<String> idempotencyKeys = new ArrayList<>();
    private final List<String> bodies = new ArrayList<>();

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/send", this::handle);
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    private void handle(HttpExchange exchange) throws IOException {
        idempotencyKeys.add(exchange.getRequestHeaders().getFirst("Idempotency-Key"));
        bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
        try {
            Thread.sleep(delayMillis.get());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        exchange.sendResponseHeaders(status.get(), -1);
        try (OutputStream ignored = exchange.getResponseBody()) {
            // 본문 없음. 발송기가 본문을 버리는지도 여기서 함께 확인된다.
        }
    }

    private HttpNotificationSender sender() {
        URI base = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/send");
        return new HttpNotificationSender(base.toString(),
                Duration.ofMillis(30), Duration.ofMillis(60));
    }

    private static Notification notification() {
        return new Notification(41L, 10L, 20L, 100L, Notification.DEFAULT_CHANNEL,
                NotificationStatus.PENDING, 2, 0, null, "member:20", "coupon-issued:100",
                Instant.parse("2026-09-05T00:00:00Z"), Instant.parse("2026-09-05T00:00:00Z"),
                null, null);
    }

    private static NotifyFailureReason reasonOf(int httpStatus, HttpNotificationSender sender) {
        try {
            sender.send(notification());
            throw new AssertionError("보내기가 성공해 버렸습니다. status=" + httpStatus);
        } catch (NotificationSendException failure) {
            return failure.reason();
        }
    }

    @Test
    @DisplayName("2xx 는 성공이다")
    void successOnTwoHundred() {
        status.set(202);

        assertThatCode(() -> sender().send(notification())).doesNotThrowAnyException();
    }

    /**
     * <b>재시도 가능/불가가 상태 코드 번호가 아니라 원인으로 갈린다.</b>
     * {@code 429} 가 4xx 인데 재시도 가능인 것이 그 증거다 — 받는 쪽 사정이라 잠시 뒤에는
     * 다르다.
     */
    @Test
    @DisplayName("5xx 와 429 는 재시도 가능, 400·422 는 불가")
    void mapsStatusToRetryability() {
        HttpNotificationSender sender = sender();

        status.set(503);
        assertThat(reasonOf(503, sender).retryable()).isTrue();
        status.set(429);
        assertThat(reasonOf(429, sender).retryable())
                .as("429 는 4xx 지만 받는 쪽 사정이라 다시 보내면 될 수 있다")
                .isTrue();

        status.set(400);
        assertThat(reasonOf(400, sender).retryable())
                .as("다시 보내도 같은 실패다 — 반복하면 failure_count 만 올라 DEAD 로 간다")
                .isFalse();
        status.set(422);
        assertThat(reasonOf(422, sender).retryable()).isFalse();
        status.set(403);
        assertThat(reasonOf(403, sender).retryable()).isFalse();
    }

    /**
     * <b>타임아웃을 안 걸면 워커가 무한히 붙잡히고 lease 가 만료된다</b> — 그러면 다른
     * 워커가 같은 알림을 다시 보낸다(CY-906 이 그 관계를 기동 시 검사한다).
     * 그래서 <b>정말 끊는지</b>를 느리게 답하는 서버로 잰다.
     */
    @Test
    @DisplayName("느린 상대에서 타임아웃으로 끊고 재시도 가능으로 읽는다")
    void timesOutAndTreatsItAsRetryable() {
        delayMillis.set(500);
        HttpNotificationSender sender = sender();

        long started = System.nanoTime();
        NotifyFailureReason reason = reasonOf(200, sender);
        long elapsedMillis = (System.nanoTime() - started) / 1_000_000L;

        assertThat(reason).isEqualTo(NotifyFailureReason.SEND_TIMEOUT);
        assertThat(reason.retryable()).isTrue();
        assertThat(elapsedMillis)
                .as("타임아웃(60ms)보다 한참 늦게 끊으면 워커가 예산을 넘겨 붙잡는다")
                .isLessThan(400);
    }

    /**
     * <b>같은 시도는 같은 키다.</b> outbox 도 Kafka 도 at-least-once 라 같은 알림이 두 번
     * 갈 수 있고, 그것을 합치는 것은 <b>받는 쪽</b>이다 — 우리가 할 일은 키를 일관되게
     * 주는 것뿐이다.
     */
    @Test
    @DisplayName("멱등 키가 notificationId:attemptCount 이고 재전달에도 같다")
    void sendsAStableIdempotencyKey() {
        HttpNotificationSender sender = sender();

        sender.send(notification());
        sender.send(notification());

        assertThat(idempotencyKeys)
                .as("같은 시도가 두 번 가도 받는 쪽이 하나로 합칠 수 있어야 한다")
                .containsExactly("41:2", "41:2");
    }

    /** 본문에 수신처와 메시지가 실린다 — <b>여기서만</b> 그렇다. 예외에는 안 실린다. */
    @Test
    @DisplayName("본문은 보내되 실패 예외에는 수신처가 안 실린다")
    void carriesTheRecipientInTheBodyButNotInFailures() {
        HttpNotificationSender sender = sender();
        sender.send(notification());
        assertThat(bodies.getFirst()).contains("member:20", "coupon-issued:100");

        status.set(500);
        assertThatThrownBy(() -> sender.send(notification()))
                .hasMessageNotContaining("member:20")
                .hasMessageNotContaining("coupon-issued:100");
    }

    /**
     * <b>타임아웃 합이 건당 예산을 넘으면 기동을 막는다.</b> 넘긴 채로 돌면 워커가 lease 를
     * 태워 <b>같은 알림이 두 번 나간다</b> — 운영 중 첫 지연에서야 드러날 일을 기동으로 당긴다.
     */
    @Test
    @DisplayName("타임아웃이 건당 예산을 넘으면 기동에서 거절한다")
    void refusesTimeoutsBeyondThePerItemBudget() {
        assertThatThrownBy(() -> new HttpNotificationSender("http://notify.test/send",
                Duration.ofMillis(50), Duration.ofMillis(80)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("두 번");
    }
}
