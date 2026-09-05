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
            sender.send(notification(), "41:1");
            throw new AssertionError("보내기가 성공해 버렸습니다. status=" + httpStatus);
        } catch (NotificationSendException failure) {
            return failure.reason();
        }
    }

    @Test
    @DisplayName("2xx 는 성공이다")
    void successOnTwoHundred() {
        status.set(202);

        assertThatCode(() -> sender().send(notification(), "41:1")).doesNotThrowAnyException();
    }

    /**
     * <b>재시도 가능/불가가 상태 코드 번호가 아니라 원인으로 갈린다.</b>
     * {@code 429} 가 4xx 인데 재시도 가능인 것이 그 증거다 — 받는 쪽 사정이라 잠시 뒤에는
     * 다르다.
     */
    @Test
    @DisplayName("5xx·429·408 은 재시도 가능, 400·422·403 은 불가")
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

        status.set(408);
        assertThat(reasonOf(408, sender).retryable())
                .as("408 을 기본 4xx 갈래로 흘리면 잠깐 느렸던 것을 영영 포기한다")
                .isTrue();
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
     * <b>키를 발송기가 만들지 않는다.</b> 무엇이 "같은 발송" 인지는 {@code Notification}
     * 하나로 알 수 없다 — 자동 재시도가 {@code attemptCount} 를 올리므로 그것으로 키를
     * 만들면 <b>재시도마다 키가 바뀌어</b> 받는 쪽이 못 합친다. 그 경계를 아는 배달 판정이
     * 만들어 넘기고, 발송기는 <b>그대로 싣기만</b> 한다.
     */
    @Test
    @DisplayName("받은 멱등 키를 그대로 보낸다 — 스스로 만들지 않는다")
    void sendsTheGivenIdempotencyKey() {
        HttpNotificationSender sender = sender();

        sender.send(notification(), "41:1");
        sender.send(notification(), "41:1");

        assertThat(idempotencyKeys)
                .as("받은 키를 그대로 보내야 받는 쪽이 합칠 수 있다")
                .containsExactly("41:1", "41:1");
    }

    /** 본문에 수신처와 메시지가 실린다 — <b>여기서만</b> 그렇다. 예외에는 안 실린다. */
    @Test
    @DisplayName("본문은 보내되 실패 예외에는 수신처가 안 실린다")
    void carriesTheRecipientInTheBodyButNotInFailures() {
        HttpNotificationSender sender = sender();
        sender.send(notification(), "41:1");
        assertThat(bodies.getFirst()).contains("member:20", "coupon-issued:100");

        status.set(500);
        assertThatThrownBy(() -> sender.send(notification(), "41:1"))
                .hasMessageNotContaining("member:20")
                .hasMessageNotContaining("coupon-issued:100");
    }

    /**
     * <b>한 건이 아니라 한 묶음을 재 본다.</b> 한 번의 {@code poll} 이 기본 500 건을
     * 가져오고 그것을 <b>차례로</b> 보내므로, 건당 상한이 아무리 그럴듯해도
     * {@code 500 × 건당} 이 {@code max.poll.interval.ms}(기본 5분)를 넘으면 소비자가
     * 그룹에서 쫓겨나고 <b>그 묶음이 통째로 재전달된다</b> — 이미 보낸 것까지 다시 보낸다.
     *
     * <p>⚠️ 두 번 틀렸다. 처음엔 <b>릴레이의 건당 발행 예산(100ms)</b>과 비교했는데 축이
     * 달랐다 — 그 예산은 릴레이가 Kafka 로 발행하는 시간이고 이 발송기는 소비자 쪽에서
     * 돈다. 고쳐 놓은 10초는 <b>한 건만 보고 손으로 고른 값</b>이라 500 건이면 83 분이 되어
     * 5 분 한계를 훌쩍 넘겼다. 둘 다 리뷰가 짚었다. 지금 값은 유도한 것이다 —
     * {@code 300,000 / 500 = 600ms}.
     */
    @Test
    @DisplayName("초 단위 타임아웃은 기동에서 거절한다 — 500건 묶음이 5분을 넘긴다")
    void refusesTimeoutsThatCannotFinishAPollBatch() {
        assertThatThrownBy(() -> new HttpNotificationSender("http://notify.test/send",
                Duration.ofSeconds(1), Duration.ofSeconds(3)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 각각은 상한 안인데 <b>합이</b> 넘는 경우도 막아야 한다 — 실제로 도는 건 합이다. */
    @Test
    @DisplayName("각각은 상한 안이어도 합이 넘으면 거절한다")
    void refusesTimeoutsWhoseSumExceedsTheCap() {
        assertThatThrownBy(() -> new HttpNotificationSender("http://notify.test/send",
                Duration.ofMillis(400), Duration.ofMillis(400)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max.poll.interval.ms");
    }

    /** 상한 안의 값은 통과해야 한다 — 방어선이 성능을 정하는 자리가 아니다. */
    @Test
    @DisplayName("유도한 상한 안의 타임아웃은 통과한다")
    void acceptsTimeoutsWithinTheDerivedCap() {
        assertThatCode(() -> new HttpNotificationSender("http://notify.test/send",
                Duration.ofMillis(200), Duration.ofMillis(400)))
                .doesNotThrowAnyException();
    }

    /**
     * <b>합만 보면 넘침으로 상한을 그냥 지나간다.</b> {@code toMillis()} 둘을 더해
     * {@code long} 이 넘치면 <b>음수</b>가 되고, 음수는 어떤 상한보다도 작아서 검사를
     * 통과한다 — 상한이 있는데 없는 것과 같다. 리뷰가 짚었다.
     *
     * <p>그래서 <b>더하기 전에 각각</b>을 먼저 상한에 건다. 이 테스트는 그 순서를 지킨다:
     * 각 항 검사를 지우면 여기서 넘침이 통과해 버린다(돌연변이로 확인했다).
     */
    @Test
    @DisplayName("타임아웃 합이 long 을 넘겨도 상한을 우회하지 못한다")
    void refusesTimeoutsWhoseSumOverflowsLong() {
        assertThatThrownBy(() -> new HttpNotificationSender("http://notify.test/send",
                Duration.ofMillis(Long.MAX_VALUE), Duration.ofMillis(Long.MAX_VALUE)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>제어문자가 본문을 깨뜨리면 안 된다.</b> 메시지에 줄바꿈이 하나만 들어와도
     * 깨진 JSON 이 되어 받는 쪽이 400 을 내고, 그러면 <b>재시도 불가로 읽혀 그 알림이
     * {@code DEAD} 로 간다.</b> 사용자 문구에 줄바꿈은 흔하다.
     */
    @Test
    @DisplayName("줄바꿈이 든 메시지도 유효한 JSON 으로 나간다")
    void escapesControlCharacters() {
        Notification withNewline = new Notification(41L, 10L, 20L, 100L,
                Notification.DEFAULT_CHANNEL, NotificationStatus.PENDING, 2, 0, null,
                "member:20", "첫 줄\n둘째 줄\t끝", Instant.parse("2026-09-05T00:00:00Z"),
                Instant.parse("2026-09-05T00:00:00Z"), null, null);

        sender().send(withNewline, "41:1");

        String body = bodies.getFirst();
        assertThat(body)
                .as("원문 줄바꿈이 들어가면 JSON 이 깨진다")
                .doesNotContain("\n둘째")
                .contains("\\n", "\\t");
    }

    /** 잘못된 endpoint 를 두면 <b>모든 알림이 매번 실패</b>하다 전부 종착한다. */
    @Test
    @DisplayName("http(s) 가 아니거나 호스트가 없는 endpoint 는 기동에서 거절한다")
    void refusesAnEndpointThatIsNotHttp() {
        assertThatThrownBy(() -> new HttpNotificationSender("notify.test/send",
                Duration.ofMillis(30), Duration.ofMillis(60)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HttpNotificationSender("ftp://notify.test/send",
                Duration.ofMillis(30), Duration.ofMillis(60)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** {@code 0} 은 "무한" 이 아니라 즉시 실패다 — 그대로 두면 모든 알림이 타임아웃된다. */
    @Test
    @DisplayName("0 이나 음수 타임아웃은 기동에서 거절한다")
    void refusesNonPositiveTimeouts() {
        assertThatThrownBy(() -> new HttpNotificationSender("http://notify.test/send",
                Duration.ZERO, Duration.ofMillis(60)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HttpNotificationSender("http://notify.test/send",
                Duration.ofMillis(30), Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
