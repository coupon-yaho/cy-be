package com.kafkick.infra.mq.notification;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.Objects;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.kafkick.core.notification.NotificationSendException;
import com.kafkick.core.notification.NotificationSender;
import com.kafkick.core.notification.domain.Notification;
import com.kafkick.core.notification.domain.NotifyFailureReason;

/**
 * 알림을 <b>실제로 밖으로 보낸다.</b> outbox 가 나르던 것이 여기서 처음으로 도착한다.
 *
 * <h2>왜 이 클래스가 생겼나</h2>
 *
 * <p>CY-901~913 이 발행 파이프라인을 다듬는 동안 그 끝은 {@link MockNotificationSender} —
 * {@code null} 검사 한 줄 — 이었다. <b>끝이 안 이어져 있으면 그 앞의 전부가 연습이다.</b>
 * outbox 가 푸는 문제(dual-write)는 <i>"보낸 것이 유실되면 안 된다"</i> 인데, 보내는 것이
 * 없으면 유실될 것도 없다.
 *
 * <h2>타임아웃이 lease 와 묶여 있다</h2>
 *
 * <p><b>안 걸면 워커가 무한히 붙잡힌다.</b> 그러면 그 클레임의 lease 가 만료되고 다른
 * 워커가 <b>같은 알림을 다시 보낸다</b> — 릴레이가 기동 시 검사하는
 * {@code ceil(maxInFlight / workerCount) × 건당 예산 < lease} 관계가 그것이다(CY-906).
 * 그래서 여기 타임아웃은 <b>그 예산 안에 있어야 하고</b>, 기동 시 검사한다.
 *
 * <h2>at-least-once 는 우리가 못 막는다</h2>
 *
 * <p>outbox 도 Kafka 도 at-least-once 라 <b>같은 알림이 두 번 갈 수 있다.</b> 그것을 막는
 * 것은 <b>받는 쪽</b>이고, 우리가 할 일은 <b>같은 시도에 같은 키를 주는 것</b>이다.
 *
 * <p>키는 {@code notificationId:attemptCount} 다. 같은 시도가 재전달되면 같은 키이고,
 * 사람이 재발송해 <b>새 시도</b>가 되면 다른 키다 — 그것은 실제로 다시 보내야 하는 건이라
 * 합쳐지면 안 된다. Stripe·IETF 의 {@code Idempotency-Key} 가 같은 자리를 쓴다.
 *
 * <h2>실패를 재시도 가능 여부로 옮긴다</h2>
 *
 * <p>{@link NotifyFailureReason} 이 이미 그 축으로 갈려 있다. 여기서 하는 일은
 * <b>HTTP 응답을 그 축으로 옮기는 것</b>뿐이다 — 판정을 새로 만들지 않는다.
 *
 * <p>⚠️ <b>응답 본문을 예외에도 로그에도 안 싣는다.</b> 수신처 정보가 섞여 올 수 있고,
 * 이 예외 메시지는 지표 태그와 로그로 흘러간다.
 */
@Component
@ConditionalOnProperty("notification.sender.http.enabled")
public class HttpNotificationSender implements NotificationSender {

    /**
     * 건당 발행 예산. <b>릴레이의 상수와 같은 값이어야 한다</b> —
     * {@code NotificationOutboxRelay.PER_ITEM_PUBLISH_BUDGET_MILLIS} 가 lease 검사에 쓰는
     * 그것이다. 여기 타임아웃이 그보다 크면 <b>워커가 예산을 넘겨 붙잡고</b>, 뒤쪽 행이
     * 처리 전에 회수되어 중복 발행이 된다.
     *
     * <p>두 벌로 두는 것이 마음에 안 들지만, 릴레이 상수는 {@code private} 이고 이 클래스는
     * 릴레이를 몰라도 되는 자리다. 대신 <b>기동 시 검사</b>가 둘이 갈리는 것을 막는다.
     */
    private static final long PER_ITEM_BUDGET_MILLIS = 100;

    private final HttpClient http;
    private final URI endpoint;
    private final Duration requestTimeout;

    /**
     * <p><b>기본값은 30ms + 60ms = 90ms</b> 로 예산(100ms) 안이다. 처음에 50+80=130 을
     * 적었다가 <b>이 검사에 스스로 걸렸다</b> — 그것이 이 검사가 일한다는 증거다.
     *
     * @throws IllegalArgumentException 타임아웃이 <b>건당 예산을 넘을 때</b>. 기동을 막는
     *         것이 목적이다 — 넘긴 채로 돌면 lease 만료로 <b>같은 알림이 두 번 간다</b>
     */
    public HttpNotificationSender(
            @Value("${notification.sender.http.endpoint}") String endpoint,
            @Value("${notification.sender.http.connect-timeout:30ms}") Duration connectTimeout,
            @Value("${notification.sender.http.request-timeout:60ms}") Duration requestTimeout) {
        this.endpoint = URI.create(Objects.requireNonNull(endpoint, "endpoint"));
        this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
        long total = connectTimeout.toMillis() + requestTimeout.toMillis();
        if (total > PER_ITEM_BUDGET_MILLIS) {
            throw new IllegalArgumentException(
                    "연결+요청 타임아웃 합(" + total + "ms)이 건당 발행 예산("
                            + PER_ITEM_BUDGET_MILLIS + "ms)을 넘습니다. 그대로 두면 워커가 "
                            + "lease 를 태워 같은 알림이 두 번 나갑니다.");
        }
        this.http = HttpClient.newBuilder().connectTimeout(connectTimeout).build();
    }

    @Override
    public void send(Notification notification) {
        Objects.requireNonNull(notification, "notification");
        HttpResponse<Void> response;
        try {
            response = http.send(request(notification), HttpResponse.BodyHandlers.discarding());
        } catch (HttpTimeoutException timeout) {
            throw new NotificationSendException(NotifyFailureReason.SEND_TIMEOUT, timeout);
        } catch (IOException unreachable) {
            throw new NotificationSendException(NotifyFailureReason.CONNECTION_ERROR, unreachable);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new NotificationSendException(NotifyFailureReason.SEND_UNAVAILABLE, interrupted);
        }
        reject(response.statusCode());
    }

    /**
     * 응답 본문을 <b>버린다</b>({@code discarding}). 수신처 정보가 섞여 올 수 있고, 우리가
     * 그것으로 할 일이 없다 — 성공 여부는 상태 코드가 말한다.
     */
    private HttpRequest request(Notification notification) {
        return HttpRequest.newBuilder(endpoint)
                .timeout(requestTimeout)
                .header("Content-Type", "application/json")
                // 같은 시도가 재전달되면 같은 키다. 받는 쪽이 그것으로 합친다.
                .header("Idempotency-Key",
                        notification.id() + ":" + notification.attemptCount())
                .POST(HttpRequest.BodyPublishers.ofString(body(notification)))
                .build();
    }

    /**
     * <b>4xx 는 다시 보내도 같다.</b> 요청이 잘못됐거나 받는 쪽이 거절한 것이라, 재시도는
     * 같은 실패를 반복하며 {@code failure_count} 만 올린다 — 결국 {@code DEAD} 로 가는데
     * 그 시간 동안 워커를 쓴다.
     *
     * <p><b>5xx 와 429 는 다시 보내면 될 수 있다.</b> 받는 쪽 사정이라 잠시 뒤에는 다르다.
     * {@code 429} 가 4xx 인데 재시도 가능인 것이 그 축이 <b>상태 코드 번호가 아니라
     * 원인</b>이라는 증거다.
     */
    private static void reject(int status) {
        if (status >= 200 && status < 300) {
            return;
        }
        if (status == 429 || status >= 500) {
            throw new NotificationSendException(NotifyFailureReason.SEND_UNAVAILABLE,
                    new IllegalStateException("HTTP " + status));
        }
        if (status == 400 || status == 422) {
            throw new NotificationSendException(NotifyFailureReason.INVALID_RECIPIENT,
                    new IllegalStateException("HTTP " + status));
        }
        throw new NotificationSendException(NotifyFailureReason.REJECTED_BY_PROVIDER,
                new IllegalStateException("HTTP " + status));
    }

    /**
     * <b>수신처와 본문을 그대로 싣는다 — 그것이 이 요청의 내용이다.</b> 다만 <b>여기서만</b>
     * 그렇다: 예외·로그·지표에는 안 간다.
     */
    private static String body(Notification notification) {
        return """
                {"notificationId":%d,"memberId":%d,"couponId":%d,"channel":"%s",\
                "recipient":"%s","message":"%s"}"""
                .formatted(notification.id(), notification.memberId(), notification.couponId(),
                        escape(notification.channel()), escape(notification.recipientContact()),
                        escape(notification.messageBody()));
    }

    /**
     * 큰따옴표와 역슬래시만 막는다. <b>JSON 라이브러리를 안 쓰는 이유</b> — 이 모듈이
     * Jackson 을 이미 물고 있지만, 그것은 <b>이벤트 직렬화용이고 core 의 골든 픽스처와 같은
     * 매퍼를 타야 한다</b>({@code infra/mq/build.gradle} 주석). 발송 본문이 그 매퍼 설정에
     * 딸려 바뀌면 <b>받는 쪽 계약이 우리 모르게 흔들린다.</b>
     */
    private static String escape(String value) {
        return value == null ? "" : value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
