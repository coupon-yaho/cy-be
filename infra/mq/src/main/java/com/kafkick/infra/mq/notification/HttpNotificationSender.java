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
import com.kafkick.infra.mq.config.KafkaConsumerGroups;

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
 * <h2>타임아웃이 poll 예산과 묶여 있다</h2>
 *
 * <p><b>안 걸면 소비자가 무한히 붙잡힌다.</b> 이 발송기는 Kafka 소비자 스레드에서 돌고,
 * 한 번의 {@code poll} 이 가져온 <b>여러 건을 차례로</b> 보낸다. 그 묶음이
 * {@code max.poll.interval.ms} 안에 안 끝나면 소비자가 그룹에서 쫓겨나고
 * <b>묶음이 통째로 재전달된다</b> — 이미 보낸 것까지 다시 보낸다. 그래서 건당 타임아웃은
 * {@link #MAX_TOTAL_TIMEOUT_MILLIS} 안에 있어야 하고, <b>기동 시 검사한다.</b>
 *
 * <p>⚠️ 한때 이 자리에 <b>릴레이의 lease 예산</b>(CY-906 의
 * {@code ceil(maxInFlight / workerCount) × 건당 예산 < lease})이 적혀 있었는데 <b>축이
 * 다르다.</b> 그 예산은 릴레이가 클레임을 쥔 채 Kafka 로 <i>발행</i>하는 시간이고, 이
 * 발송기는 그 뒤 소비자 쪽에서 돈다. 리뷰가 짚었다.
 *
 * <h2>at-least-once 는 우리가 못 막는다</h2>
 *
 * <p>outbox 도 Kafka 도 at-least-once 라 <b>같은 알림이 두 번 갈 수 있다.</b> 그것을 막는
 * 것은 <b>받는 쪽</b>이고, 우리가 할 일은 <b>같은 시도에 같은 키를 주는 것</b>이다.
 *
 * <p><b>키를 여기서 만들지 않는다.</b> 무엇이 "같은 발송" 인지는 {@code Notification}
 * 하나로 알 수 없다 — 자동 재시도는 같은 발송이고 사람의 재발송은 다른 발송인데, 그
 * 경계를 아는 것은 배달 판정({@code NotificationDeliveryDecision#idempotencyKey})이다.
 * 그쪽이 {@code notificationId:baseAttemptSeq} 로 만들어 넘기고, 이 클래스는 <b>받은 것을
 * 그대로 헤더에 싣기만</b> 한다. Stripe·IETF 의 {@code Idempotency-Key} 가 같은 자리를 쓴다.
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
     * 한 건에 허용하는 시간의 절대 상한 — <b>손으로 고르지 않고 유도한다.</b>
     *
     * <p>한 묶음의 레코드를 <b>차례로</b> 보내므로 최악은
     * {@code max.poll.records × 건당 시간} 이다. 그것이 {@code max.poll.interval.ms} 를
     * 넘기면 소비자가 그룹에서 쫓겨나고 그 묶음이 통째로 재전달된다 —
     * <b>이미 보낸 것까지 다시 보낸다.</b>
     *
     * <p>⚠️ 처음엔 10초를 <b>손으로</b> 적었다. 한 건만 보고 고른 값이라
     * <b>500건 × 10초 = 83분</b> 이 되어 5분 한계를 훌쩍 넘겼다 — 리뷰가 짚었다.
     * 지금은 {@link KafkaConsumerGroups} 가 못박은 두 값에서 계산한다
     * ({@code 300,000 / 500 = 600ms}). 컨슈머 설정을 바꾸면 <b>여기가 따라온다</b> —
     * 숫자를 박아 두면 그 관계가 끊긴다.
     */
    private static final long MAX_TOTAL_TIMEOUT_MILLIS =
            KafkaConsumerGroups.MAX_POLL_INTERVAL_MILLIS / KafkaConsumerGroups.MAX_POLL_RECORDS;

    private final HttpClient http;
    private final URI endpoint;
    private final Duration requestTimeout;

    /**
     * @throws IllegalArgumentException 아래 셋 중 하나일 때. <b>전부 빈 생성에서 터지므로
     *         기동이 거부된다</b> — 잘못 설정된 발송기가 조용히 도는 것보다 낫다
     *         <ul>
     *           <li>{@code endpoint} 가 {@code http}·{@code https} 가 아니거나 호스트가 없을 때 —
     *               그대로 두면 <b>모든 알림이 매번 실패</b>하고, 4xx 도 5xx 도 아니라
     *               {@code CONNECTION_ERROR} 로 재시도되다 전부 {@code DEAD} 로 간다</li>
     *           <li>타임아웃이 0 이거나 음수일 때 — 0 은 "무한" 이 아니라 즉시 실패다</li>
     *           <li>어느 하나가, 또는 합이 {@link #MAX_TOTAL_TIMEOUT_MILLIS} 를 넘을 때 —
     *               <b>한 묶음이 poll 간격 안에 못 끝난다</b></li>
     *         </ul>
     */
    public HttpNotificationSender(
            @Value("${notification.sender.http.endpoint}") String endpoint,
            @Value("${notification.sender.http.connect-timeout:30ms}") Duration connectTimeout,
            @Value("${notification.sender.http.request-timeout:60ms}") Duration requestTimeout) {
        this.endpoint = requireHttpUri(Objects.requireNonNull(endpoint, "endpoint"));
        this.requestTimeout = requireWithinCap(requestTimeout, "요청");
        requireWithinCap(connectTimeout, "연결");
        // **각각을 먼저 상한 안으로 거른 뒤 더한다.** 그냥 더하면 큰 값 둘에서 long 이
        // 넘쳐 음수가 되고, 그 음수가 상한 검사를 통과한다(리뷰가 짚었다).
        long total = connectTimeout.toMillis() + requestTimeout.toMillis();
        if (total > MAX_TOTAL_TIMEOUT_MILLIS) {
            throw new IllegalArgumentException(
                    "연결+요청 타임아웃 합(" + total + "ms)이 상한("
                            + MAX_TOTAL_TIMEOUT_MILLIS + "ms)을 넘습니다. 한 묶음("
                            + KafkaConsumerGroups.MAX_POLL_RECORDS + "건)을 차례로 보내면 "
                            + "max.poll.interval.ms(" + KafkaConsumerGroups.MAX_POLL_INTERVAL_MILLIS
                            + "ms)를 넘겨 소비자가 그룹에서 쫓겨나고, 그 묶음이 통째로 "
                            + "재전달됩니다 — 이미 보낸 것까지 다시 보냅니다.");
        }
        this.http = HttpClient.newBuilder().connectTimeout(connectTimeout).build();
    }

    /**
     * @param notification 보낼 알림
     * @param idempotencyKey 배달 판정이 만든 키. <b>여기서 만들지 않는다</b>
     * @throws NullPointerException 인자가 {@code null} 일 때. <b>키가 없으면 받는 쪽이 중복을
     *         못 합치므로</b> 조용히 빈 키로 보내지 않고 그 자리에서 멈춘다
     * @throws NotificationSendException 보내지 못했을 때. {@code reason} 이 재시도 가능
     *         여부를 진다
     */
    @Override
    public void send(Notification notification, String idempotencyKey) {
        Objects.requireNonNull(notification, "notification");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey");
        HttpResponse<Void> response;
        try {
            response = http.send(request(notification, idempotencyKey),
                    HttpResponse.BodyHandlers.discarding());
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
    private HttpRequest request(Notification notification, String idempotencyKey) {
        return HttpRequest.newBuilder(endpoint)
                .timeout(requestTimeout)
                .header("Content-Type", "application/json")
                // 키는 배달 판정이 만든다 — 자동 재시도 사이에 안 변해야 하는데,
                // 여기서는 무엇이 "같은 발송" 인지 알 수 없다(NotificationSender javadoc).
                .header("Idempotency-Key", idempotencyKey)
                .POST(HttpRequest.BodyPublishers.ofString(body(notification)))
                .build();
    }

    /**
     * <b>4xx 는 다시 보내도 같다.</b> 요청이 잘못됐거나 받는 쪽이 거절한 것이라, 재시도는
     * 같은 실패를 반복하며 {@code failure_count} 만 올린다 — 결국 {@code DEAD} 로 가는데
     * 그 시간 동안 워커를 쓴다.
     *
     * <p><b>5xx·429·408 은 다시 보내면 될 수 있다.</b> 받는 쪽 사정이라 잠시 뒤에는 다르다.
     * 뒤의 둘이 4xx 인데 재시도 가능인 것이 그 축이 <b>상태 코드 번호가 아니라 원인</b>이라는
     * 증거다 — {@code 408 Request Timeout} 을 기본 4xx 갈래로 흘리면 <b>잠깐 느렸던 것을
     * 영영 포기한다</b>(리뷰가 짚었다).
     */
    private static void reject(int status) {
        if (status >= 200 && status < 300) {
            return;
        }
        if (status == 408 || status == 429 || status >= 500) {
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
     * JSON 문자열 이스케이프.
     *
     * <p><b>제어문자까지 막는다.</b> 처음에 큰따옴표와 역슬래시만 막았는데, 메시지에
     * 줄바꿈이 하나만 들어와도 <b>본문이 깨진 JSON 이 되어 받는 쪽이 400 을 낸다</b> —
     * 그러면 재시도 불가로 읽혀 그 알림이 {@code DEAD} 로 간다. 사용자 문구에 줄바꿈은
     * 흔하다. 리뷰가 짚었다.
     *
     * <p><b>JSON 라이브러리를 안 쓰는 이유</b> — 이 모듈이 Jackson 을 이미 물고 있지만,
     * 그것은 <b>이벤트 직렬화용이고 core 의 골든 픽스처와 같은 매퍼를 타야 한다</b>
     * ({@code infra/mq/build.gradle} 주석). 발송 본문이 그 매퍼 설정에 딸려 바뀌면
     * <b>받는 쪽 계약이 우리 모르게 흔들린다.</b>
     */
    private static String escape(String value) {
        if (value == null) {
            return "";
        }
        StringBuilder escaped = new StringBuilder(value.length() + 8);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> escaped.append("\\\"");
                case '\\' -> escaped.append("\\\\");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                case '\b' -> escaped.append("\\b");
                case '\f' -> escaped.append("\\f");
                default -> {
                    if (c < 0x20) {
                        escaped.append(String.format("\\u%04x", (int) c));
                    } else {
                        escaped.append(c);
                    }
                }
            }
        }
        return escaped.toString();
    }

    /** {@code http}·{@code https} 와 호스트를 요구한다 — 아니면 모든 알림이 매번 실패한다. */
    private static URI requireHttpUri(String endpoint) {
        URI uri = URI.create(endpoint);
        String scheme = uri.getScheme();
        if (uri.getHost() == null
                || !("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme))) {
            throw new IllegalArgumentException(
                    "발송 endpoint 는 호스트가 있는 http(s) 주소여야 합니다. 받은 값=" + endpoint);
        }
        return uri;
    }

    /**
     * 하나씩 <b>양수이고 상한 안</b>인지 본다.
     *
     * <p>0 은 "무한" 이 아니라 <b>즉시 실패</b>다 — 그대로 두면 모든 알림이 타임아웃된다.
     *
     * <p>상한을 <b>더하기 전에 각각</b> 보는 이유는 넘침이다. {@code Duration.ofDays(...)}
     * 둘의 {@code toMillis()} 를 더하면 {@code long} 이 넘쳐 <b>음수</b>가 되고, 그 음수는
     * 합 검사를 그냥 통과한다 — 상한이 있는데도 없는 것과 같아진다(리뷰가 짚었다).
     * 각 항이 상한 이하이면 합은 상한의 두 배를 못 넘으므로 넘칠 수가 없다.
     */
    private static Duration requireWithinCap(Duration timeout, String name) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException(
                    name + " 타임아웃은 양수여야 합니다. 0 은 무한이 아니라 즉시 실패입니다. "
                            + "받은 값=" + timeout);
        }
        if (timeout.toMillis() > MAX_TOTAL_TIMEOUT_MILLIS) {
            throw new IllegalArgumentException(
                    name + " 타임아웃(" + timeout + ")이 한 건 상한("
                            + MAX_TOTAL_TIMEOUT_MILLIS + "ms)을 넘습니다.");
        }
        return timeout;
    }
}
