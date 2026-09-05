package com.kafkick.core.notification;

import com.kafkick.core.notification.domain.Notification;

/**
 * 알림을 밖으로 내보낸다.
 *
 * <p><b>{@code idempotencyKey} 를 함께 받는다.</b> outbox 도 Kafka 도 at-least-once 라
 * 같은 알림이 여러 번 도착할 수 있고, 그것을 합치는 것은 <b>받는 쪽</b>이다 — 우리가 할
 * 일은 <b>같은 논리적 발송에 같은 키를 주는 것</b>뿐이다.
 *
 * <p><b>키를 발송기가 스스로 만들지 않는 이유</b> — 무엇이 "같은 발송" 인지는
 * {@code Notification} 하나로는 알 수 없다. 자동 재시도는 {@code attemptCount} 를 올리므로
 * 그것으로 키를 만들면 <b>재시도마다 키가 바뀌어</b> 받는 쪽이 못 합친다: 첫 요청이 처리된
 * 뒤 응답만 타임아웃된 경우 <b>두 번 발송된다.</b> 그 경계를 아는 것은 배달 판정
 * ({@code NotificationDeliveryDecision}) 쪽이라 거기서 만들어 넘긴다.
 */
public interface NotificationSender {

    /**
     * <p><b>둘 다 {@code null} 이면 보내지 않고 그 자리에서 멈춘다.</b> 특히 키가 없으면
     * 받는 쪽이 중복을 <b>못 합치므로</b>, 빈 키를 실어 조용히 보내는 것보다 안 보내는 것이
     * 낫다 — 안 보낸 것은 재시도로 회복되지만 <b>키 없이 두 번 간 것은 회복이 없다.</b>
     * 구현이 이 검사를 빠뜨리지 않도록 계약으로 적는다.
     *
     * @param notification 보낼 알림
     * @param idempotencyKey 같은 논리적 발송을 가리키는 키. <b>자동 재시도 사이에 안 변한다</b>
     * @throws NullPointerException 둘 중 하나가 {@code null} 일 때
     * @throws NotificationSendException 보내지 못했을 때. {@code reason} 이 재시도 가능
     *         여부를 진다
     */
    void send(Notification notification, String idempotencyKey);
}
