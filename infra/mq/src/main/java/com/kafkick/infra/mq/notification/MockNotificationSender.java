package com.kafkick.infra.mq.notification;

import java.util.Objects;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.kafkick.core.notification.NotificationSender;
import com.kafkick.core.notification.domain.Notification;

/**
 * 아무 데도 안 보내는 발송기.
 *
 * <p><b>지우지 않는다.</b> 실제 연동이 없는 회차(로컬·테스트·시연)에서 파이프라인이
 * 그대로 돌아야 하고, 이 저장소의 통합 테스트가 이것으로 돈다.
 *
 * <p><b>{@code matchIfMissing = true} 다.</b> 스위치를 안 켜면 이쪽이 뜬다 —
 * 반대로 두면 설정을 빠뜨린 환경에서 발송기가 <b>아예 없어</b> 기동이 죽는다.
 * 실제 연동은 {@link HttpNotificationSender} 가 <b>명시적으로 켤 때만</b> 대신 선다.
 *
 * <p>⚠️ 그 대가는 <b>"안 보내는데 성공으로 보인다"</b> 는 것이다. 스위치를 안 켠 환경에서
 * {@code app.notify.sent{result=success}} 가 올라간다 — 그 지표를 운영 판단에 쓰려면
 * 스위치가 켜져 있는지 먼저 봐야 한다.
 */
@Component
@ConditionalOnProperty("kafka.enabled")
@ConditionalOnProperty(name = "notification.sender.http.enabled", havingValue = "false",
        matchIfMissing = true)
public class MockNotificationSender implements NotificationSender {

    /**
     * <b>아무 데도 안 보내지만 계약은 똑같이 건다.</b> 이 저장소의 통합 테스트가 이것으로
     * 도는데, 여기서 {@code null} 키를 그냥 받아 주면 키를 빠뜨린 호출자가 <b>로컬에서
     * 전부 통과</b>하고 실제 연동에서만 터진다. 그때 터지는 모양이 중복 발송이라 회복이 없다.
     */
    @Override
    public void send(Notification notification, String idempotencyKey) {
        Objects.requireNonNull(notification, "notification");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey");
    }
}
