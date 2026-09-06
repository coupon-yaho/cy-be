// 지금 선 발송기가 실물인지 목인지를 지표로 냅니다.
package com.kafkick.infra.mq.notification;

import java.util.Objects;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import com.kafkick.core.notification.NotificationSender;

/**
 * <b>성공 지표가 거짓말을 할 수 있다는 사실을 드러낸다.</b>
 *
 * <p>{@link MockNotificationSender} 는 아무 데도 안 보내면서 예외도 안 던진다. 그래서
 * 그것이 선 환경에서도 {@code app.notify.sent{result=success}} 가 <b>정상적으로 오른다.</b>
 * outbox 는 {@code PUBLISHED} 가 되고 소비자도 정상 소비하므로
 * {@code OutboxCommandsDead} 도 {@code OutboxBacklogGrowing} 도 안 뜬다 —
 * <b>파이프라인 전체가 초록인데 사용자에게는 아무것도 안 간다.</b>
 *
 * <p>스위치({@code notification.sender.http.enabled})가 {@code matchIfMissing = true} 라
 * <b>설정을 빠뜨리면 조용히 목이 뜬다.</b> 그것을 막지는 않는다 — 로컬·테스트·시연이
 * 그것으로 돌고, 뒤집으면 설정을 빠뜨린 환경에서 발송기가 아예 없어 기동이 죽는다.
 * 막는 대신 <b>보이게</b> 한다.
 *
 * <p><b>스위치가 아니라 실제로 선 빈을 본다.</b> 프로퍼티를 읽으면 그 프로퍼티와 조건부
 * 배선이 갈렸을 때 지표가 <b>배선이 아니라 의도</b>를 말하게 된다 — 이 게이지가 잡으려는
 * 것이 정확히 그 갈라짐이다.
 */
public class NotificationSenderModeGauge {

    /**
     * 실물 발송기가 섰는가 — 1 실물 · 0 목.
     *
     * <p><b>알림 규칙이 이 이름을 그대로 쓴다</b>({@code rules/outbox-alerts.yml} 의
     * {@code NotifySuccessesAreNotReal}). 여기서 이름을 바꾸고 규칙을 안 고치면
     * Prometheus 는 에러가 아니라 <b>빈 결과</b>를 돌려주고 알림이 영원히 안 뜬다.
     * {@code OutboxAlertRuleContractTest} 가 이 상수와 규칙 파일을 대조한다.
     */
    public static final String GAUGE = "cy_notify_sender_live";

    private final boolean live;

    public NotificationSenderModeGauge(NotificationSender sender, MeterRegistry registry) {
        Objects.requireNonNull(sender, "sender");
        this.live = !(sender instanceof MockNotificationSender);
        if (registry == null) {
            return;
        }
        Gauge.builder(GAUGE, this, self -> self.live ? 1 : 0)
                .description("실제로 밖으로 보내는 발송기가 섰는가 — 1 실물 · 0 목(안 보냄)")
                .register(registry);
    }

    /** 테스트가 배선을 확인하는 자리. 지표와 같은 값을 본다. */
    public boolean isLive() {
        return live;
    }
}
