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
     * <b>목 발송기가 아닌 것이 섰는가</b> — 1 목 아님 · 0 목.
     *
     * <p>⚠️ <b>"실제로 보낸다" 를 보장하지 않는다.</b> 이 값이 보는 것은 선 빈이
     * {@link MockNotificationSender} 가 <b>아니라는 것</b> 하나뿐이라, 아무 일도 안 하는
     * 발송기가 새로 생기면 그것도 1 로 센다(리뷰가 짚었다). 그래도 이 판정을 쓰는 이유는
     * 아래 {@code live} 필드 주석에 있다 — 반대로 적으면 <b>정상 운영에 알림이 울린다.</b>
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
        // **"목이 아니면 1" 이다 — "Http 면 1" 이 아니다.** 뒤집으면 발송기가 하나 더
        // 늘었을 때 그것이 조용히 0 이 되어, 멀쩡히 보내고 있는 인스턴스에
        // NotifySuccessesAreNotReal 이 뜬다. 오탐이 정탐보다 비싼 자리다 —
        // 이 알림은 critical 이라 사람을 부른다.
        //
        // 대가는 **아무 일도 안 하는 발송기도 1 로 센다**는 것이다. 그 위험은 이 게이지가
        // 아니라 NotificationSenderNullContractTest 와 발송기별 테스트가 진다.
        this.live = !(sender instanceof MockNotificationSender);
        // ⚠️ **여기서 조용히 돌아가면 알림이 영구히 안 뜬다.** 계열이 없으면
        // NotifySuccessesAreNotReal 의 `and` 가 항상 빈 결과가 되고, 알림이 안 오는 것은
        // "사고가 없다" 와 구분되지 않는다.
        //
        // 그래도 던지지 않는 이유 — 운영 배선은 ObjectProvider 가 항상 하나를 주므로
        // (NotificationConsumerConfig) 여기가 null 인 것은 **직접 생성하는 테스트뿐**이다.
        // 그 자리에서 기동을 죽이면 얻는 것 없이 테스트만 어려워진다.
        // 배선이 빠지는 진짜 사고는 런타임이 아니라 빌드에서 막는다 —
        // KafkaLayerWiringTest 가 진짜 api 컨텍스트에서 이 계열의 값을 본다.
        if (registry == null) {
            return;
        }
        Gauge.builder(GAUGE, this, self -> self.live ? 1 : 0)
                .description("목 발송기가 아닌 것이 섰는가 — 1 목 아님 · 0 목(안 보냄)")
                .register(registry);
    }

    /**
     * 테스트가 배선을 확인하는 자리. 지표와 같은 값을 본다.
     *
     * <p>이름이 {@code isLive} 지만 뜻은 <b>"목이 아니다"</b> 다 — {@link #GAUGE} 주석 참고.
     */
    public boolean isLive() {
        return live;
    }
}
