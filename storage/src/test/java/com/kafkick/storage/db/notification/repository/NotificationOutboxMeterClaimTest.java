package com.kafkick.storage.db.notification.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Locale;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.observation.DomainMeterNames;

/**
 * <b>{@link NotificationOutboxMeter#claimed(Map)} 의 가드를 태운다.</b>
 *
 * <p>가드를 적어 두고 안 태우면 <b>{@code @throws} 두 줄이 못 박히지 않은 산문</b>이
 * 된다 — 형제 {@code ClaimResultTest} 가 같은 모양의 가드를 같은 방식으로 태운다.
 *
 * <p>DB 가 필요 없어 스프링 컨텍스트를 안 띄운다.
 */
class NotificationOutboxMeterClaimTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final NotificationOutboxMeter meter = new NotificationOutboxMeter(registry);

    @Test
    @DisplayName("outbox 행에 올 수 없는 종류를 세면 거부한다")
    void rejectsAKindThatCannotBeAnOutboxRow() {
        assertThatThrownBy(() -> meter.claimed(Map.of(AttemptTrigger.AUTO, 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("AUTO");
    }

    @Test
    @DisplayName("음수를 세면 거부한다")
    void rejectsANegativeCount() {
        assertThatThrownBy(() -> meter.claimed(Map.of(AttemptTrigger.MANUAL, -1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("-1");
    }

    /**
     * <b>시계열은 아무도 안 세도 존재해야 한다.</b> 실패가 나야 생기게 두면 대시보드가
     * 0 과 "지표 없음" 을 구분하지 못한다 — 그것이 생성자가 미리 등록하는 이유이고,
     * 동시에 부르는 쪽이 <b>0 을 안 부르는</b> 이유이기도 하다.
     */
    @Test
    @DisplayName("아무것도 안 세도 종류별 시계열이 이미 있다")
    void everyKindHasASeriesBeforeAnythingIsClaimed() {
        for (AttemptTrigger kind : AttemptTrigger.outboxKinds()) {
            assertThat(registry.get(DomainMeterNames.OUTBOX_CLAIMED)
                    .tag(DomainMeterNames.TAG_TRIGGER, kind.name().toLowerCase(Locale.ROOT))
                    .counter().count())
                    .as("%s", kind)
                    .isZero();
        }
    }

    @Test
    @DisplayName("센 만큼 오른다")
    void addsTheCountedAmount() {
        meter.claimed(Map.of(AttemptTrigger.MANUAL, 3, AttemptTrigger.INITIAL, 61));

        assertThat(count(AttemptTrigger.MANUAL)).isEqualTo(3);
        assertThat(count(AttemptTrigger.INITIAL)).isEqualTo(61);
    }

    private double count(AttemptTrigger kind) {
        return registry.get(DomainMeterNames.OUTBOX_CLAIMED)
                .tag(DomainMeterNames.TAG_TRIGGER, kind.name().toLowerCase(Locale.ROOT))
                .counter().count();
    }
}
