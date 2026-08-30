// 회차 전이 스케줄러의 크론 가드가 기동 시점에 걸리는지 확인합니다.
package com.kafkick.batch.schedule;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.annotation.Scheduled;

import com.kafkick.batch.config.CouponRoundMetrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * <b>컨테이너를 안 띄운다.</b> 이 검사는 생성자 안에 있고 DB 와 아무 상관이 없다 —
 * 형제 {@code *SettingsTest} 들이 같은 이유로 같은 모양이다.
 */
class CouponRoundSchedulerSettingsTest {

    /**
     * <b>끄는 수단은 하나여야 한다.</b> {@code "-"} 로 끄면 트리거만 죽고 되읽기는 그대로
     * 돈다(그 빈은 조건부가 아니다) — {@code CouponRoundsNotOpening} 이 영원히 뜨는데
     * runbook 이 가리키는 자리에 {@code "-"} 가 앉아 있어 읽는 사람이 막힌다.
     * 형제 스케줄러 둘이 <b>다른 이유로</b> 같은 거절을 한다({@code asOf} 를 못 만든다).
     */
    @Test
    @DisplayName("coupon-open-cron 을 \"-\" 로 끄면 기동하지 못한다 — 끄는 수단은 하나다")
    void rejectDisabledCron() {
        assertThatThrownBy(() -> new CouponRoundScheduler(null, null, new CouponRoundMetrics(new SimpleMeterRegistry()), Scheduled.CRON_DISABLED))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.scheduling.enabled=false");
    }

    @Test
    @DisplayName("보통 크론은 통과한다")
    void acceptsNormalCron() {
        assertThatCode(() -> new CouponRoundScheduler(null, null, new CouponRoundMetrics(new SimpleMeterRegistry()), "0 * * * * *"))
                .doesNotThrowAnyException();
    }

}
