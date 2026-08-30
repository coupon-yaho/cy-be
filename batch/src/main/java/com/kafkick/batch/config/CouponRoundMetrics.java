// 회차 전이 스케줄러가 내는 카운터를 소유합니다.
package com.kafkick.batch.config;

import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * <b>카운터를 스케줄러에서 떼어 낸다.</b> {@code CouponRoundScheduler} 는
 * {@code batch.scheduling.enabled} 에 묶인 조건부 빈인데, 카운터가 그 안에 있으면
 * <b>스케줄러를 끈 배치에서 시리즈가 아예 사라진다</b> — 그러면
 * {@code increase(cy_coupon_round_ticks_total[5m]) == 0} 이라는 감별을 <b>평가할 수조차 없다</b>.
 * runbook 이 그것을 필요로 하는 순간이 정확히 그 순간이다.
 *
 * <p>{@code ExpireMetrics} 가 같은 이유로 같은 모양이다 — 미터의 수명은 그것을 올리는 코드의
 * 수명보다 길어야 한다.
 *
 * <p><b>0 과 "없음" 은 다르다.</b> 여기서 미터를 만들어 두면 스케줄러가 안 뜬 배치에서도
 * {@code 0} 이 나가고, 관제는 <i>"돌지 않았다"</i> 를 읽는다. 미터가 없으면 그 식이 빈 벡터가
 * 되어 <b>조용히 참도 거짓도 아니게</b> 된다.
 */
@Component
public class CouponRoundMetrics {

    private final Counter ticks;

    private final Counter transitionFailures;

    /**
     * <b>대상 조회 실패는 전이 실패와 다른 축이다.</b> 조회가 매 tick 죽으면 {@code ticks} 는
     * 오르고 {@code transitionFailures} 는 <b>0</b> 이라, runbook 이 <i>"돌고 있으면 전이 실패를
     * 보라"</i> 로 보낸 사람이 <b>막다른 길</b>에 선다. 커넥션 풀 고갈이 정확히 그 경로다 —
     * 조회가 전이보다 먼저 죽는다.
     */
    private final Counter selectFailures;

    public CouponRoundMetrics(MeterRegistry registry) {
        this.ticks = Counter.builder("cy_coupon_round_ticks_total")
                .description("회차 상태 전이 스케줄러가 한 주기를 끝낸 횟수")
                .register(registry);
        this.transitionFailures = Counter.builder("cy_coupon_round_transition_failures_total")
                .description("회차 상태 전이 UPDATE 가 실패한 횟수")
                .register(registry);
        this.selectFailures = Counter.builder("cy_coupon_round_select_failures_total")
                .description("전이 대상 조회가 실패해 그 축을 건너뛴 횟수")
                .register(registry);
    }

    /** 한 주기를 끝냈다. 전이가 0건이어도 오른다 — 그게 이 카운터의 뜻이다. */
    public void tickCompleted() {
        ticks.increment();
    }

    public void transitionsFailed(int count) {
        transitionFailures.increment(count);
    }

    /** 대상 조회가 실패해 그 축(열기 또는 닫기)을 건너뛰었다. */
    public void selectFailed() {
        selectFailures.increment();
    }
}
