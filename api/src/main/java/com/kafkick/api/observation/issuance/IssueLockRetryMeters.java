package com.kafkick.api.observation.issuance;

import java.util.Objects;

import io.micrometer.core.instrument.Counter;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * 발급이 락 경합으로 물러선 횟수와 그 결말을 센다.
 *
 * <p><b>재시도의 효과는 지표로만 보인다.</b> 다시 시도해 살아난 요청은 응답이 201 이라
 * 에러율에도, 응답 코드 분포에도 흔적이 없다. 이 값이 없으면 부하 회차에서
 * <i>"재시도 덕에 에러율이 얼마나 내려갔나"</i> 를 로그 grep 으로 증명해야 한다.
 *
 * <p>두 결말을 가른다.
 *
 * <ul>
 *   <li>{@code recovered} — 물러섰고 다시 시도했다. <b>이 수가 곧 재시도가 없었으면
 *       500 이 됐을 요청 수</b>다</li>
 *   <li>{@code exhausted} — 상한이나 시간 예산까지 갔고 그대로 실패했다. 이 수가 오르면
 *       재시도로 덮을 수 없는 무언가가 있다는 뜻이라 사람이 봐야 한다</li>
 * </ul>
 *
 * <p>둘을 한 이름의 태그로 두는 이유 — 합이 곧 락 경합 총 발생 수다. 이름을 나누면
 * 그 합을 대시보드에서 다시 만들어야 한다.
 */
@Component
public final class IssueLockRetryMeters {

    private static final String NAME = "coupon.issue.lock.retry";

    private final Counter recovered;
    private final Counter exhausted;

    public IssueLockRetryMeters(MeterRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        this.recovered = Counter.builder(NAME)
                .description("발급이 락 경합으로 물러선 뒤 다시 시도한 횟수")
                .tag("outcome", "recovered")
                .register(registry);
        this.exhausted = Counter.builder(NAME)
                .description("발급이 락 경합으로 상한까지 가서 실패한 횟수")
                .tag("outcome", "exhausted")
                .register(registry);
    }

    /** 물러섰고 다시 시도한다. */
    public void recovered() {
        recovered.increment();
    }

    /** 상한이나 시간 예산까지 갔고 그대로 실패한다. */
    public void exhausted() {
        exhausted.increment();
    }
}
