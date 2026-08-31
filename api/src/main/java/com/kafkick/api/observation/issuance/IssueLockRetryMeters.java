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
 *   <li>{@code recovered} — 물러섰다가 <b>끝내 성공한 요청</b>. 이 수가 곧 재시도가
 *       없었으면 500 이 됐을 요청 수다</li>
 *   <li>{@code exhausted} — 상한이나 시간 예산까지 가서 <b>끝내 실패한 요청</b></li>
 * </ul>
 *
 * <p><b>둘 다 요청 단위이고 서로 배타다.</b> 물러섬마다 올리면 두 번 물러선 요청이 둘로
 * 세어지고, 끝내 실패한 요청이 양쪽에 동시에 들어가 <i>"재시도가 몇 건을 살렸나"</i> 를
 * 계산할 수 없게 된다. 합은 <b>락 경합을 한 번이라도 만난 요청 수</b>다.
 *
 * <p>물러선 <i>횟수</i>가 필요해지면 그때 별도 카운터를 둔다. 지금 이름에 섞지 않는다.
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

    /** 물러섰다가 끝내 성공했다. 요청당 한 번만 부른다. */
    public void recovered() {
        recovered.increment();
    }

    /** 상한이나 시간 예산까지 가서 끝내 실패했다. 요청당 한 번만 부른다. */
    public void exhausted() {
        exhausted.increment();
    }
}
