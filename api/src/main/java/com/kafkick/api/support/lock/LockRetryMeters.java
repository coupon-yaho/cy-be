package com.kafkick.api.support.lock;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.kafkick.api.observation.MeterNames;

import io.micrometer.core.instrument.Counter;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * 락 경합으로 물러선 요청과 그 결말을 센다. <b>{@code operation} 태그로 경로를 가른다</b> —
 * 발급·사용·사용취소·발급취소가 같은 멱등 행과 발급 행을 치므로, 어느 쪽이 부딪히는지
 * 나눠 봐야 한다.
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
 *   <li>{@code abandoned} — 물러섰는데 <b>락 경합이 아닌 실패</b>로 끝난 요청</li>
 * </ul>
 *
 * <p><b>셋 다 요청 단위이고 서로 배타다.</b> 물러섬마다 올리면 두 번 물러선 요청이 둘로
 * 세어지고, 끝내 실패한 요청이 양쪽에 동시에 들어가 <i>"재시도가 몇 건을 살렸나"</i> 를
 * 계산할 수 없게 된다. <b>셋의 합</b>이 락 경합을 한 번이라도 만난 요청 수다.
 *
 * <p><b>{@code abandoned} 가 없으면 그 합이 틀린다.</b> 처음엔 둘만 두었는데, 사용·취소에서는
 * 경합에서 진 뒤 다시 했을 때 상대가 이미 상태를 바꿔 놓아 전이가 거절되는 것이 정상
 * 시나리오다. 그 요청은 경합을 만났는데도 어느 쪽에도 안 들어가, <i>"사용 경로에 경합이
 * 얼마나 있었나"</i> 를 물으면 0 이라는 답이 나온다(리뷰가 잡았다).
 *
 * <p>물러선 <i>횟수</i>가 필요해지면 그때 별도 카운터를 둔다. 지금 이름에 섞지 않는다.
 *
 * <p><b>Micrometer description 도 같은 단위로 적는다.</b> 대시보드에서 사람이 먼저 보는
 * 것은 이 설명이라, 여기만 "횟수" 로 남으면 요청 단위 값을 시도 횟수로 읽는다.
 */
@Component
public final class LockRetryMeters {

    private static final String NAME = MeterNames.COUPON_LOCK_RETRY;

    /**
     * 경로 × 결말을 <b>기동 때 미리 등록한다.</b> 첫 증가 때 만들면, 한 번도 안 부딪힌
     * 경로가 대시보드에서 0 이 아니라 "데이터 없음" 으로 나온다 — 그 둘은 아주 다른 말이다.
     * 경로 넷 × 결말 셋 = 열둘로 고정이라 미리 만들어도 시계열이 안 늘어난다.
     */
    private final Map<String, Map<String, Counter>> byOutcome;

    public LockRetryMeters(MeterRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        Map<String, Map<String, Counter>> counters = new HashMap<>();
        for (String outcome : LockRetryOperations.OUTCOMES) {
            counters.put(outcome, counters(registry, outcome, DESCRIPTIONS.get(outcome)));
        }
        this.byOutcome = Map.copyOf(counters);
    }

    /** 물러섰다가 끝내 성공했다. 요청당 한 번만 부른다. */
    public void recovered(String operation) {
        counter(LockRetryOperations.RECOVERED, operation).increment();
    }

    /** 상한이나 시간 예산까지 가서 끝내 실패했다. 요청당 한 번만 부른다. */
    public void exhausted(String operation) {
        counter(LockRetryOperations.EXHAUSTED, operation).increment();
    }

    /**
     * 물러섰는데 <b>락 경합이 아닌 실패</b>로 끝났다. 요청당 한 번만 부른다.
     *
     * <p>이 결말이 없으면 합이 안 맞는다. 사용·취소에서는 경합에서 진 뒤 다시 했을 때
     * 상대가 이미 상태를 바꿔 놓아 전이가 거절되는 일이 정상 시나리오다. 그 요청은
     * 락 경합을 <b>만났는데도</b> 두 카운터 어디에도 안 들어가, "사용 경로에 경합이
     * 얼마나 있었나" 를 물으면 0 이라는 답이 나온다.
     */
    public void abandoned(String operation) {
        counter(LockRetryOperations.ABANDONED, operation).increment();
    }

    private static final Map<String, String> DESCRIPTIONS = Map.of(
            LockRetryOperations.RECOVERED,
            "락 경합으로 물러섰다가 끝내 성공한 요청 수 (요청당 1)",
            LockRetryOperations.EXHAUSTED,
            "락 경합으로 상한이나 예산까지 가서 끝내 실패한 요청 수 (요청당 1)",
            LockRetryOperations.ABANDONED,
            "락 경합으로 물러섰지만 다른 실패로 끝난 요청 수 (요청당 1)");

    private static Map<String, Counter> counters(
            MeterRegistry registry, String outcome, String description) {
        Map<String, Counter> byOperation = new HashMap<>();
        for (String operation : LockRetryOperations.ALL) {
            byOperation.put(operation, Counter.builder(NAME)
                    .description(description)
                    .tag("operation", operation)
                    .tag("outcome", outcome)
                    .register(registry));
        }
        return Map.copyOf(byOperation);
    }

    /**
     * 모르는 이름이면 세지 않고 <b>거절한다.</b> 조용히 만들어 주면 오타 하나가 새 시계열이
     * 되고, 요청에서 온 값이 섞이면 태그가 끝없이 늘어난다.
     */
    private Counter counter(String outcome, String operation) {
        Counter counter = byOutcome.get(outcome).get(operation);
        if (counter == null) {
            throw new IllegalArgumentException(
                    "LockRetryOperations 에 없는 경로입니다: " + operation);
        }
        return counter;
    }
}
