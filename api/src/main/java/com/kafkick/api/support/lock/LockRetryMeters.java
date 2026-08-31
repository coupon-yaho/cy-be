package com.kafkick.api.support.lock;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import io.micrometer.core.instrument.Counter;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * 락 경합으로 물러선 요청과 그 결말을 센다. <b>{@code operation} 태그로 경로를 가른다</b> —
 * 발급·사용·사용취소·발급취소가 같은 재고 행을 치므로, 어느 쪽이 부딪히는지 나눠 봐야 한다.
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
 *
 * <p><b>Micrometer description 도 같은 단위로 적는다.</b> 대시보드에서 사람이 먼저 보는
 * 것은 이 설명이라, 여기만 "횟수" 로 남으면 요청 단위 값을 시도 횟수로 읽는다.
 */
@Component
public final class LockRetryMeters {

    private static final String NAME = "coupon.lock.retry";

    /**
     * 경로마다 두 결말을 <b>기동 때 미리 등록한다.</b> 첫 증가 때 만들면, 한 번도 안 부딪힌
     * 경로가 대시보드에서 0 이 아니라 "데이터 없음" 으로 나온다 — 그 둘은 아주 다른 말이다.
     * 경로 수가 넷으로 고정이라 미리 만들어도 시계열이 안 늘어난다.
     */
    private final Map<String, Counter> recovered;
    private final Map<String, Counter> exhausted;

    public LockRetryMeters(MeterRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        this.recovered = counters(registry, "recovered",
                "락 경합으로 물러섰다가 끝내 성공한 요청 수 (요청당 1)");
        this.exhausted = counters(registry, "exhausted",
                "락 경합으로 상한이나 예산까지 가서 끝내 실패한 요청 수 (요청당 1)");
    }

    /** 물러섰다가 끝내 성공했다. 요청당 한 번만 부른다. */
    public void recovered(String operation) {
        counter(recovered, operation).increment();
    }

    /** 상한이나 시간 예산까지 가서 끝내 실패했다. 요청당 한 번만 부른다. */
    public void exhausted(String operation) {
        counter(exhausted, operation).increment();
    }

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
    private static Counter counter(Map<String, Counter> byOperation, String operation) {
        Counter counter = byOperation.get(operation);
        if (counter == null) {
            throw new IllegalArgumentException(
                    "LockRetryOperations 에 없는 경로입니다: " + operation);
        }
        return counter;
    }
}
