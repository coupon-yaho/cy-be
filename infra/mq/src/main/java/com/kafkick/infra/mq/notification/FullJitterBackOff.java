// 실패한 발행 명령을 언제 다시 집을지 정합니다. 값이 흩어지는 것이 이 클래스의 목적입니다.
package com.kafkick.infra.mq.notification;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * <b>지연을 흩뜨린다 — 늦추는 것이 목적이 아니다.</b>
 *
 * <p>예전에는 실패한 명령을 전부 <b>고정 1초</b> 뒤로 예약했다. 그러면 한 번에 실패한 것들이
 * <b>다음에도 같은 시각에 함께</b> 온다. 재시도 무리가 그대로 유지되므로 경합도 그대로다.
 *
 * <p>AWS <i>Exponential Backoff And Jitter</i> 가 순수 지수 백오프조차 같은 이유로 부족하다고
 * 적었다 — <i>"경쟁하는 수만 줄었을 뿐, 아무도 경쟁하지 않는 시간대를 만든 것"</i>. 세 변형을
 * 실측 비교해 <b>Full Jitter</b> 와 Decorrelated Jitter 가 크게 앞섰고 Equal Jitter 가 최악이었다.
 *
 * <pre>
 *   delay = random(0, min(cap, base × 2^attempt))
 * </pre>
 *
 * <p><b>하한이 0 인 것이 핵심이다.</b> 하한을 두면(Equal Jitter) 그 구간에 다시 뭉친다.
 *
 * <h2>밀리초여야 하는 이유</h2>
 *
 * <p>초 단위로 자르면 이 클래스가 <b>무의미해진다.</b> {@code base=200ms} 에서 첫 재시도의
 * 상한은 400ms 라 초로 자르면 전부 0 이 되고, 그러면 실패한 것들이 <b>즉시 동시에</b> 다시
 * 온다 — 고정 1초보다 나쁘다. 그래서 저장소 어댑터가 마이크로초로 더하도록 함께 고쳤다.
 *
 * <h2>{@code attempt} 는 몇 번째 재시도인가</h2>
 *
 * <p>부르는 쪽이 {@code failure_count + 1} 을 준다. 클레임 시점의 {@code failure_count} 는
 * <b>이번 실패를 세기 전</b> 값이라, 그대로 쓰면 첫 실패의 상한이 {@code base} 가 되어
 * 한 칸씩 밀린다.
 *
 * <p>음수나 0 을 받으면 1 로 본다 — 지연 계산이 부르는 쪽의 산술 실수로 <b>예외를 던져
 * 발행을 막는 것</b>이 더 나쁘다. 이 클래스는 정확성이 아니라 분산을 담당한다.
 */
public final class FullJitterBackOff {

    /**
     * {@code base << attempt} 의 자리이동 상한.
     *
     * <p>{@code long} 이라 63 까지 밀 수 있지만, 30 이면 {@code base=200ms} 에서 이미
     * 약 2.5년이라 어떤 {@code cap} 이든 넘는다. 더 밀 이유가 없고, 밀면 부호가 뒤집혀
     * <b>음수 상한</b>이 되어 {@code nextLong} 이 던진다.
     */
    private static final int MAX_SHIFT = 30;

    private final long baseMillis;
    private final long capMillis;

    public FullJitterBackOff(Duration base, Duration cap) {
        long baseMillis = requirePositiveMillis(base, "backoff base");
        long capMillis = requirePositiveMillis(cap, "backoff cap");
        if (capMillis < baseMillis) {
            throw new IllegalArgumentException(
                    "backoff cap 은 base 이상이어야 합니다. 그렇지 않으면 첫 재시도부터 상한에 "
                            + "걸려 지수 구간이 통째로 사라집니다. base=" + baseMillis
                            + "ms cap=" + capMillis + "ms");
        }
        this.baseMillis = baseMillis;
        this.capMillis = capMillis;
    }

    /**
     * @param attempt 몇 번째 재시도인가. 부르는 쪽이 {@code failure_count + 1} 을 준다
     * @return {@code [0, min(cap, base × 2^attempt)]} 안의 값. 밀리초 정밀도다
     */
    public Duration nextDelay(int attempt) {
        int shift = Math.min(Math.max(attempt, 1), MAX_SHIFT);
        long ceiling = Math.min(capMillis, baseMillis << shift);
        // nextLong 의 상한은 배타적이다. ceiling 자체도 나올 수 있어야 한다.
        return Duration.ofMillis(ThreadLocalRandom.current().nextLong(0, ceiling + 1));
    }

    private static long requirePositiveMillis(Duration duration, String name) {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(name + " 은 양수여야 합니다.");
        }
        return duration.toMillis();
    }
}
