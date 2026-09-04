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
 *
 * <p><b>생성만은 예외다.</b> 설정이 틀린 것은 부르는 쪽의 산술 실수와 달리 기동 시점에
 * 드러나야 한다 — 생성자 참조.
 */
public final class FullJitterBackOff {

    /**
     * 경계값 상한. 저장소의 지연 변환기가 이 위에서 던지는데, 그것은
     * <b>첫 실패가 실제로 났을 때</b> 터진다. 여기서 막으면 생성 시점에 드러난다.
     *
     * <p>이 상한이 {@link #nextDelay} 의 {@code ceiling + 1} 오버플로도 함께 막는다 —
     * 상한이 없으면 {@code cap} 이 {@code Long.MAX_VALUE} 밀리초일 때 그 덧셈이 뒤집힌다.
     */
    private static final long MAX_MILLIS = Duration.ofDays(365).toMillis();

    /**
     * 자리이동 수 상한. <b>자바가 {@code long} 이동 수를 63 으로 마스킹하기 때문에만 있다</b> —
     * {@code x << 64} 는 {@code x << 0} 이라, 자르지 않으면 큰 {@code attempt} 에서 판별식
     * 자체가 무너진다.
     *
     * <p><b>오버플로 방어가 아니다.</b> 그것은 {@link #ceilingMillis} 의 나눗셈이 한다.
     * 한때 이 값이 30 이었는데, 그때는 자리이동 결과의 부호로 오버플로를 걸렀기 때문이다.
     * 나눗셈으로 바꾼 뒤에는 그 역할이 없어졌고 <b>계약을 깎기만 했다</b> —
     * {@code base=1ms · cap=365일 · attempt=35} 에서 계약상 상한은 365일인데
     * 30 에서 잘려 12.4일이 됐다(Qodo 리뷰가 잡았다).
     */
    private static final int MAX_SHIFT = 62;

    private final long baseMillis;
    private final long capMillis;

    /**
     * @param base 기본 간격. 첫 재시도 상한이 {@code min(cap, base × 2)} 다 —
     *             {@code cap == base} 인 구성도 유효하고 그때는 {@code base} 다
     * @param cap  지연 상한
     * @throws IllegalArgumentException {@code null}·0·음수이거나, 365일을 넘거나,
     *         <b>밀리초로 환산해 0 이 되거나</b>(예: {@code Duration.ofNanos(1)}),
     *         {@code cap} 이 {@code base} 보다 작을 때
     */
    public FullJitterBackOff(Duration base, Duration cap) {
        this.baseMillis = requireUsableMillis(base, "backoff base");
        this.capMillis = requireUsableMillis(cap, "backoff cap");
        if (capMillis < baseMillis) {
            throw new IllegalArgumentException(
                    "backoff cap 은 base 이상이어야 합니다. 그렇지 않으면 첫 재시도부터 상한에 "
                            + "걸려 지수 구간이 통째로 사라집니다. base=" + baseMillis
                            + "ms cap=" + capMillis + "ms");
        }
    }

    /**
     * @param attempt 몇 번째 재시도인가. 부르는 쪽이 {@code failure_count + 1} 을 준다
     * @return {@code [0, ceilingMillis(attempt)]} 안의 값. 밀리초 정밀도다
     */
    public Duration nextDelay(int attempt) {
        // nextLong 의 상한은 배타적이다. ceiling 자체도 나올 수 있어야 한다.
        // ceiling 은 cap 이하이고 cap 은 MAX_MILLIS 이하라 이 덧셈은 넘치지 않는다.
        long ceiling = ceilingMillis(attempt);
        return Duration.ofMillis(ThreadLocalRandom.current().nextLong(0, ceiling + 1));
    }

    /**
     * {@code min(cap, base × 2^attempt)} — <b>자리이동을 하지 않고</b> 구한다.
     *
     * <p><b>자리이동 결과로 오버플로를 판별할 수 없다.</b> {@code long} 은 음수로만 넘치지
     * 않는다 — 작은 양수나 <b>0</b> 으로도 감긴다. 허용 범위 안의 {@code base = 2^34ms}
     * (약 198.8일)를 30 밀면 정확히 {@code 2^64} 라 <b>0 으로 감기고</b>, 그러면 상한이 0 이
     * 되어 <b>모든 재시도가 즉시</b> 실행된다 — 흩뜨리려고 만든 클래스가 정확히 반대로 동작한다.
     * 한때 {@code shifted < 0} 로 걸렀는데 그 사례를 못 잡았다.
     *
     * <p>그래서 <b>나눗셈으로 먼저 묻는다</b> — {@code base > cap / 2^attempt} 면 곱이 이미
     * {@code cap} 을 넘은 것이다. 아니라면 곱은 {@code cap} 이하라 자리이동이 안전하다.
     * {@code attempt} 가 {@link #MAX_SHIFT}(62) 로 잘려 있어 우변의 자리이동도 안전하다 —
     * 자바는 이동 수를 63 으로 마스킹하므로 그 잘림이 없으면 판별식 자체가 무너진다.
     * 62 는 <b>깎지 않는다</b>: {@code cap >> 62} 는 0 이고 {@code base} 는 1 이상이라
     * 그 지점에서 판별식이 {@code cap} 을 고른다 — 계약과 같다.
     *
     * <p>가시성이 package-private 인 것은 <b>테스트가 표본이 아니라 값을 보게</b> 하기
     * 위해서다. 상한을 난수로 확인하면 검증이 확률적이 된다.
     */
    long ceilingMillis(int attempt) {
        int shift = Math.min(Math.max(attempt, 1), MAX_SHIFT);
        return baseMillis > (capMillis >> shift) ? capMillis : (baseMillis << shift);
    }

    private static long requireUsableMillis(Duration duration, String name) {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(name + " 은 양수여야 합니다.");
        }
        if (duration.compareTo(Duration.ofDays(365)) > 0) {
            throw new IllegalArgumentException(
                    name + " 은 365일 이하여야 합니다. 저장소의 지연 변환기가 그 위에서 "
                            + "던지는데, 그것은 첫 실패가 났을 때야 터집니다. 받은 값=" + duration);
        }
        long millis = duration.toMillis();
        if (millis < 1) {
            // 양수인데 환산하면 0 이 되는 구간이 있다. 그대로 두면 상한이 0 이라
            // 지터가 사라지고 모든 재시도가 즉시 실행된다 — 조용히 반대로 동작한다.
            throw new IllegalArgumentException(
                    name + " 은 1ms 이상이어야 합니다. 밀리초로 환산하면 0 이 되어 지터가 "
                            + "사라지고 재시도가 즉시 실행됩니다. 받은 값=" + duration);
        }
        return Math.min(millis, MAX_MILLIS);
    }
}
