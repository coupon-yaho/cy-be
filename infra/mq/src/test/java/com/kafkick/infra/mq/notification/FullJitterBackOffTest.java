package com.kafkick.infra.mq.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

class FullJitterBackOffTest {

    private static final Duration BASE = Duration.ofMillis(200);
    private static final Duration CAP = Duration.ofSeconds(20);

    private final FullJitterBackOff backOff = new FullJitterBackOff(BASE, CAP);

    /** 상한이 {@code base × 2^attempt} 다. 넘으면 뭉치는 것을 막으려던 상한이 뚫린 것이다. */
    @Test
    void staysWithinTheExponentialCeiling() {
        for (int attempt = 1; attempt <= 6; attempt++) {
            long ceiling = Math.min(CAP.toMillis(), BASE.toMillis() << attempt);
            for (int i = 0; i < 200; i++) {
                assertThat(backOff.nextDelay(attempt).toMillis())
                        .isBetween(0L, ceiling);
            }
        }
    }

    /**
     * <b>이 테스트가 이 클래스의 존재 이유다.</b> 고정 지연이었을 때는 값이 하나였고,
     * 그래서 같이 실패한 것들이 같이 돌아왔다.
     */
    @Test
    void spreadsValuesInsteadOfReturningOne() {
        Set<Long> seen = new HashSet<>();
        IntStream.range(0, 500).forEach(i -> seen.add(backOff.nextDelay(5).toMillis()));

        assertThat(seen).hasSizeGreaterThan(100);
    }

    /**
     * 하한이 0 이어야 한다. 하한을 두면(Equal Jitter) 그 구간에 다시 뭉친다.
     *
     * <p><b>전용 백오프를 쓴다.</b> 공용 픽스처({@code base=200ms})로는 첫 재시도 상한이
     * 400ms 라 값이 401개고, 2,000번 뽑아도 0 이 한 번도 안 나올 확률이
     * {@code (400/401)^2000 ≈ 0.68%} 다 — <b>147번에 한 번 깨진다.</b> 확률적 테스트는
     * 뽑는 횟수가 아니라 <b>표본 공간</b>을 좁혀야 안정된다.
     *
     * <p>{@code base=cap=1ms} 면 상한이 {@code min(1, 1<<1)=1} 이라 값이 {0,1} 둘뿐이고,
     * 20번에 0 이 안 나올 확률이 {@code 0.5^20 ≈ 1e-6} 이다.
     */
    @Test
    void canReturnZeroSoThereIsNoFloorToClusterOn() {
        FullJitterBackOff coinFlip =
                new FullJitterBackOff(Duration.ofMillis(1), Duration.ofMillis(1));

        boolean sawZero = IntStream.range(0, 20)
                .anyMatch(i -> coinFlip.nextDelay(1).isZero());

        assertThat(sawZero).isTrue();
    }

    /** 밀리초 정밀도가 없으면 첫 재시도 상한(400ms)이 통째로 0 이 된다. */
    @Test
    void firstRetryCeilingIsSubSecondSoSecondsWouldCollapseIt() {
        long ceiling = BASE.toMillis() << 1;

        assertThat(ceiling).isLessThan(1_000L);
        assertThat(backOff.nextDelay(1).toMillis()).isBetween(0L, ceiling);
    }

    /** {@code attempt} 가 커도 상한을 넘지 않는다. 자리이동 오버플로로 음수가 되면 안 된다. */
    @Test
    void largeAttemptIsClampedInsteadOfOverflowing() {
        for (int attempt : new int[] {30, 31, 62, 63, 64, Integer.MAX_VALUE}) {
            assertThat(backOff.nextDelay(attempt).toMillis())
                    .isBetween(0L, CAP.toMillis());
        }
    }

    /**
     * <b>큰 {@code base} 와 큰 {@code attempt} 를 함께 태운다.</b>
     *
     * <p>위 테스트는 {@code base=200ms} 픽스처로만 돌아서 <b>틀린 구현도 통과시킨다</b> —
     * 작은 {@code base} 는 30번을 밀어도 {@code long} 을 안 넘기 때문이다.
     * {@code base} 가 365일이면 {@code base << 30} 이 넘쳐
     * {@code -3,031,965,985,755,103,232} 이 되고, 음수 상한을 받은 {@code nextLong} 이 던진다.
     *
     * <p>지금 호출 경로로는 도달하지 않는다({@code failureCount ≤ 10} 이라 자리이동이 11 까지다).
     * 그래도 막는 이유는 이 클래스가 <b>예외로 발행을 막지 않겠다</b>고 스스로 적었기 때문이다.
     */
    @Test
    void doesNotOverflowEvenWhenBaseIsAtTheConfigurableMaximum() {
        Duration yearLong = Duration.ofDays(365);
        FullJitterBackOff huge = new FullJitterBackOff(yearLong, yearLong);

        for (int attempt : new int[] {1, 11, 29, 30, 31, Integer.MAX_VALUE}) {
            assertThat(huge.nextDelay(attempt).toMillis())
                    .isBetween(0L, yearLong.toMillis());
        }
    }

    /**
     * 0 이나 음수는 1 로 본다. 지연 계산이 부르는 쪽의 산술 실수로 예외를 던져
     * <b>발행을 막는 것</b>이 더 나쁘다.
     */
    @Test
    void nonPositiveAttemptIsTreatedAsTheFirstRetry() {
        long ceiling = BASE.toMillis() << 1;

        for (int attempt : new int[] {0, -1, Integer.MIN_VALUE}) {
            assertThat(backOff.nextDelay(attempt).toMillis()).isBetween(0L, ceiling);
        }
    }

    @Test
    void rejectsCapBelowBaseBecauseTheExponentialRangeWouldVanish() {
        assertThatThrownBy(() -> new FullJitterBackOff(Duration.ofSeconds(5), Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cap");
    }

    @Test
    void rejectsNonPositiveBounds() {
        assertThatThrownBy(() -> new FullJitterBackOff(Duration.ZERO, CAP))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FullJitterBackOff(BASE, Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
