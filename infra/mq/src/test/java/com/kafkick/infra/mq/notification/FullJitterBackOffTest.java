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

    /** 하한이 0 이어야 한다. 하한을 두면(Equal Jitter) 그 구간에 다시 뭉친다. */
    @Test
    void canReturnZeroSoThereIsNoFloorToClusterOn() {
        boolean sawZero = IntStream.range(0, 2000)
                .anyMatch(i -> backOff.nextDelay(1).isZero());

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
