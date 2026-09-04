package com.kafkick.core.notification.retry;

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

    // ── 상한 계산 — 결정론 ────────────────────────────────────────────────
    //
    // 상한을 난수로 확인하면 검증이 확률적이 된다. 값을 직접 본다.

    @Test
    void ceilingDoublesUntilItReachesTheCap() {
        assertThat(backOff.ceilingMillis(1)).isEqualTo(400);
        assertThat(backOff.ceilingMillis(2)).isEqualTo(800);
        assertThat(backOff.ceilingMillis(3)).isEqualTo(1_600);
        assertThat(backOff.ceilingMillis(4)).isEqualTo(3_200);
        assertThat(backOff.ceilingMillis(5)).isEqualTo(6_400);
        assertThat(backOff.ceilingMillis(6)).isEqualTo(12_800);
        // 200 << 7 = 25,600 이라 여기서부터 cap 에 걸린다.
        assertThat(backOff.ceilingMillis(7)).isEqualTo(CAP.toMillis());
        assertThat(backOff.ceilingMillis(10)).isEqualTo(CAP.toMillis());
    }

    /**
     * <b>자리이동 결과로 오버플로를 판별할 수 없다.</b> {@code long} 은 음수로만 넘치지
     * 않는다 — {@code base = 2^34ms}(약 198.8일, 허용 범위 안)를 30 밀면 정확히
     * {@code 2^64} 라 <b>0 으로</b> 감긴다.
     *
     * <p>한때 {@code shifted < 0} 로 걸렀고, 그때 이 입력은 상한을 0 으로 만들어
     * <b>모든 재시도를 즉시</b> 실행시켰다 — 흩뜨리려고 만든 클래스가 정반대로 동작한다.
     * 이 테스트가 그 회귀를 막는다.
     */
    @Test
    void ceilingSurvivesShiftWraparoundToZero() {
        Duration wrapsToZero = Duration.ofMillis(1L << 34);
        FullJitterBackOff huge = new FullJitterBackOff(wrapsToZero, wrapsToZero);

        // nextDelay 의 결과를 단언하지 않는다 — 0 은 합법이고, 그것이 이 정책의 핵심이다.
        // 상한을 직접 보는 것으로 충분하다.
        assertThat(huge.ceilingMillis(30)).isEqualTo(1L << 34);
    }

    /** 작은 양수로 감기는 경우도 같다 — {@code 2^34+1} 을 30 밀면 {@code 2^30} 이 된다. */
    @Test
    void ceilingSurvivesShiftWraparoundToASmallPositive() {
        Duration wrapsSmall = Duration.ofMillis((1L << 34) + 1);
        FullJitterBackOff huge = new FullJitterBackOff(wrapsSmall, wrapsSmall);

        assertThat(huge.ceilingMillis(30)).isEqualTo((1L << 34) + 1);
    }

    /**
     * <b>자르는 것이 계약을 깎으면 안 된다.</b> 한때 상한이 30 이었고, 그때
     * {@code base=1ms · cap=365일 · attempt=35} 의 상한이 계약(365일)이 아니라
     * 12.4일이었다 — 자리이동 결과의 부호로 오버플로를 걸던 시절의 잔재였다.
     */
    @Test
    void ceilingReachesTheCapEvenWithATinyBase() {
        Duration year = Duration.ofDays(365);
        FullJitterBackOff tiny = new FullJitterBackOff(Duration.ofMillis(1), year);

        assertThat(tiny.ceilingMillis(35)).isEqualTo(year.toMillis());
        assertThat(tiny.ceilingMillis(62)).isEqualTo(year.toMillis());
        assertThat(tiny.ceilingMillis(Integer.MAX_VALUE)).isEqualTo(year.toMillis());
    }

    /** {@code cap == base} 도 유효한 구성이다. 그때 첫 상한은 {@code base × 2} 가 아니라 {@code base} 다. */
    @Test
    void firstCeilingIsBaseWhenCapEqualsBase() {
        FullJitterBackOff flat = new FullJitterBackOff(BASE, BASE);

        assertThat(flat.ceilingMillis(1)).isEqualTo(BASE.toMillis());
    }

    @Test
    void ceilingNeverExceedsTheCapForAnyAttempt() {
        for (int attempt : new int[] {0, -1, 1, 11, 29, 30, 31, 63, 64, Integer.MAX_VALUE,
                Integer.MIN_VALUE}) {
            assertThat(backOff.ceilingMillis(attempt))
                    .isBetween(1L, CAP.toMillis());
        }
    }

    /**
     * 0 이나 음수는 1 로 본다. 지연 계산이 부르는 쪽의 산술 실수로 예외를 던져
     * <b>발행을 막는 것</b>이 더 나쁘다.
     */
    @Test
    void nonPositiveAttemptIsTreatedAsTheFirstRetry() {
        long first = backOff.ceilingMillis(1);

        assertThat(backOff.ceilingMillis(0)).isEqualTo(first);
        assertThat(backOff.ceilingMillis(-1)).isEqualTo(first);
        assertThat(backOff.ceilingMillis(Integer.MIN_VALUE)).isEqualTo(first);
    }

    // ── 뽑기 ─────────────────────────────────────────────────────────────

    @Test
    void drawsStayWithinTheCeiling() {
        for (int attempt = 1; attempt <= 8; attempt++) {
            long ceiling = backOff.ceilingMillis(attempt);
            for (int i = 0; i < 200; i++) {
                assertThat(backOff.nextDelay(attempt).toMillis()).isBetween(0L, ceiling);
            }
        }
    }

    /**
     * <b>이 테스트가 이 클래스의 존재 이유다.</b> 고정 지연이었을 때는 값이 하나였고,
     * 그래서 같이 실패한 것들이 같이 돌아왔다.
     *
     * <p>임계값이 기대값({@code ≈481})에서 표준편차 90배 아래라 확률적으로 안전하다.
     */
    @Test
    void spreadsValuesInsteadOfReturningOne() {
        Set<Long> seen = new HashSet<>();
        IntStream.range(0, 500).forEach(i -> seen.add(backOff.nextDelay(5).toMillis()));

        assertThat(seen).hasSizeGreaterThan(100);
    }

    // ── 생성 ─────────────────────────────────────────────────────────────

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
        assertThatThrownBy(() -> new FullJitterBackOff(null, CAP))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>양수인데 환산하면 0 이 되는 구간이 있다.</b> 그대로 두면 상한이 0 이라 지터가
     * 사라지고 재시도가 즉시 실행된다 — 조용히 정반대로 동작한다.
     */
    @Test
    void rejectsSubMillisecondBoundsThatWouldTruncateToZero() {
        assertThatThrownBy(() -> new FullJitterBackOff(Duration.ofNanos(1), CAP))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("1ms");
        assertThatThrownBy(() -> new FullJitterBackOff(Duration.ofNanos(999_999), CAP))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 저장소의 지연 변환기가 365일 위에서 던지는데, 그것은 첫 실패가 났을 때야 터진다. */
    @Test
    void rejectsBoundsBeyondWhatTheAdapterCanPersist() {
        assertThatThrownBy(() -> new FullJitterBackOff(BASE, Duration.ofDays(366)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("365일");
    }
}
