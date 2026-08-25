package com.kafkick.infra.mq.attempt;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * live 층화 샘플링의 <b>설정 계약</b>. DEC-04.
 *
 * <p><b>여기 적힌 수치는 잠정값이다.</b> 이 티켓이 고정하는 것은 손잡이의 이름과 의미이지
 * 값이 아니다 — 값은 부하 배포 전에 실측으로 정한다. 계약을 먼저 박는 이유는, 값이 정해질 때
 * 코드를 고쳐야 하면 그 회차에 코드 변경과 튜닝이 섞여 어느 쪽이 결과를 바꿨는지 못 가르기
 * 때문이다.
 *
 * <h2>왜 샘플링이 필요한가 — 산수</h2>
 *
 * {@code attempt:live} 는 {@code MAXLEN ~ 200} 이다. 초당 attempt 가 3,000 건이면 200 개
 * 버퍼는 <b>67ms 분량</b>이다. 화면은 1 초마다 폴링하므로, 샘플링이 없으면 매 폴링이 직전
 * 67ms 창만 본다 — 표본을 고르는 것이 아니라 <b>도착 순서가 우연히 그 창에 걸린 것</b>만 본다.
 * 그 창은 결과 코드에 대해 균등하지 않다. 재고가 소진되면 그 구간의 거의 전부가 409 라
 * 화면이 409 로만 채워지고, 그때 봐야 하는 5xx 한 건이 그 사이에서 사라진다.
 *
 * <h2>두 손잡이는 목적이 다르다</h2>
 *
 * <ul>
 *   <li>{@code min-per-stratum-per-second} — <b>보이게 하는</b> 손잡이. 결과 코드마다 초당 이
 *       개수까지는 무조건 통과시킨다. 드문 결과가 흔한 결과에 밀려 사라지지 않게 한다.</li>
 *   <li>{@code max-per-second} — <b>줄이는</b> 손잡이. 전체 초당 상한. Redis 쓰기량과
 *       {@code MAXLEN} 회전 속도를 여기서 정한다.</li>
 * </ul>
 *
 * <p><b>둘은 충돌할 수 있고, 그때는 최소 보장이 이긴다.</b> 활성 층이 N 개면 실제 상한은
 * {@code max(maxPerSecond, N × minPerStratumPerSecond)} 다. 상한을 하드로 걸면 늦게 나타난
 * 층 — 정확히 새로 생긴 장애 — 이 그 초에 한 건도 못 들어간다. 그건 이 샘플링이 없애려던
 * 상태와 같으므로 반대로 골랐다. <b>대가는 상한이 상한이 아니라는 것</b>이고, 그래서 실제
 * 통과량을 {@code app.attempt.live.sampled} 로 내보내 값 검증을 사람이 아니라 지표가 하게 한다.
 *
 * <p>층의 개수도 상한이 있다({@code max-strata}). {@code eventType} 4 종 × HTTP 상태이므로
 * 정상 범위에서는 열댓 개를 넘지 않지만, 상태 코드가 예상 밖으로 퍼지면 최소 보장이 곱해져
 * 상한이 무의미해진다. 그 수를 넘은 층은 <b>최소 보장 없이</b> 전체 상한만 적용받는다.
 *
 * @param minPerStratumPerSecond 층별 초당 최소 통과 건수. 0 이면 최소 보장 없음
 * @param maxPerSecond 전체 초당 상한. 최소 보장이 이 값을 넘을 수 있다
 * @param maxStrata 최소 보장을 받는 층의 최대 개수
 */
@ConfigurationProperties("observation.attempt.live.sampling")
public record AttemptSamplingProperties(
        Integer minPerStratumPerSecond,
        Integer maxPerSecond,
        Integer maxStrata
) {

    /** 잠정값. 층 10개가 모두 활성이면 실제 상한이 초당 50 이 된다 — 위 충돌 규칙 참고. */
    public static final int DEFAULT_MIN_PER_STRATUM_PER_SECOND = 5;

    /** 잠정값. 1 초 폴링에 200 짜리 버퍼이므로, 화면이 놓치지 않는 범위의 위쪽을 잡았다. */
    public static final int DEFAULT_MAX_PER_SECOND = 100;

    /** 잠정값. eventType 4 종 × 정상 범위의 HTTP 상태를 덮는다. */
    public static final int DEFAULT_MAX_STRATA = 20;

    public AttemptSamplingProperties {
        minPerStratumPerSecond = requireAtLeast(minPerStratumPerSecond,
                DEFAULT_MIN_PER_STRATUM_PER_SECOND, 0, "min-per-stratum-per-second");
        maxPerSecond = requireAtLeast(maxPerSecond, DEFAULT_MAX_PER_SECOND, 1, "max-per-second");
        maxStrata = requireAtLeast(maxStrata, DEFAULT_MAX_STRATA, 1, "max-strata");
    }

    public int resolvedMinPerStratumPerSecond() {
        return minPerStratumPerSecond;
    }

    public int resolvedMaxPerSecond() {
        return maxPerSecond;
    }

    public int resolvedMaxStrata() {
        return maxStrata;
    }

    /**
     * 이 설정이 만들 수 있는 <b>최악의</b> 초당 통과량. 기동 로그가 이 값을 적는다.
     *
     * <p>{@code max-per-second} 만 읽고 Redis 쓰기 예산을 잡으면 실제와 어긋난다. 어긋나는
     * 방향이 항상 "예상보다 많다" 이므로 조용히 넘어가면 안 된다.
     */
    public long worstCasePerSecond() {
        // long 으로 곱한다. int 로 두면 두 값이 커질 때 조용히 음수가 되어, 경고하려던 자리가
        // "예상보다 적다" 를 보고하는 상태가 된다.
        return Math.max(maxPerSecond, (long) maxStrata * minPerStratumPerSecond);
    }

    private static int requireAtLeast(Integer value, int fallback, int minimum, String name) {
        int resolved = value == null ? fallback : value;
        if (resolved < minimum) {
            throw new IllegalArgumentException(
                    "observation.attempt.live.sampling." + name + " 는 " + minimum + " 이상이어야 합니다: " + resolved);
        }
        return resolved;
    }
}
