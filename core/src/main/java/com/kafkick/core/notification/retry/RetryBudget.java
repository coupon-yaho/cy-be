// 재시도 일정이 최악으로 얼마나 오래 끄는지 셉니다. 마감이 있는 쪽이 그것을 알아야 합니다.
package com.kafkick.core.notification.retry;

import java.time.Duration;

/**
 * <b>재시도 일정이 최악으로 잡아먹는 시간.</b>
 *
 * <h2>⚠️ 아직 부르는 곳이 없다</h2>
 *
 * <p>이 클래스를 <b>기동 때 부르는 빈이 없다.</b> 그것을 붙이는 것은 마감이 있는 첫
 * 사용처가 들어오는 티켓이고(아래), 그때까지 여기 있는 것은 <b>계산과 그 계산을 못 박는
 * 시험</b>뿐이다.
 *
 * <p><b>그래도 값을 하는 것은 시험 쪽이다.</b> {@link NotificationRetryBackOffProperties} 의
 * javadoc 이 설계 교환을 정당화하며 <b>최악 85.2초</b>를 근거로 드는데, 지금까지 그 수를
 * 지키는 것이 아무것도 없었다 — {@code cap} 을 60초로 올리면 그 값이 <b>162초(1.90배)</b>가
 * 되는데 문서만 조용히 거짓이 된다. {@code RetryBudgetTest} 가 그것을 매 빌드마다 잰다.
 *
 * <h2>저장소가 스스로 남긴 미완의 계약</h2>
 *
 * <p>{@link NotificationRetryBackOffProperties} 가 이렇게 적어 뒀다.
 *
 * <blockquote>
 * 마감이 있는 쪽(예: 소비자에게 SLA 가 붙은 발행)에 이 백오프를 쓰게 되면
 * <b>{@code 10 × cap < 마감} 을 기동 시 검증해야 한다</b>.
 * </blockquote>
 *
 * <p>그 검증은 없다. 알림에는 마감이 없어서다.
 *
 * <p><b>그 규칙의 진짜 결함은 "과대평가" 가 아니다.</b> {@code 10 × cap} 은 대기를 크게
 * 잡으므로 그것만 보면 <b>보수적</b>이다(200초가 들면 85.2초는 확실히 든다) — 정상 구성을
 * 거절할 뿐 사고를 통과시키지 않는다. 결함은 <b>요청 자체의 시간을 안 세는 것</b>이고,
 * 그 경계는 계산된다.
 *
 * <pre>
 * 한 번의 시도 비용이 (200,000 - 85,200) / 10 = 11,480ms 를 넘으면
 *   → 10 × cap 규칙은 "든다" 고 말하는데 실제로는 안 든다
 *   예) 비용 15초 → 실제 235.2초, 규칙은 200초로 보고, 마감 210초를 통과시킨다
 * </pre>
 *
 * <h2>마감이 있는 첫 사용처</h2>
 *
 * <p>융합프로젝트 <b>사전예약 시스템 PRD v2.0</b>(2026-09-02)이 대사 배치에 외부 취소 API
 * 호출을 들여오고, 실패하면 재시도하게 한다. 그 배치 Step 에는
 * {@code batch.<잡>.step-timeout-ms} 라는 마감이 있다 — 재시도 총합이 그것을 넘으면 청크가
 * <b>전량 롤백</b>되어 진도가 0 이 된다.
 *
 * <p><b>그다음 시체 판정까지 가지는 않는다.</b> 이 저장소의 {@code stuck-job-after-ms} 는
 * 30분이고 Step 데드라인은 120~600초라, {@code docs/15} 가 실측으로 적어 둔 그대로
 * <i>"그 전에 Step 데드라인이 잡을 죽인다 — 시체 오판 위험이 구조적으로 낮다"</i> 이다.
 * 한때 이 자리에 시체로 이어진다고 적었는데 <b>그 문서의 결론과 어긋났다.</b>
 */
public final class RetryBudget {

    /**
     * 셀 수 있는 실패 횟수 상한. <b>오버플로가 fail-open 이라 막는다</b> —
     * {@code long} 누적이 감기면 음수가 되고, {@code fitsWithin} 이 그것을 마감보다 작다고
     * 읽어 <b>일정이 우주적으로 길 때 정확히 그때만 통과</b>시킨다. 상한이 없는 것보다 나쁘다.
     *
     * <p>이 저장소가 실제로 강제하는 값은 10 이다. 1000 은 그보다 두 자릿수 여유이고,
     * {@code cap} 이 365일이어도 {@code long} 이 안 넘친다.
     */
    static final int MAX_FAILURE_COUNT_LIMIT = 1000;

    private final FullJitterBackOff backOff;

    public RetryBudget(FullJitterBackOff backOff) {
        if (backOff == null) {
            throw new IllegalArgumentException("backOff 는 필수입니다.");
        }
        this.backOff = backOff;
    }

    /**
     * <b>기다리는 횟수는 상한보다 하나 적다.</b> 마지막 실패는 {@code failure_count} 를
     * 상한으로 올려 그 자리에서 종착으로 보내므로, 그 지연은 <b>쓰이지 않는다</b> —
     * {@code NotificationOutboxRepositoryImpl} 이 {@code nextFailureCount >= 상한} 에서
     * {@code DEAD} 로 보낸다.
     *
     * @param failureCountLimit 그 값에 닿으면 종착으로 보내는 실패 횟수
     * @return 최악의 누적 <b>대기</b>. 시도 자체의 비용은 안 들어 있다 —
     *         마감과 비교하려면 {@link #worstCaseTotal} 을 쓴다
     */
    public Duration worstCaseTotalWait(int failureCountLimit) {
        if (failureCountLimit < 1 || failureCountLimit > MAX_FAILURE_COUNT_LIMIT) {
            throw new IllegalArgumentException(
                    "failureCountLimit 은 1 이상 " + MAX_FAILURE_COUNT_LIMIT
                            + " 이하여야 합니다. 받은 값=" + failureCountLimit);
        }
        long total = 0;
        for (int attempt = 1; attempt < failureCountLimit; attempt++) {
            // 회차별 상한을 그대로 더한다. nextDelay 는 [0, 상한] 의 난수라 최악이 상한이다.
            total = Math.addExact(total, backOff.ceilingMillis(attempt));
        }
        return Duration.ofMillis(total);
    }

    /**
     * <b>마감과 맞댈 값.</b> 대기에 <b>시도마다의 비용</b>을 더한다.
     *
     * @param perAttemptCost <b>한 번의 실패가 벽시계로 잡아먹는 최대 시간.</b> 동기 호출이면
     *        요청 타임아웃이지만, <b>그것이 유일한 형태가 아니다</b> — 이 저장소의 알림은
     *        lease 로도 실패를 센다({@code recoverExpiredClaims}). 그 경로에서 한 번의
     *        실패는 요청이 끝나서가 아니라 <b>lease 가 만료돼서</b> 오르므로 비용이
     *        <b>lease 길이</b>다(기본 30초). 요청 타임아웃을 넣으면 그만큼 과소평가하고,
     *        <b>과소평가는 사고를 통과시킨다</b> — 이 클래스가 막으려는 방향이다
     * @param failureCountLimit 시도 횟수. 대기보다 한 번 많다 — 요청은 매번 나간다
     */
    public Duration worstCaseTotal(int failureCountLimit, Duration perAttemptCost) {
        if (perAttemptCost == null || perAttemptCost.isNegative()) {
            throw new IllegalArgumentException("perAttemptCost 는 음수가 아니어야 합니다.");
        }
        return worstCaseTotalWait(failureCountLimit)
                .plus(perAttemptCost.multipliedBy(failureCountLimit));
    }

    /**
     * <b>마감 안에 드는가.</b> 이미 지난 마감(음수)은 <b>아무 일정도 못 든다</b> —
     * 그것이 이 비교의 정의이고, 우연이 아니라 계약이다.
     */
    public boolean fitsWithin(int failureCountLimit, Duration perAttemptCost, Duration deadline) {
        if (deadline == null) {
            throw new IllegalArgumentException("deadline 은 필수입니다.");
        }
        return worstCaseTotal(failureCountLimit, perAttemptCost).compareTo(deadline) < 0;
    }
}
