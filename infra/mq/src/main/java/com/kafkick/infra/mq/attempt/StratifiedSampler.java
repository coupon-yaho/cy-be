package com.kafkick.infra.mq.attempt;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.kafkick.core.observation.EventType;
import com.kafkick.core.observation.IssuanceFlowEvent;

/**
 * 결과 코드별 층화 샘플링. DEC-04. 계약과 반대 방향 실패는 {@link AttemptSamplingProperties} 에 적었다.
 *
 * <h2>통과 조건은 둘의 OR 다</h2>
 *
 * <pre>
 * 통과 = (층 카운터 &lt; 최소 보장)  또는  (전체 카운터 &lt; 전체 상한)
 * </pre>
 *
 * <p><b>우선순위가 아니라 논리합이다.</b> 처음에는 이걸 "최소 보장을 먼저 본다" 는 순서
 * 규칙으로 적었는데, 두 분기의 본문이 같아서 순서를 뒤집어도 동작이 한 글자도 안 바뀐다
 * (일부러 뒤집어 돌려 보고 확인했다 — 테스트 5 개가 그대로 초록이었다). 순서를 계약이라고
 * 적어 두면 다음 사람이 지킬 것이 없는 규칙을 지키게 된다.
 *
 * <p>실제로 드문 결과를 살리는 것은 <b>왼쪽 항이 전체 상한을 보지 않는다</b>는 사실이다.
 * 흔한 결과가 상한을 다 써도 새 층은 왼쪽 항으로 들어온다 — 재고 소진 구간에서 화면이
 * 409 로만 채워지지 않는 이유가 그것이다.
 *
 * <h2>층은 (eventType, httpStatus) 다</h2>
 *
 * {@code reasonCode} 가 아니다. 화면이 채워지는 방식을 정하는 것은 HTTP 결과이고,
 * {@code reasonCode} 는 4xx·5xx 에만 있어서 성공과 단계 이벤트를 층으로 가르지 못한다.
 * {@code httpStatus} 가 없는 이벤트({@code ISSUE_ATTEMPT} · {@code QUEUE_ADMITTED})는
 * {@code null} 을 키의 일부로 그대로 쓴다 — 그 둘도 각자 하나의 층이다.
 *
 * <h2>창은 1 초이고 경계에서 통째로 리셋된다</h2>
 *
 * 슬라이딩 창이 아니다. 창이 넘어가는 순간 카운터가 0 이 되므로, 경계에 걸친 두 초에는
 * 순간적으로 상한의 2 배까지 통과할 수 있다. 화면이 1 초 폴링이라 그 편차가 관측되지 않고,
 * 슬라이딩 창을 유지하는 비용(건별 타임스탬프 큐)이 이 경로에 붙을 이유가 없다.
 *
 * <h2>동기화</h2>
 *
 * 리스너 {@code concurrency} 가 1 보다 크면 여러 스레드가 동시에 들어온다. 카운터를 원자
 * 변수들로 흩으면 "층 카운터는 올랐는데 전체 카운터는 다른 창의 것" 같은 상태가 생겨 상한이
 * 조용히 새므로, 판정 전체를 한 잠금 안에서 한다. 이 메서드는 Redis I/O 앞단이라 잠금 비용이
 * 뒤따르는 네트워크 왕복에 묻힌다.
 */
public final class StratifiedSampler {

    private final Clock clock;
    private final int minPerStratum;
    private final int maxPerSecond;
    private final int maxStrata;

    private final Map<Stratum, Integer> stratumCounts = new HashMap<>();
    private long windowSecond = Long.MIN_VALUE;
    private int windowCount;

    public StratifiedSampler(AttemptSamplingProperties properties, Clock clock) {
        Objects.requireNonNull(properties, "properties");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.minPerStratum = properties.resolvedMinPerStratumPerSecond();
        this.maxPerSecond = properties.resolvedMaxPerSecond();
        this.maxStrata = properties.resolvedMaxStrata();
    }

    /**
     * 이 이벤트를 화면 버퍼에 넣을지 정한다.
     *
     * @param event 도착한 이벤트
     * @return 통과시키면 true
     */
    public synchronized boolean sample(IssuanceFlowEvent event) {
        Objects.requireNonNull(event, "event");
        rollWindowIfNeeded();

        Stratum stratum = new Stratum(event.eventType(), event.httpStatus());
        Integer seen = stratumCounts.get(stratum);
        if (seen == null && stratumCounts.size() >= maxStrata) {
            // 층 상한을 넘었다. 최소 보장 없이 전체 상한만 적용한다 — 여기서 새 층을 계속
            // 만들면 층 수 × 최소 보장이 곱해져 전체 상한이 아무것도 뜻하지 않게 된다.
            return admitWithinGlobalCap();
        }

        int stratumCount = seen == null ? 0 : seen;
        boolean withinStratumMinimum = stratumCount < minPerStratum;
        if (!withinStratumMinimum && windowCount >= maxPerSecond) {
            return false;
        }
        stratumCounts.put(stratum, stratumCount + 1);
        windowCount++;
        return true;
    }

    private boolean admitWithinGlobalCap() {
        if (windowCount >= maxPerSecond) {
            return false;
        }
        windowCount++;
        return true;
    }

    /**
     * 창이 바뀌었으면 카운터를 통째로 버린다.
     *
     * <p>{@code stratumCounts} 를 {@code clear()} 하는 것이지 값만 0 으로 되돌리는 것이 아니다.
     * 값만 되돌리면 한 번이라도 나타난 층이 영원히 맵에 남아, 상태 코드가 한 번 퍼진 뒤로는
     * {@code maxStrata} 가 계속 차 있는 상태가 된다 — 그 뒤에 나타난 진짜 장애 층이 최소
     * 보장을 못 받는다.
     */
    private void rollWindowIfNeeded() {
        long now = Math.floorDiv(clock.millis(), 1_000L);
        if (now != windowSecond) {
            windowSecond = now;
            windowCount = 0;
            stratumCounts.clear();
        }
    }

    /**
     * 층의 키. {@code httpStatus} 는 {@code null} 이 유효한 값이라 그대로 싣는다 —
     * 0 이나 -1 로 바꿔 적으면 실제 상태 코드와 구분되지 않는다.
     */
    private record Stratum(EventType eventType, Integer httpStatus) {
    }
}
