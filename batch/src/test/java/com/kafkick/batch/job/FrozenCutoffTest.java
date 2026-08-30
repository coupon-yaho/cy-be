// 정리 잡의 컷오프 동결을 컨테이너 없이 잽니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.infrastructure.item.ExecutionContext;

/**
 * <b>통합 테스트로는 이 축을 못 잰다.</b> {@code CleanupJobTest} 는 {@code Clock} 을
 * {@code AS_OF} 에 고정하므로 청크가 몇 번을 돌든 {@code now()} 가 같고, 그 픽스처에서는
 * 두 Step 의 태스클릿이 <b>각각 한 번씩만</b> 호출된다. 그래서
 * <b>{@code if (!containsKey)} 만 지우는 회귀</b>가 그쪽에서는 살아남는다 — 키도 있고
 * 값도 같기 때문이다. 실제로 그 돌연변이를 심어 통합 테스트가 초록인 것을 확인했다.
 *
 * <p>여기서는 <b>계산이 몇 번 불렸나</b>를 센다. 그 회귀가 이 자리에서 죽는다.
 */
class FrozenCutoffTest {

    private static final LocalDateTime FIRST = LocalDateTime.of(2026, 4, 1, 9, 0);

    /** 부를 때마다 1분씩 나아간다 — 안 얼면 값이 갈린다. */
    private static Supplier<LocalDateTime> ticking(AtomicInteger calls) {
        return () -> FIRST.plusMinutes(calls.getAndIncrement());
    }

    @Test
    @DisplayName("계산은 한 번만 부른다 — if 를 지우는 회귀가 여기서 죽는다")
    void computesOnlyOnce() {
        ExecutionContext context = new ExecutionContext();
        AtomicInteger calls = new AtomicInteger();
        Supplier<LocalDateTime> clock = ticking(calls);

        CleanupJobConfig.frozenCutoff(context, "cleanup.probe", clock);
        CleanupJobConfig.frozenCutoff(context, "cleanup.probe", clock);
        CleanupJobConfig.frozenCutoff(context, "cleanup.probe", clock);

        assertThat(calls)
                .as("청크마다 다시 잡으면 드레인이 길어질수록 기준이 앞으로 밀린다 — "
                        + "Step 1 에서는 그것이 도는 검증의 입력(asof_state)을 걷는다")
                .hasValue(1);
    }

    @Test
    @DisplayName("두 번째부터는 첫 값을 그대로 돌려준다")
    void returnsTheFirstValueForever() {
        ExecutionContext context = new ExecutionContext();
        Supplier<LocalDateTime> clock = ticking(new AtomicInteger());

        LocalDateTime first = CleanupJobConfig.frozenCutoff(context, "cleanup.probe", clock);

        assertThat(CleanupJobConfig.frozenCutoff(context, "cleanup.probe", clock))
                .isEqualTo(first)
                .isEqualTo(FIRST);
    }

    /**
     * 키가 다르면 서로 안 섞인다. <b>두 Step 이 문맥을 공유해서가 아니다</b> — 각자
     * {@code StepExecution} 문맥을 쓰므로 키가 같아도 안 섞인다. 이것은 한 문맥 안에서
     * 키 둘을 쓸 때의 성질이고, Job 문맥으로 올리는 날 실제로 필요해진다.
     */
    @Test
    @DisplayName("키가 다르면 따로 얼린다")
    void freezesEachKeyOnItsOwn() {
        ExecutionContext context = new ExecutionContext();
        Supplier<LocalDateTime> clock = ticking(new AtomicInteger());

        LocalDateTime one = CleanupJobConfig.frozenCutoff(context, "cleanup.one", clock);
        LocalDateTime two = CleanupJobConfig.frozenCutoff(context, "cleanup.two", clock);

        assertThat(one).isEqualTo(FIRST);
        assertThat(two).isEqualTo(FIRST.plusMinutes(1));
    }
}
