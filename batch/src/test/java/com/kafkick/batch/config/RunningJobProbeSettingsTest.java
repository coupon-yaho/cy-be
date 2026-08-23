// 시체 판정 임계가 Step 데드라인들과의 관계를 기동 때 지키는지 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>가드가 지키는 것은 "살아 있다" 와 "죽었다" 가 같은 숫자를 안 쓰는 것이다.</b>
 * {@code verifyJob} 의 Step 열 개는 단발 태스클릿이고 정리 Step 은 하트비트가 청크 커밋에만
 * 뛰므로, {@code stuck-job-after-ms} 를 그 데드라인들 아래로 내리면 <b>정상적으로 오래 도는
 * 실행이 시체로 판정된다</b> — 그러면 만료가 검증 한복판을 지나가거나
 * ({@code asOf} 를 영구히 못 쓰게 된다) 살아 있는 정리가 {@code BatchStuckExecution} 으로
 * 신고된다.
 *
 * <p><b>이 검사 자체가 무보증이었다.</b> 잡이 늘 때마다 생성자 인자가 하나씩 느는 구조인데
 * (그것이 이 가드의 값이다), 세 팔 중 어느 것을 지워도 전부 초록이었다.
 *
 * <p><b>컨텍스트를 안 띄운다.</b> 검사는 생성자 안에 있고 {@code JobRepository} 는 거기서
 * 안 쓰인다 — {@code CleanupJobSettingsTest}·{@code ExpireJobSettingsTest} 와 같은 자리다.
 */
class RunningJobProbeSettingsTest {

    private static final long STUCK_AFTER = 1_800_000L;

    @Test
    @DisplayName("기본값 조합은 기동한다 — 가드가 정상까지 막으면 안 된다")
    void acceptsDefaults() {
        assertThatCode(() -> probe(STUCK_AFTER, 600_000L, 120_000L, 120_000L))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("검증 Step 데드라인이 시체 임계를 넘으면 기동하지 못한다")
    void rejectsVerifyTimeoutAboveStuckAfter() {
        assertThatThrownBy(() -> probe(120_000L, 600_000L, 60_000L, 60_000L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.verify.step-timeout-ms");
    }

    @Test
    @DisplayName("만료 Step 데드라인이 시체 임계를 넘으면 기동하지 못한다")
    void rejectsExpireTimeoutAboveStuckAfter() {
        assertThatThrownBy(() -> probe(120_000L, 60_000L, 600_000L, 60_000L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.expire.step-timeout-ms");
    }

    /**
     * CY-397 이 더한 팔이다. 이것이 빠지면 {@code CLEANUP_STEP_TIMEOUT_MS} 를 40분으로
     * 올리는 것만으로 살아 있는 정리가 시체가 된다 — 기동은 성공하고 04:30 만 이상해진다.
     */
    @Test
    @DisplayName("정리 Step 데드라인이 시체 임계를 넘으면 기동하지 못한다")
    void rejectsCleanupTimeoutAboveStuckAfter() {
        assertThatThrownBy(() -> probe(120_000L, 60_000L, 60_000L, 600_000L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.cleanup.step-timeout-ms");
    }

    /** 같으면 안 된다. 붙여 놓으면 경계에서 두 판정이 같은 숫자를 쓴다. */
    @Test
    @DisplayName("시체 임계가 가장 긴 Step 데드라인과 같아도 기동하지 못한다")
    void rejectsEqualBoundary() {
        assertThatThrownBy(() -> probe(600_000L, 600_000L, 120_000L, 120_000L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static RunningJobProbe probe(long stuckAfterMs, long verifyMs, long expireMs,
            long cleanupMs) {
        return new RunningJobProbe(null, stuckAfterMs, verifyMs, expireMs, cleanupMs);
    }
}
