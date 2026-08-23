// 정리 잡 설정값이 기동 시점에 걸러지는지 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>가드는 그 자체가 안전장치라 안 재면 지워져도 아무도 모른다.</b> 넷 다 어겼을 때의
 * 결말이 <i>"조용히 이상해진다"</i> 라서, 발화 시점이 아니라 기동 시점에 막는다.
 *
 * <p><b>{@code chunk-size = 0} 이 특히 조용하다.</b> MySQL 은 {@code LIMIT 0} 을 오류로 보지
 * 않고 0건을 돌려주므로 첫 청크가 곧 종료 신호가 되고, 잡은 {@code COMPLETED} 로 닫힌다 —
 * {@code CleanupNotSucceeding} 도 안 울어서 <b>가장 무거운 테이블만 안 걷히는 상태가
 * 감시망을 통째로 통과한다.</b> {@code ExpireJobSettingsTest} 가 만료 쪽에서 같은 축을 잰다.
 *
 * <p><b>컨텍스트를 안 띄운다.</b> 이 검사들은 생성자 안에 있고 DB 와 아무 상관이 없다.
 * 그래서 값 검사를 Step 빈 메서드 파라미터가 아니라 생성자로 올려 뒀다 — 그러지 않으면
 * 잘못된 값 하나를 재는 데 Testcontainers MySQL 이 하나씩 뜬다.
 */
class CleanupJobSettingsTest {

    private static final long VALID_TIMEOUT = 120_000L;
    private static final int VALID_KEEP = 5;
    private static final int VALID_CHUNK = 10_000;
    private static final long VALID_ABANDONED_HOURS = 24L;

    @Test
    @DisplayName("기본값 조합은 기동한다 — 가드가 정상까지 막으면 안 된다")
    void acceptsDefaults() {
        assertThatCode(() -> config(VALID_TIMEOUT, VALID_KEEP, VALID_CHUNK, VALID_ABANDONED_HOURS))
                .doesNotThrowAnyException();
    }

    /**
     * 스프링의 트랜잭션 타임아웃은 초 단위라 999 는 <b>0 으로 내려앉는다.</b> 0 은 "무제한" 이
     * 아니라 <b>데드라인이 이미 지났음</b>이라, 첫 문장에서 {@code TransactionTimedOutException}
     * 이 난다 — 기동은 성공하고 04:30 만 매일 조용히 실패하는 모양이 된다.
     */
    @Test
    @DisplayName("step-timeout-ms 가 1000 미만이면 기동하지 못한다")
    void rejectSubSecondTimeout() {
        assertThatThrownBy(() -> config(999, VALID_KEEP, VALID_CHUNK, VALID_ABANDONED_HOURS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.cleanup.step-timeout-ms");
    }

    /** 0 은 1000 의 배수라 배수 검사로는 안 걸린다. 하한이 유일한 방어다. */
    @Test
    @DisplayName("step-timeout-ms 가 0 이어도 기동하지 못한다")
    void rejectZeroTimeout() {
        assertThatThrownBy(() -> config(0, VALID_KEEP, VALID_CHUNK, VALID_ABANDONED_HOURS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.cleanup.step-timeout-ms");
    }

    /** 1500 은 1 초로 잘려 <b>적어 준 값보다 짧게</b> 끊기는데, 그 사실이 어디에도 안 남는다. */
    @Test
    @DisplayName("step-timeout-ms 가 1000 의 배수가 아니면 기동하지 못한다")
    void rejectNonMultipleTimeout() {
        assertThatThrownBy(() -> config(1_500, VALID_KEEP, VALID_CHUNK, VALID_ABANDONED_HOURS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.cleanup.step-timeout-ms");
    }

    @Test
    @DisplayName("chunk-size 가 0 이면 기동하지 못한다")
    void rejectZeroChunkSize() {
        assertThatThrownBy(() -> config(VALID_TIMEOUT, VALID_KEEP, 0, VALID_ABANDONED_HOURS))
                .as("DELETE … LIMIT 0 은 오류 없이 0건을 돌려줘 잡이 성공으로 닫힌다")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.cleanup.chunk-size");
    }

    @Test
    @DisplayName("chunk-size 가 음수여도 기동하지 못한다")
    void rejectNegativeChunkSize() {
        assertThatThrownBy(() -> config(VALID_TIMEOUT, VALID_KEEP, -1, VALID_ABANDONED_HOURS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.cleanup.chunk-size");
    }

    /** 0 이면 방금 시작한 검증까지 "버려진 것" 으로 보고 <b>그 판정의 입력</b>을 걷는다. */
    @Test
    @DisplayName("abandoned-after-hours 가 0 이면 기동하지 못한다")
    void rejectZeroAbandonedWindow() {
        assertThatThrownBy(() -> config(VALID_TIMEOUT, VALID_KEEP, VALID_CHUNK, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.cleanup.abandoned-after-hours");
    }

    /** 0 이면 방금 끝난 판정의 파생 행까지 그날 밤에 사라져 되짚을 근거가 없다. */
    @Test
    @DisplayName("asof-state-keep-runs 가 0 이면 기동하지 못한다")
    void rejectZeroKeepRuns() {
        assertThatThrownBy(() -> config(VALID_TIMEOUT, 0, VALID_CHUNK, VALID_ABANDONED_HOURS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.verify.asof-state-keep-runs");
    }

    private static CleanupJobConfig config(long timeoutMillis, int keepRuns, int chunkSize,
            long abandonedAfterHours) {
        return new CleanupJobConfig(null, null, timeoutMillis, keepRuns, chunkSize,
                abandonedAfterHours);
    }
}
