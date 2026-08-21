// 만료 잡 설정값이 기동 시점에 걸러지는지 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * <b>두 값이 틀리면 잡이 조용히 이상해진다.</b> 그때 가서는 아무도 모르므로 기동 시점에 막는다.
 *
 * <p><b>{@code chunk-size = 0} 이 특히 조용하다.</b> MySQL 은 {@code LIMIT 0} 을 오류로 보지
 * 않고 0건을 돌려주므로, 첫 청크가 곧 종료 신호가 되어 <b>5분마다 정상 완료로 보이면서 아무것도
 * 안 한다.</b> {@code asOf} 를 빠뜨렸을 때와 같은 실패 모드인데, 그쪽은 파라미터 검증기가 막고
 * 있으니 이쪽도 막는다.
 *
 * <p>감지 수단이 없다는 것이 근거다 — 잡은 {@code COMPLETED} 라 스케줄러가 로그를 안 남기고,
 * {@code BatchJobNotRunning} 알림은 잡이 <i>도는지</i>만 보므로 울리지 않는다.
 * 검증도 안 잡는다(만료 지연은 finding 이 아니라 관측 지표로 정해 뒀다).
 *
 * <p><b>컨텍스트를 안 띄운다.</b> 이 검사는 생성자 안에 있고, 그것을 확인하는 데 컨테이너가
 * 필요하지 않다. {@code ResolvedBatchConfigTest} 는 키가 <i>해석되는지</i>를 보는 자리이고
 * 거기서는 빈을 만들지 않으므로 이 검사가 걸리지 않는다 — 두 테스트가 보는 축이 다르다.
 */
class ExpireJobSettingsTest {

    private static final long VALID_TIMEOUT = 120_000L;
    private static final int VALID_CHUNK = 1000;

    @Test
    @DisplayName("chunk-size 가 0 이면 기동하지 못한다")
    void rejectZeroChunkSize() {
        assertThatThrownBy(() -> new ExpireJobConfig(null, null, 0, VALID_TIMEOUT))
                .as("LIMIT 0 은 오류 없이 0건을 돌려줘 만료가 조용히 멈춘다")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.expire.chunk-size");
    }

    @Test
    @DisplayName("chunk-size 가 음수여도 기동하지 못한다")
    void rejectNegativeChunkSize() {
        assertThatThrownBy(() -> new ExpireJobConfig(null, null, -1, VALID_TIMEOUT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.expire.chunk-size");
    }

    /**
     * 반대 방향의 사고다. 너무 짧으면 정상 청크가 끊겨 5분마다 실패한다 —
     * 조용하지는 않지만 만료가 진행되지 않는 것은 같다.
     */
    @Test
    @DisplayName("step-timeout-ms 가 1초 미만이면 기동하지 못한다")
    void rejectTooShortStepTimeout() {
        assertThatThrownBy(() -> new ExpireJobConfig(null, null, VALID_CHUNK, 500L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.expire.step-timeout-ms");
    }

    @Test
    @DisplayName("설정 파일의 기본값 조합은 통과한다")
    void acceptDefaults() {
        assertThatCode(() -> new ExpireJobConfig(null, null, VALID_CHUNK, VALID_TIMEOUT))
                .as("application.yml.example 의 기본값이 스스로 막히면 안 된다")
                .doesNotThrowAnyException();
    }
}
