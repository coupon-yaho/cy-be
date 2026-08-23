// 만료 잡 설정값이 기동 시점에 걸러지는지 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * {@code ExpireNotSucceeding} 알림은 잡이 <i>성공했는지</i>만 보므로 울리지 않는다.
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
        // 0 이어야 이 갈래를 실제로 밟는다. 500 은 "1000 의 배수가 아니다" 쪽에 먼저 걸려
        // 하한 검사를 지워도 통과했다 — 테스트 이름이 주장하는 것과 거는 가드가 달랐다.
        assertThatThrownBy(() -> new ExpireJobConfig(null, null, VALID_CHUNK, 0L))
                .as("0 은 1000 의 배수라 배수 검사로는 안 걸린다. 하한이 유일한 방어다")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.expire.step-timeout-ms");
    }

    /**
     * <b>경계 자체를 찍는다.</b> 위 두 테스트는 1000 <i>미만</i>과 1000 의 <i>배수가 아닌</i> 값만
     * 본다. 조건이 {@code < 1000} 에서 {@code <= 1000} 으로 바뀌면 그것들은 그대로 통과하고,
     * 정확히 1초짜리 설정만 조용히 거부된다.
     */
    @Test
    @DisplayName("step-timeout-ms 가 정확히 1000 이면 통과한다 — 경계는 열려 있다")
    void acceptExactlyOneSecondStepTimeout() {
        assertThatCode(() -> new ExpireJobConfig(null, null, VALID_CHUNK, 1_000L))
                .doesNotThrowAnyException();
    }

    /** {@code chunk-size} 도 같다. 1 은 거부가 아니라 허용이어야 한다. */
    @Test
    @DisplayName("chunk-size 가 1 이면 통과한다 — 경계는 열려 있다")
    void acceptChunkSizeOfOne() {
        assertThatCode(() -> new ExpireJobConfig(null, null, 1, VALID_TIMEOUT))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("step-timeout-ms 가 1000 의 배수가 아니면 기동하지 못한다")
    void rejectStepTimeoutWithLostPrecision() {
        assertThatThrownBy(() -> new ExpireJobConfig(null, null, VALID_CHUNK, 1_500L))
                .as("스프링 트랜잭션 타임아웃은 초 단위라 나머지가 조용히 버려진다")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.expire.step-timeout-ms");
    }

    /**
     * <b>기본값이라는 주장을 파일에서 확인한다.</b> 예전에는 이 상수 둘이 하드코딩이었고,
     * {@code application.yml.example} 에서 값을 바꾸면 <i>테스트는 통과하는데 주석만 거짓</i>이
     * 되었다. 이제 파일에서 읽으므로 갈리면 여기서 운다.
     *
     * <p>읽는 것은 {@code build.gradle} 의 {@code processTestResources} 가 복사한
     * {@code resolved/application.yml} 이다 — 원본이 {@code .example} 이라 확장자로 파서를
     * 못 고른다({@code ResolvedBatchConfigTest} 와 같은 자리).
     */
    @Test
    @DisplayName("설정 파일에 적힌 기본값 조합이 스스로 막히지 않는다")
    void acceptDefaultsFromTheSettingsFile() throws IOException {
        int chunkSize = Integer.parseInt(defaultOf("EXPIRE_CHUNK_SIZE"));
        long stepTimeout = Long.parseLong(defaultOf("EXPIRE_STEP_TIMEOUT_MS"));

        assertThat(chunkSize)
                .as("여기가 갈리면 아래 단언이 '기본값' 이 아닌 값을 검사하게 된다")
                .isEqualTo(VALID_CHUNK);
        assertThat(stepTimeout).isEqualTo(VALID_TIMEOUT);
        assertThatCode(() -> new ExpireJobConfig(null, null, chunkSize, stepTimeout))
                .as("application.yml.example 의 기본값이 스스로 막히면 안 된다")
                .doesNotThrowAnyException();
    }

    /** {@code ${EXPIRE_CHUNK_SIZE:1000}} 의 콜론 뒤 기본값. */
    private String defaultOf(String variable) throws IOException {
        try (InputStream in = getClass().getResourceAsStream("/resolved/application.yml")) {
            if (in == null) {
                throw new IllegalStateException(
                        "resolved/application.yml 이 없다 — build.gradle 의 processTestResources 가 "
                                + "application.yml.example 을 복사하는 자리를 확인해라");
            }
            String yaml = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            Matcher matcher = Pattern.compile("\\$\\{" + variable + ":([^}]+)}").matcher(yaml);
            if (!matcher.find()) {
                throw new IllegalStateException(variable + " 의 기본값을 설정 파일에서 못 찾았다");
            }
            return matcher.group(1).trim();
        }
    }
}
