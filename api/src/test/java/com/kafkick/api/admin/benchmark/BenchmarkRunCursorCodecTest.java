// Benchmark cursor 가 왕복하고, 비정규·계약 위반 토큰을 하나의 오류로 거절하는지입니다.
package com.kafkick.api.admin.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.kafkick.core.benchmark.BenchmarkRunPosition;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

/**
 * <b>이 코덱에는 시험이 없었다.</b> 형제 넷 중 유일하게 파일 자체가 없었고, 그래서
 * <b>정규 인코딩 검사를 통째로 지워도 {@code :api:test} 가 초록이었다</b>(실측).
 *
 * <p>형제 {@code IssuanceInquiryCursorCodecTest} 와 같은 모양으로 만든다 —
 * 왕복 한 건 + 잘못된 토큰을 <b>하나의 오류</b>로 접는지 목록으로.
 */
class BenchmarkRunCursorCodecTest {

    private static final String INVALID_CURSOR_MESSAGE = "유효하지 않은 Benchmark cursor입니다.";

    private final BenchmarkRunCursorCodec codec = new BenchmarkRunCursorCodec();

    @Test
    void roundTripsDeterministicUrlSafeCursor() {
        BenchmarkRunPosition position = new BenchmarkRunPosition(
                Instant.ofEpochSecond(0L, 123_456_789L), 42L);

        String cursor = codec.encode(position);

        assertThat(codec.decode(cursor)).isEqualTo(position);
        assertThat(cursor).as("패딩을 안 붙인다 — 붙이면 자기 디코더가 거절한다")
                .doesNotContain("=");
        assertThat(cursor).as("질의 문자열에 그대로 실을 수 있어야 한다")
                .matches("[A-Za-z0-9_-]+");
        assertThatThrownBy(() -> codec.encode(null))
                .isInstanceOf(NullPointerException.class);
    }

    /**
     * <b>무엇이 틀렸든 같은 오류로 접는다.</b> 오류를 갈라 주면 그 자체가 토큰의
     * 내부 형식을 알려 주는 신호가 된다.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidCursors")
    void rejectsEveryInvalidFormAsOneInputError(String scenario, String cursor) {
        assertThatThrownBy(() -> codec.decode(cursor))
                .isInstanceOfSatisfying(BusinessException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(CommonErrorCode.INVALID_INPUT);
                    assertThat(exception).hasMessage(INVALID_CURSOR_MESSAGE);
                });
    }

    /**
     * ⚠️ <b>nano 범위는 여기 없다 — 이 코덱이 안 막기 때문이다.</b>
     *
     * <p>형제 {@code IssuanceHistoryCursorCodec} 은 {@code v1|0|-1|1} 과
     * {@code v1|0|1000000000|1} 을 거절하는데 이쪽은 통과시킨다.
     * {@code Instant.ofEpochSecond(sec, nano)} 가 범위 밖 nano 를 <b>정규화해서</b>
     * 받아들이기 때문이다 — {@code -1} 은 직전 초의 999,999,999 가 된다.
     *
     * <p><b>이 시험에서 그 케이스를 단언하지 않는다.</b> 없는 계약을 시험이 만들어
     * 내면 안 된다. 다만 그 차이는 실재하고, 결과는 <b>encode 가 만들 수 없는 위치로
     * 디코딩된다</b>는 것이다(왕복이 안 닫힌다). 고칠지는 별도 판단이다.
     */
    private static Stream<Arguments> invalidCursors() {
        String canonical = encoded("v1|0|123456789|42");
        return Stream.of(
                Arguments.of("null", null),
                Arguments.of("blank", " "),
                Arguments.of("too long", "a".repeat(257)),
                Arguments.of("invalid Base64 URL", "%%%"),
                Arguments.of("padded", canonical + "="),
                // **이 케이스가 정규 검사를 유일하게 태운다.** 빼면 그 검사를 통째로
                // 지워도 초록이다 — 나머지는 다른 가드가 대신 잡는다.
                Arguments.of("non-canonical unused bits", nonCanonical(canonical)),
                Arguments.of("wrong segment count", encoded("v1|0|0")),
                Arguments.of("unknown version", encoded("v2|0|0|1")),
                Arguments.of("non-numeric epochSecond", encoded("v1|now|0|1")),
                Arguments.of("epochSecond outside Instant range",
                        encoded("v1|31556889864403200|0|1")),
                Arguments.of("non-numeric runId", encoded("v1|0|0|id")),
                Arguments.of("zero runId", encoded("v1|0|0|0")),
                Arguments.of("negative runId", encoded("v1|0|0|-1")));
    }

    /**
     * 같은 바이트열로 디코딩되는 <b>다른</b> 문자열.
     *
     * <p>Base64 마지막 글자는 남는 비트를 싣고 디코더가 그것을 버린다 — 그 자리를
     * 바꿔도 결과가 같다. 정규 검사가 없으면 통과한다.
     */
    private static String nonCanonical(String canonical) {
        String head = canonical.substring(0, canonical.length() - 1);
        byte[] want = Base64.getUrlDecoder().decode(canonical);
        for (char c : "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
                .toCharArray()) {
            String candidate = head + c;
            if (!candidate.equals(canonical)
                    && Arrays.equals(Base64.getUrlDecoder().decode(candidate), want)) {
                return candidate;
            }
        }
        throw new IllegalStateException("비정규 변형을 못 만들었습니다: " + canonical);
    }

    private static String encoded(String payload) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
    }
}
