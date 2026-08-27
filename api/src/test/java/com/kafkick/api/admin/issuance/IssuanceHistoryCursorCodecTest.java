package com.kafkick.api.admin.issuance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryQuery.HistoryPosition;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

/** 발급 이력 HTTP cursor의 결정론적 인코딩과 입력 오류 통일을 검증합니다. */
class IssuanceHistoryCursorCodecTest {

    private static final String INVALID_CURSOR_MESSAGE = "유효하지 않은 발급 이력 cursor입니다.";

    private final IssuanceHistoryCursorCodec codec = new IssuanceHistoryCursorCodec();

    /** 동일한 이력 위치를 padding 없는 고정 Base64 URL 문자열로 왕복하는지 검증합니다. */
    @Test
    @DisplayName("발급 이력 위치를 v1 payload의 padding 없는 Base64 URL cursor로 왕복한다")
    void encodesAndDecodesDeterministicUrlSafeCursor() {
        HistoryPosition position = new HistoryPosition(Instant.ofEpochSecond(0L, 123_456_789L), 42L);

        String first = codec.encode(position);
        String second = codec.encode(position);

        assertThat(first).isEqualTo("djF8MHwxMjM0NTY3ODl8NDI");
        assertThat(second).isEqualTo(first);
        assertThat(first).doesNotContain("=");
        assertThat(codec.decode(first)).isEqualTo(position);
    }

    /** 형식·버전·시각·ID가 잘못된 모든 입력을 같은 업무 오류로 거부하는지 검증합니다. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidCursors")
    @DisplayName("잘못된 cursor를 COMMON-001과 단일 상세 메시지로 거부한다")
    void rejectsEveryInvalidCursorWithUnifiedBusinessError(String scenario, String cursor) {
        assertThatThrownBy(() -> codec.decode(cursor))
                .isInstanceOfSatisfying(BusinessException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(CommonErrorCode.INVALID_INPUT);
                    assertThat(exception).hasMessage(INVALID_CURSOR_MESSAGE);
                });
    }

    /** cursor의 형식과 숫자 범위별 잘못된 입력을 제공합니다. */
    private static Stream<Arguments> invalidCursors() {
        return Stream.of(
                Arguments.of("null", null),
                Arguments.of("blank", " "),
                Arguments.of("too long", "a".repeat(257)),
                Arguments.of("invalid Base64 URL", "%%%"),
                Arguments.of("padded Base64 URL", encoded("v1|0|0|1") + "="),
                Arguments.of("wrong segment count", encoded("v1|0|0")),
                Arguments.of("unknown version", encoded("v2|0|0|1")),
                Arguments.of("non-numeric epochSecond", encoded("v1|now|0|1")),
                Arguments.of("epochSecond outside Instant range", encoded("v1|31556889864403200|0|1")),
                Arguments.of("negative nano", encoded("v1|0|-1|1")),
                Arguments.of("nano upper boundary exceeded", encoded("v1|0|1000000000|1")),
                Arguments.of("non-numeric historyId", encoded("v1|0|0|id")),
                Arguments.of("zero historyId", encoded("v1|0|0|0")),
                Arguments.of("negative historyId", encoded("v1|0|0|-1")));
    }

    /** 사람이 읽는 payload를 padding 없는 Base64 URL 테스트 입력으로 만듭니다. */
    private static String encoded(String payload) {
        return java.util.Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payload.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}
