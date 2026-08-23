package com.kafkick.api.admin.issuance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryQuery.InquiryPosition;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryQuery.SourceKind;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

class IssuanceInquiryCursorCodecTest {

    private static final String INVALID_CURSOR_MESSAGE = "유효하지 않은 발급 문의 cursor입니다.";
    private final IssuanceInquiryCursorCodec codec = new IssuanceInquiryCursorCodec();

    @Test
    void roundTripsDeterministicUrlSafeCursorForBothSourceKinds() {
        InquiryPosition attempt = new InquiryPosition(
                Instant.ofEpochSecond(0L, 123_456_789L), SourceKind.ATTEMPT, 42L);
        InquiryPosition issuance = new InquiryPosition(
                Instant.ofEpochSecond(0L, 123_456_789L), SourceKind.ISSUANCE, 42L);

        assertThat(codec.encode(attempt)).isEqualTo("djF8MHwxMjM0NTY3ODl8QVRURU1QVHw0Mg");
        assertThat(codec.encode(issuance)).isEqualTo("djF8MHwxMjM0NTY3ODl8SVNTVUFOQ0V8NDI");
        assertThat(codec.decode(codec.encode(attempt))).isEqualTo(attempt);
        assertThat(codec.decode(codec.encode(issuance))).isEqualTo(issuance);
        assertThat(codec.encode(attempt)).doesNotContain("=");
        assertThatThrownBy(() -> codec.encode(null))
                .isInstanceOf(NullPointerException.class);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidCursors")
    void rejectsInvalidAndNonCanonicalCursorsAsCommonInvalidInput(
            String scenario,
            String cursor
    ) {
        assertThatThrownBy(() -> codec.decode(cursor))
                .isInstanceOfSatisfying(BusinessException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(CommonErrorCode.INVALID_INPUT);
                    assertThat(exception).hasMessage(INVALID_CURSOR_MESSAGE);
                });
    }

    private static Stream<Arguments> invalidCursors() {
        String canonical = encoded("v1|0|123456789|ATTEMPT|42");
        String nonCanonical = canonical.substring(0, canonical.length() - 1) + "h";
        return Stream.of(
                Arguments.of("null", null),
                Arguments.of("blank", " "),
                Arguments.of("too long", "a".repeat(257)),
                Arguments.of("invalid Base64 URL", "%%%"),
                Arguments.of("padded", canonical + "="),
                Arguments.of("non-canonical unused bits", nonCanonical),
                Arguments.of("wrong field count", encoded("v1|0|0|ATTEMPT")),
                Arguments.of("wrong version", encoded("v2|0|0|ATTEMPT|1")),
                Arguments.of("non-numeric epoch", encoded("v1|now|0|ATTEMPT|1")),
                Arguments.of("epoch outside Instant", encoded(
                        "v1|31556889864403200|0|ATTEMPT|1")),
                Arguments.of("negative nano", encoded("v1|0|-1|ATTEMPT|1")),
                Arguments.of("nano too large", encoded("v1|0|1000000000|ATTEMPT|1")),
                Arguments.of("unknown source kind", encoded("v1|0|0|OTHER|1")),
                Arguments.of("empty source kind", encoded("v1|0|0||1")),
                Arguments.of("non-numeric id", encoded("v1|0|0|ATTEMPT|id")),
                Arguments.of("zero id", encoded("v1|0|0|ATTEMPT|0")),
                Arguments.of("negative id", encoded("v1|0|0|ATTEMPT|-1")));
    }

    private static String encoded(String payload) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
    }
}
