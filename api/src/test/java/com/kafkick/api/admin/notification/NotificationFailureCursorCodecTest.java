package com.kafkick.api.admin.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import com.kafkick.core.support.exception.BusinessException;

class NotificationFailureCursorCodecTest {
    private final NotificationFailureCursorCodec codec = new NotificationFailureCursorCodec();

    @Test
    void encodesVersionedPayloadDeterministicallyAndRoundTrips() {
        String cursor = codec.encode(12_345L);

        assertThat(cursor).isEqualTo("djF8MTIzNDU");
        assertThat(codec.decode(cursor)).isEqualTo(12_345L);
    }

    @ParameterizedTest
    @MethodSource("invalidCursors")
    void rejectsEveryInvalidFormAsOneInputError(String cursor) {
        assertThatThrownBy(() -> codec.decode(cursor))
                .isInstanceOf(BusinessException.class)
                .hasMessageContaining("유효하지 않은 알림 cursor입니다.");
    }

    static Stream<String> invalidCursors() {
        return Stream.of(null, "", " ", "djF8MQ==", "*", "djF8MR",
                encoded("v2|1"), encoded("v1"), encoded("v1|0"), encoded("v1|-1"),
                encoded("v1|9223372036854775808"), "a".repeat(257),
                nonCanonical(encoded("v1|42")));
    }

    /**
     * <b>같은 바이트열로 디코딩되는 다른 문자열.</b>
     *
     * <p>Base64 마지막 글자는 남는 비트를 싣고 디코더가 그것을 버린다 — 그래서 그
     * 자리를 바꿔도 결과가 같다. 코덱이 정규 인코딩만 받는다는 성질을 <b>이 케이스가
     * 유일하게</b> 태운다. 없으면 그 검사를 통째로 지워도 시험이 초록이다(실측).
     */
    private static String nonCanonical(String canonical) {
        String head = canonical.substring(0, canonical.length() - 1);
        byte[] want = Base64.getUrlDecoder().decode(canonical);
        for (char c : "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
                .toCharArray()) {
            String candidate = head + c;
            if (!candidate.equals(canonical)
                    && java.util.Arrays.equals(
                            Base64.getUrlDecoder().decode(candidate), want)) {
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
