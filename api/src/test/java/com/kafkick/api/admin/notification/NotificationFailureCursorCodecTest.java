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
                encoded("v1|9223372036854775808"), "a".repeat(257));
    }

    private static String encoded(String payload) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
    }
}
