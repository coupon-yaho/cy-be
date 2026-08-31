package com.kafkick.core.coupon.v2;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IssuedValueCodecTest {

    private final IssuedValueCodec codec = new IssuedValueCodec();

    @Test
    void encodeAndDecodeRoundTrip() {
        IssuedValue value = new IssuedValue(
                IssuedValue.Status.PENDING,
                1_725_000_000_123L,
                "api-1-42-7",
                "order|member|retry"
        );

        String encoded = codec.encode(value);

        assertThat(encoded).isEqualTo("P|1725000000123|api-1-42-7|order|member|retry");
        assertThat(codec.decode(encoded)).isEqualTo(value);
    }

    @Test
    void comparesOverlappingIdempotencyKeysByWholeValue() {
        IssuedValue stored = codec.decode("P|1|api-1-42-7|abcdef");

        assertThat(stored.hasIdempotencyKey("abc")).isFalse();
        assertThat(stored.hasIdempotencyKey("abcdef")).isTrue();
    }

    @Test
    void classifiesMalformedValueAsCorrupt() {
        assertThatThrownBy(() -> codec.decode("P|not-a-number|api-1-42-7|key"))
                .isInstanceOf(IssuedValueCorruptException.class);
    }

    @Test
    void rejectsUnknownStatusAsCorrupt() {
        assertThatThrownBy(() -> codec.decode("X|1|api-1-42-7|key"))
                .isExactlyInstanceOf(IssuedValueCorruptException.class);
    }

    @Test
    void rejectsNegativeClaimedAtWhenCreatingValue() {
        assertThatThrownBy(() -> new IssuedValue(
                IssuedValue.Status.PENDING,
                -1L,
                "api-1-42-7",
                "key"
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsEmptyIdempotencyKeyWhenCreatingValue() {
        assertThatThrownBy(() -> new IssuedValue(
                IssuedValue.Status.PENDING,
                1L,
                "api-1-42-7",
                ""
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsClaimedAtWiderThanThirteenDigitsWhenCreatingValue() {
        assertThatThrownBy(() -> new IssuedValue(
                IssuedValue.Status.PENDING,
                12_345_678_901_234L,
                "api-1-42-7",
                "key"
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void roundTripsThirteenDigitClaimedAt() {
        IssuedValue value = new IssuedValue(
                IssuedValue.Status.PENDING,
                9_999_999_999_999L,
                "api-1-42-7",
                "key"
        );

        assertThat(codec.decode(codec.encode(value))).isEqualTo(value);
    }

    @Test
    void corruptExceptionIsNotAnInvalidClientArgument() {
        assertThat(IllegalArgumentException.class.isAssignableFrom(
                IssuedValueCorruptException.class
        )).isFalse();
    }

    @Test
    void stringRepresentationDoesNotExposeIdempotencyKey() {
        IssuedValue value = new IssuedValue(
                IssuedValue.Status.DONE,
                1L,
                "api-1-42-7",
                "member-123-order-456"
        );

        assertThat(value.toString())
                .doesNotContain("member-123-order-456");
    }

    @Test
    void classifiesEmptyIdempotencyKeyAsCorrupt() {
        assertThatThrownBy(() -> codec.decode("P|1|api-1-42-7|"))
                .isExactlyInstanceOf(IssuedValueCorruptException.class);
    }

    @Test
    void decodesWhitespaceOnlyIdempotencyKey() {
        IssuedValue decoded = codec.decode("P|1|api-1-42-7|   ");

        assertThat(decoded.hasIdempotencyKey("   ")).isTrue();
    }

    @Test
    void decodesIdempotencyKeyContainingNewline() {
        IssuedValue decoded = codec.decode("P|1|api-1-42-7|order-1\ntrace-2");

        assertThat(decoded.hasIdempotencyKey("order-1\ntrace-2")).isTrue();
    }

    @Test
    void roundTripsIdempotencyKeyContainingNewline() {
        IssuedValue value = new IssuedValue(
                IssuedValue.Status.PENDING,
                1L,
                "api-1-42-7",
                "order-1\ntrace-2"
        );

        assertThat(codec.decode(codec.encode(value))).isEqualTo(value);
    }

    @Test
    void rejectsEmptyRequestTokenWhenCreatingValue() {
        assertThatThrownBy(() -> new IssuedValue(
                IssuedValue.Status.PENDING,
                1L,
                "",
                "key"
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void decodesWhitespaceOnlyRequestToken() {
        IssuedValue decoded = codec.decode("P|1|   |key");

        assertThat(decoded.requestToken()).isEqualTo("   ");
    }

    @Test
    void rejectsDelimiterInRequestTokenWhenCreatingValue() {
        assertThatThrownBy(() -> new IssuedValue(
                IssuedValue.Status.PENDING,
                1L,
                "api|1-42-7",
                "key"
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void classifiesClaimedAtWiderThanThirteenDigitsAsCorrupt() {
        assertThatThrownBy(() -> codec.decode("P|12345678901234|api-1-42-7|key"))
                .isExactlyInstanceOf(IssuedValueCorruptException.class);
    }

    @Test
    void acceptsThirteenDigitClaimedAt() {
        IssuedValue decoded = codec.decode("P|9999999999999|api-1-42-7|key");

        assertThat(decoded.claimedAtEpochMillis()).isEqualTo(9_999_999_999_999L);
    }
}
