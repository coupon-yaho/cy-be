package com.kafkick.core.coupon.v2;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class IssuedValueCodec {

    private static final String DELIMITER = "|";
    private static final int CLAIMED_AT_MAX_DIGITS = Long.toString(IssuedValue.CLAIMED_AT_MAX).length();
    private static final Pattern VALUE_PATTERN = Pattern.compile(
            "\\A([" + statusCodes() + "])"
                    + Pattern.quote(DELIMITER) + "([0-9]{1," + CLAIMED_AT_MAX_DIGITS + "})"
                    + Pattern.quote(DELIMITER) + "([^" + DELIMITER + "]+)"
                    + Pattern.quote(DELIMITER) + "(.*)\\z",
            Pattern.DOTALL
    );

    public String encode(IssuedValue value) {
        Objects.requireNonNull(value, "value");

        return String.join(
                DELIMITER,
                value.status().code(),
                Long.toString(value.claimedAtEpochMillis()),
                value.requestToken(),
                value.idempotencyKey()
        );
    }

    public IssuedValue decode(String encoded) {
        if (encoded == null) {
            throw new IssuedValueCorruptException("issued 값은 null일 수 없습니다.");
        }

        Matcher matcher = VALUE_PATTERN.matcher(encoded);
        if (!matcher.matches()) {
            throw new IssuedValueCorruptException("issued 값이 4필드 고정 형식과 일치하지 않습니다.");
        }

        try {
            return new IssuedValue(
                    parseStatus(matcher.group(1)),
                    Long.parseLong(matcher.group(2)),
                    matcher.group(3),
                    matcher.group(4)
            );
        } catch (IllegalArgumentException exception) {
            throw new IssuedValueCorruptException("issued 값의 필드를 해석할 수 없습니다.", exception);
        }
    }

    private static String statusCodes() {
        StringBuilder codes = new StringBuilder();
        for (IssuedValue.Status status : IssuedValue.Status.values()) {
            codes.append(status.code());
        }
        return codes.toString();
    }

    private static IssuedValue.Status parseStatus(String code) {
        for (IssuedValue.Status status : IssuedValue.Status.values()) {
            if (status.code().equals(code)) {
                return status;
            }
        }
        throw new IssuedValueCorruptException("issued 값의 상태 문자를 알 수 없습니다: " + code);
    }
}
