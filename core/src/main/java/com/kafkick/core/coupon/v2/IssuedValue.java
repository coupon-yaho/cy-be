package com.kafkick.core.coupon.v2;

import java.util.Objects;

public record IssuedValue(
        Status status,
        long claimedAtEpochMillis,
        String requestToken,
        String idempotencyKey
) {

    static final long CLAIMED_AT_MAX = 9_999_999_999_999L;

    public IssuedValue {
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(requestToken, "requestToken");
        Objects.requireNonNull(idempotencyKey, "idempotencyKey");
        if (claimedAtEpochMillis < 0) {
            throw new IllegalArgumentException("claimedAtEpochMillis는 음수일 수 없습니다.");
        }
        if (claimedAtEpochMillis > CLAIMED_AT_MAX) {
            throw new IllegalArgumentException("claimedAtEpochMillis는 13자리를 넘을 수 없습니다.");
        }
        if (requestToken.isBlank()) {
            throw new IllegalArgumentException("requestToken은 비어 있을 수 없습니다.");
        }
        if (requestToken.indexOf('|') >= 0) {
            throw new IllegalArgumentException("requestToken에는 '|'를 포함할 수 없습니다.");
        }
        if (idempotencyKey.isEmpty()) {
            throw new IllegalArgumentException("idempotencyKey는 비어 있을 수 없습니다.");
        }
    }

    public boolean hasIdempotencyKey(String candidate) {
        return idempotencyKey.equals(candidate);
    }

    public boolean isDone() {
        return status == Status.DONE;
    }

    @Override
    public String toString() {
        return "IssuedValue[status=" + status
                + ", claimedAtEpochMillis=" + claimedAtEpochMillis
                + ", requestToken=<redacted>, idempotencyKey=<redacted>]";
    }

    public enum Status {
        PENDING("P"),
        DONE("D");

        private final String code;

        Status(String code) {
            this.code = code;
        }

        public String code() {
            return code;
        }

    }
}
