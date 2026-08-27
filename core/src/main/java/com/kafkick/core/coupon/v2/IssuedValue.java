package com.kafkick.core.coupon.v2;

import java.util.Objects;

/**
 * {@code claimedAtEpochMillis} 는 <b>상태와 무관하게 선점 시각</b>이다.
 * 완료 승격은 상태 문자만 바꾸고 이 값을 덮지 않는다 — 완료 시각의 원본은
 * {@code issuances} 이고, "언제 선점됐나" 는 Redis 에만 있다.
 */
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
        // Lua 의 토큰 캡처는 ([^|]+) 라 "   " 를 정상으로 읽는다. 여기서 isBlank() 로 막으면
        // 같은 문자열을 Java 만 파손으로 세어 LUA_GAP 이 0 으로 수렴하지 못한다 — 멱등키에서
        // isBlank() 를 isEmpty() 로 낮춘 것과 같은 이유다(01 문서). codec 이 지키는 토큰 계약은
        // "비어 있지 않다" 와 "'|' 가 없다" 둘뿐이고, 공백뿐인 토큰을 거부할 자리는 생성기다.
        if (requestToken.isEmpty()) {
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
