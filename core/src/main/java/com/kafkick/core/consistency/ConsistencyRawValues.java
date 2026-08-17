package com.kafkick.core.consistency;

/**
 * dbActiveCount와 storedActiveCount는 ISSUED + USED를 센다.
 * dbIssuedEverCount, redisIssuedEverCount, redisMemberEverCount는
 * ISSUED + USED + CANCELLED + EXPIRED 누적 집합을 센다.
 * redisRemaining과 storedActiveCount는 드리프트 원본을 보존하기 위해 음수도 허용한다.
 */
public record ConsistencyRawValues(
        long totalQuantity,
        long redisRemaining,
        long redisIssuedEverCount,
        long redisMemberEverCount,
        long dbActiveCount,
        long dbIssuedEverCount,
        long storedActiveCount
) {

    public ConsistencyRawValues {
        requireNonNegative(totalQuantity, "totalQuantity");
        requireNonNegative(redisIssuedEverCount, "redisIssuedEverCount");
        requireNonNegative(redisMemberEverCount, "redisMemberEverCount");
        requireNonNegative(dbActiveCount, "dbActiveCount");
        requireNonNegative(dbIssuedEverCount, "dbIssuedEverCount");
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + "은 0 이상이어야 합니다.");
        }
    }
}
