package com.kafkick.core.consistency;

/**
 * 정합성 gap을 계산하기 전에 수집한 Redis·DB 원천값입니다.
 *
 * <p>{@code dbActiveCount}와 {@code storedActiveCount}는 {@code ISSUED + USED}를 셉니다.
 * {@code dbIssuedEverCount}, {@code redisIssuedEverCount}, {@code redisMemberEverCount}는
 * {@code ISSUED + USED + CANCELLED + EXPIRED} 누적 집합을 셉니다.
 * {@code redisRemaining}과 {@code storedActiveCount}는 이상 상태의 원본을 보존하기 위해 음수도 허용합니다.
 *
 * @param totalQuantity 쿠폰 회차에 설정된 총 발급 수량
 * @param redisRemaining Redis에 저장된 현재 잔여 수량
 * @param redisIssuedEverCount Redis 발급 완료 누적 카운터
 * @param redisMemberEverCount Redis 발급 회원 누적 집합의 원소 수
 * @param dbActiveCount DB 쿠폰 중 현재 활성 상태인 {@code ISSUED + USED} 행 수
 * @param dbIssuedEverCount DB 쿠폰 중 발급 이력이 있는 상태의 누적 행 수
 * @param storedActiveCount {@code coupon_stocks.active_count}에 저장된 활성 카운터
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

    /** 음수가 될 수 없는 수량과 누적 카운터를 검증합니다. */
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
