package com.kafkick.batch.coupon.v2;

/**
 * 워밍업과 재구성이 <b>글자 그대로 같아야 하는</b> 검사. 각자 적어 두면 한쪽만 고쳐진 채 남고,
 * 그 사실은 어느 경로로 올린 회차냐에 따라 정합성 축이 달라질 때에야 드러난다.
 *
 * <p>{@code coupon_stocks} 갱신의 0행 검사는 여기 없다 — 그건 쓰는 자리인
 * {@link CouponRoundGateJdbc#updateActiveCount} 안에 있다. 검사를 쓰기에서 떼면 부르는 쪽마다
 * 한 벌씩 생기고, 이 클래스가 막으려던 것이 바로 그것이다.
 */
final class CouponRoundGateWrites {

    private CouponRoundGateWrites() {
    }

    /**
     * {@code uk_coupon_member} 가 회차당 회원 한 행을 강제하므로 누적 건수와 회원 수는 같아야
     * 한다(§9.1 I2). 다르면 그 제약이 깨진 것이고, 여기서 조용히 목록 쪽을 택하면 I4 위반을
     * 우리가 만들어 낸다.
     */
    static void requireMemberCountMatchesEverCount(
            long couponRoundId, CouponRoundGateJdbc.Aggregate aggregate) {
        if (aggregate.everCount() != aggregate.everMembers().size()) {
            throw new IllegalStateException(
                    "회차 " + couponRoundId + " 의 누적 건수(" + aggregate.everCount()
                            + ")와 회원 수(" + aggregate.everMembers().size() + ")가 다릅니다."
                            + " uk_coupon_member 가 깨졌을 때만 나오는 상태입니다.");
        }
    }
}
