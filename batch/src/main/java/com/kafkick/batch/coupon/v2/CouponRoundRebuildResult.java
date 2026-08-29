package com.kafkick.batch.coupon.v2;

/**
 * 재구성 결과. 집계값을 함께 돌려주는 이유는 <b>운영자가 그 자리에서 대조</b>할 수 있게 하기
 * 위해서다 — 나중에 지표로만 확인하면 게이트는 이미 열려 있다.
 *
 * <p>수치를 <b>쌍</b>으로 낸다. {@code activeCount}·{@code recountedActiveCount} 의 차는 재구성
 * 창 동안 커밋된 취소이고(4′), {@code issuedEverCount}·{@code recountedIssuedEverCount} 의 차는
 * <b>시딩 뒤에 커밋된 발급</b>이다. 하나로 합치면 그 창에서 무슨 일이 있었는지 밖에서 알 방법이
 * 없다.
 *
 * <p>{@link CouponRoundRebuildStatus#REBUILT} 가 아니면 모르는 수치는 {@code -1} 이다.
 * 0 으로 채우면 "재고 0 인 회차를 다시 세웠다" 와 구분되지 않는다.
 *
 * @param gateClosed 이 결과를 낼 때 <b>게이트가 닫힌 채로 남았는가.</b> {@code true} 면 그 회차의
 *     발급은 지금 전량 503 이고, 다시 돌리기 전까지 그대로다. 거절을 전부 같은 얼굴로 내보내면
 *     운영자가 "아무것도 안 건드렸구나" 로 읽는데, 그 오해의 대가가 회차 하나의 영구 정지다
 */
public record CouponRoundRebuildResult(
        long couponRoundId,
        CouponRoundRebuildStatus status,
        boolean gateClosed,
        long totalQuantity,
        long activeCount,
        long recountedActiveCount,
        long issuedEverCount,
        long recountedIssuedEverCount,
        long remainingStock
) {

    /** 값을 모른다. 0 으로 채우면 "재고 0 인 회차" 와 구분되지 않는다. */
    public static final long UNKNOWN = -1L;

    /**
     * 게이트를 <b>건드리기 전</b>에 끝난 거절. 그 회차의 발급은 한 건도 멈추지 않았다.
     */
    public static CouponRoundRebuildResult rejected(
            long couponRoundId, CouponRoundRebuildStatus status) {
        return of(couponRoundId, status, false, UNKNOWN, UNKNOWN, UNKNOWN);
    }

    /**
     * 게이트를 <b>닫은 뒤</b>에 끝난 거절. 그 회차는 지금 전면 503 이고, 복구는 DB 를 고친 뒤
     * 다시 돌리는 것뿐이다.
     */
    public static CouponRoundRebuildResult rejectedAfterClose(
            long couponRoundId, CouponRoundRebuildStatus status) {
        return of(couponRoundId, status, true, UNKNOWN, UNKNOWN, UNKNOWN);
    }

    /**
     * 초과 발급 거절. <b>수치를 지우지 않는다</b> — 복구하려면 초과분이 몇 장인지부터 알아야
     * 한다. 아는 값을 {@code -1} 로 덮어 로그를 뒤지게 하지 않는다.
     *
     * @param gateClosed 사전 점검에서 걸렸으면 {@code false}, 게이트를 닫은 뒤 드러났으면
     *     {@code true}
     * @param recountedActiveCount 4′ 재집계에서 드러났으면 그 값, 그 전에 걸렸으면 {@code -1}
     */
    public static CouponRoundRebuildResult overIssued(
            long couponRoundId, boolean gateClosed, long totalQuantity,
            long activeCount, long recountedActiveCount) {
        return of(couponRoundId, CouponRoundRebuildStatus.OVER_ISSUED_ROUND, gateClosed,
                totalQuantity, activeCount, recountedActiveCount);
    }

    private static CouponRoundRebuildResult of(
            long couponRoundId, CouponRoundRebuildStatus status, boolean gateClosed,
            long totalQuantity, long activeCount, long recountedActiveCount) {
        return new CouponRoundRebuildResult(couponRoundId, status, gateClosed,
                totalQuantity, activeCount, recountedActiveCount, UNKNOWN, UNKNOWN, UNKNOWN);
    }

    public boolean rebuilt() {
        return status == CouponRoundRebuildStatus.REBUILT;
    }

    /**
     * 시딩에 들어간 누적 수와 게이트를 열 때의 DB 누적 수가 다른가 — 그 차가 곧 <b>{@code issued}
     * 에서 빠진 회원 수</b>이고, 그 회원들은 Redis 층의 1인 1매 방어가 없다.
     */
    public boolean issuedHashIsShort() {
        return rebuilt() && recountedIssuedEverCount > issuedEverCount;
    }
}
