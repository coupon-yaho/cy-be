package com.kafkick.core.observation;

import java.time.Instant;

/**
 * 쿠폰 회차가 끝났다는 사실을 관측 구현에 알리는 포트입니다.
 *
 * <p>{@link EventRecorder}와 섞지 않습니다. 이벤트 기록과 미터 수명 관리는 성격이 달라 한
 * 인터페이스에 두면 구현체 교체 단위가 엉킵니다.
 *
 * <p>이 통지는 {@code coupon_id} 라벨 예외의 대가입니다. 라벨을 열면 시계열이 쿠폰 회차 수만큼
 * 늘어나므로 언제 지울지는 도메인만 알 수 있습니다.
 */
public interface CouponRoundLifecycleRecorder {

    /**
     * 쿠폰 회차 종료를 통지합니다.
     *
     * <p>구현은 멱등이어야 합니다 — 같은 {@code couponId}로 여러 번 불려도 안전해야 종료 배치의
     * 재시도를 막지 않습니다. 이건 구현에 거는 요구사항이고 이 저장소가 아직 검증하지 않습니다.
     * 상태가 없는 기본 무동작 구현은 자동으로 만족하므로 검증할 대상이 없습니다.
     * TODO(OBS-26): 실제 구현이 들어올 때 반복 호출 테스트를 그 티켓에서 함께 낸다.
     *
     * <p>{@code closedAt}은 {@code null}일 수 없습니다. 미터 제거 시점이 이 값 하나에서만 나오므로
     * 구현이 수신 시각으로 대신 채우면 실제 종료보다 늦은 시각이 기준이 됩니다. 종료 시각을 모르는
     * 호출부는 스스로 시각을 확정해 넘깁니다.
     *
     * <p><b>구현은 값 계약 위반을 호출자에게 전파하지 않습니다.</b> 이 통지는 쿠폰 회차 종료
     * 트랜잭션 안에서 호출될 수 있어, 여기서 던지면 관측이 업무를 되돌립니다. 위반은 버리되
     * 조용히 버리지는 않습니다 — 기본 구현은 WARN 으로 남깁니다. 아래 값 제약은 그래도 계약이며,
     * 어기면 미터가 언제 사라지는지 설명할 수 없게 됩니다.
     *
     * @param couponId 종료된 쿠폰 회차의 쿠폰 식별자. 양수여야 합니다.
     *                 <b>{@code coupons.id}(쿠폰 회차)이며 {@code issuances.id}가 아닙니다</b> —
     *                 저장소 전역 규약({@code docs/02-erd-decisions.md})의
     *                 {@code coupon_id → issuances.id}와 반대 방향이라 관측 모듈에서만 예외입니다.
     *                 {@code issue_attempts.coupon_id}가 같은 의미를 씁니다
     * @param closedAt 쿠폰 회차가 실제로 종료된 시각. {@code null}일 수 없습니다
     */
    void retireCouponRound(long couponId, Instant closedAt);
}
