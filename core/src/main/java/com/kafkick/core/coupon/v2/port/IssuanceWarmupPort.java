package com.kafkick.core.coupon.v2.port;

import java.util.List;

/**
 * 회차 하나의 카운터 세 키를 처음 올리는 통로. <b>{@link IssuanceGatePort} 와 나눠 둔 이유는
 * 수명이 다르기 때문</b>이다 — 게이트는 발급 경로가 매 요청 부르고, 이쪽은 회차당 한 번
 * 게이트가 닫힌 창에서만 돈다. 한 인터페이스에 두면 발급 유스케이스가 재고를 통째로 쓰는
 * 연산을 손에 쥐게 된다.
 *
 * <p><b>{@code meta} 를 쓰지 않는다.</b> 게이트를 여는 것은 {@link IssuanceGatePort#writeMeta}
 * 이고, 그 호출은 이 연산이 끝난 뒤여야 한다(설계 §6.2 의 3·4 → 5). 여기서 함께 쓰면 순서를
 * 호출부가 못 지키는 것이 아니라 <b>지킬 수 없게</b> 된다.
 */
public interface IssuanceWarmupPort {

    /**
     * {@code issued} Hash · {@code issued_ever} · {@code stock} 을 이 순서로 쓴다.
     *
     * <p><b>세 키가 한 시그니처인 것이 계약이다.</b> {@code issued_ever} 를 따로 부르게 두면
     * 빠뜨릴 수 있고, 빠뜨린 순간 {@code LUA_GAP ≠ 0} 이라 재구성 자체가 정합성 사고가 된다
     * (설계 §9.1 I4). 같은 이유로 {@code issued_ever} 값을 인자로 받지 않는다 —
     * {@code everMembers.size()} 가 곧 그 값이라, 둘이 갈라질 자리를 만들지 않는다.
     *
     * <p>{@code issued} 는 쓰기 전에 {@code UNLINK} 된다. 앞선 워밍업이 중간에 죽어 남긴 field 가
     * 섞이면 회원 수가 조용히 늘어난다. {@code DEL} 이 아닌 이유는 §3.3 이다(175k field 기준
     * 11.6ms → 1ms 미만).
     *
     * @param everMembers 누적 집합. 취소·만료 회원도 <b>포함</b>한다 — 1인 1매가 평생 기준이라
     *     재발급이 막혀야 한다. 활성 집계와 조건이 다르다
     * @param remainingStock {@code total_quantity − 활성 건수}
     * @throws IllegalArgumentException {@code everMembers} 에 같은 회원이 두 번 들어 있을 때.
     *     Hash 가 조용히 접어 {@code HLEN < issued_ever} 가 되고 그것이 곧 {@code LUA_GAP} 이다
     */
    void seedCounters(long couponRoundId, List<RebuiltIssued> everMembers, long remainingStock);
}
