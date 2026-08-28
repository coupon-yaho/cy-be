package com.kafkick.core.coupon.v2.port;

/**
 * 워밍업이 {@code issued} Hash 에 다시 적어 넣는 회원 한 명. <b>DB 에 이미 발급 행이 있는 건</b>만
 * 여기 들어온다.
 *
 * <p>상태·요청토큰·멱등키를 받지 않는 이유는 셋 다 재구성 시점에 <b>복원할 수 없는 값</b>이기
 * 때문이다. 상태는 언제나 {@code D} 다 — DB 에 행이 있다는 것이 곧 영속이 끝났다는 뜻이고,
 * {@code P} 로 되살리면 PENDING 계측(05)이 없는 미영속 건을 상시 보고한다. 토큰과 멱등키는
 * 어댑터가 재구성 표식으로 채운다. 호출부가 고를 여지를 주면 그 값이 회차마다 달라진다.
 *
 * <p>{@code claimedAtEpochMillis} 는 {@code issuances.issued_at} 이다. 재구성 시각을 넣으면
 * PENDING 계측의 체류 시간이 회차 전체에서 0 으로 리셋된다.
 *
 * @throws IllegalArgumentException {@code memberId} 가 0 이하이거나 시각이 음수일 때
 */
public record RebuiltIssued(long memberId, long claimedAtEpochMillis) {

    public RebuiltIssued {
        if (memberId <= 0) {
            throw new IllegalArgumentException("memberId는 0보다 커야 합니다.");
        }
        if (claimedAtEpochMillis < 0) {
            throw new IllegalArgumentException("claimedAtEpochMillis는 음수일 수 없습니다.");
        }
    }
}
