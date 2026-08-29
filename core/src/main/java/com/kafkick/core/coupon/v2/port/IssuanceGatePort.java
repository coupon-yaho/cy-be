package com.kafkick.core.coupon.v2.port;

import java.util.Optional;

/**
 * v2 발급 게이트. Redis 의 원자 실행 다섯 종을 도메인 결과 타입으로 가둔다.
 *
 * <p><b>키 이름과 해시태그는 어댑터가 소유한다.</b> 여기서 회차 식별자만 받는 이유가 그것이다 —
 * 키 문자열이 포트를 넘어오면 호출부마다 리터럴이 하나씩 생기고, 그중 하나가 틀렸다는 사실은
 * 정합성 리더가 아무것도 못 읽을 때에야 드러난다.
 *
 * <p><b>게이트의 판정</b>은 예외가 아니라 결과다. 거절과 사고는 둘 다 결과값으로 나가고,
 * 그것을 HTTP 로 가르는 것은 응답 매핑(S5)의 몫이다.
 *
 * <p><b>Redis 통신 실패는 그대로 전파된다.</b> 이건 판정이 아니라 통로가 끊긴 것이라
 * 결과값으로 옮기지 않았다. 특히 <b>명령 타임아웃은 "실행되지 않았다" 가 아니다</b> —
 * 기본 500ms 를 넘겨 클라이언트가 포기해도 서버는 스크립트를 끝까지 실행하므로, 선점은
 * 성립하고 재고 한 장이 빠진 채 호출부만 예외를 받는다. 그 {@code P} 는 만료 배치로도
 * 안 풀린다(만료의 원본은 DB 인데 DB 에는 그 발급이 없다).
 *
 * <p>그래서 <b>예외를 받은 호출부는 "실패했다" 로 끝내면 안 된다</b> — 같은 요청토큰으로
 * 보상을 부르는 것이 그 자리의 정답이고(토큰 CAS 라 남의 선점은 못 건드린다), 그 판단은
 * 트랜잭션 경계를 아는 S4·S6 의 몫이다. 결과값 하나를 더 만들어 여기서 삼키면 그 판단이
 * 이 층으로 잘못 내려온다.
 */
public interface IssuanceGatePort {

    /** 정책 검증 · 중복 판정 · 재고 차감을 한 번의 원자 실행으로 끝낸다. */
    ClaimResult claim(ClaimCommand command);

    /** 선점을 완료로 승격한다. <b>자기 선점일 때만</b> 올라간다. */
    CompleteOutcome complete(long couponRoundId, long memberId, String requestToken);

    /** 선점을 되돌린다 — 발급이 없었던 일이므로 세 키를 다 되돌린다. */
    CompensateOutcome compensate(long couponRoundId, long memberId, String requestToken);

    /** 만료분의 재고를 되살린다. 상한 검사가 같은 원자 실행 안에 있다. */
    RestoreOutcome restore(long couponRoundId, long count);

    /**
     * 파손 값을 회수한다.
     *
     * @param restoreStock DB 에 발급이 <b>없을 때만</b> {@code true}. 파손 값에는 원래 상태가
     *     남아 있지 않아 스크립트 단독으로는 못 정한다 — 되살리면 초과 발급 방향이다
     * @param totalQuantity 총재고. {@code meta} 는 재구성 1번 단계에서 이미 지워져 있어 인자다
     */
    ReclaimOutcome reclaimCorrupt(
            long couponRoundId, long memberId, boolean restoreStock, long totalQuantity);

    /**
     * 게이트를 <b>닫는다</b> — {@code meta} 를 지운다. 재구성의 1번 단계다(설계 §6.2).
     *
     * <p>이 호출 뒤 그 회차의 선점·복원은 전량 미준비({@code -9}·{@code -1})다. 그것이 재구성
     * 창의 안전 상태다 — 카운터를 통째로 다시 쓰는 동안 아무도 낡은 값 위에서 발급받지 않는다.
     * 도중에 죽어도 게이트가 닫힌 채 남으므로, 다시 돌리는 것이 곧 복구다.
     *
     * <p><b>{@code DEL} 이 아니라 {@code UNLINK} 다</b>(§3.3). 회수를 다른 스레드로 넘기지
     * 않으면 그 시간만큼 Redis 단일 스레드가 서고, 그동안 발급이 전면 정지한다. {@code meta} 는
     * 다섯 필드뿐이라 그 자체로는 짧지만, 같은 규칙을 키마다 다르게 적용하면 어느 키가 예외인지를
     * 사람이 기억해야 한다.
     *
     * <p><b>게이트만 닫는다.</b> 카운터 세 키는 남는다 — 지우는 것은 시딩의 몫이고
     * ({@link IssuanceWarmupPort#seedCounters}), 여기서 함께 지우면 집계를 읽기도 전에
     * {@code issued} 가 사라져 도중에 죽었을 때 남은 상태가 아무것도 말해 주지 않는다.
     */
    void closeGate(long couponRoundId);

    /** 다섯 필드를 <b>한 번에</b> 쓴다. 부분 상태를 남기지 않는다. */
    void writeMeta(long couponRoundId, GateMeta meta);

    /** @return 다섯 필드가 모두 있을 때만 값. 부분 상태는 비어 있다 */
    Optional<GateMeta> readMeta(long couponRoundId);
}
