package com.kafkick.infra.redis.coupon.v2;

import java.util.Optional;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;

import com.kafkick.core.coupon.v2.port.ClaimCommand;
import com.kafkick.core.coupon.v2.IssuanceGateCircuitOpenException;
import com.kafkick.core.coupon.v2.port.ClaimResult;
import com.kafkick.core.coupon.v2.port.CompensateOutcome;
import com.kafkick.core.coupon.v2.port.CompleteOutcome;
import com.kafkick.core.coupon.v2.port.GateMeta;
import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.core.coupon.v2.port.ReclaimOutcome;
import com.kafkick.core.coupon.v2.port.RestoreOutcome;

/**
 * Resilience4j 차단기를 발급의 Redis 왕복에 적용한다.
 *
 * <p>보상은 open 상태에서 Redis에 보내지 않고 {@link CompensateOutcome#NOT_ATTEMPTED_CIRCUIT_OPEN}
 * 으로 남긴다. 성공 보상으로 위장하면 실제 PENDING 선점을 지운 것처럼 보이는 정합성 사고가 된다.
 * 완료 CAS는 DB가 이미 커밋된 발급의 복구 경로라 차단하지 않는다 — 차단하면 발급은 됐는데
 * 게이트만 PENDING 인 상태가 늘고, 그것을 푸는 유일한 경로가 이 호출이다. 대신 open 구간에서
 * <b>명령 timeout 을 그대로 수용한다</b>(redis.yml 의 500ms). 실패한 완료 CAS 를 재조회 뒤
 * 한 번 더 태우는 복구 경로가 있어 <b>요청당 최대 2회</b>다 — 그 재시도를 없애면 DB 에
 * 커밋된 발급의 게이트가 영구히 PENDING 으로 남으므로 줄이지 않는다. DB 커밋 뒤라 되돌릴
 * 것이 없고 횟수가 상수라는 점에서 유계다. 이 어댑터는 Retry를 조합하지 않는다.
 *
 * <p><b>차단하는 것은 {@code claim} 하나다.</b> 나머지 — {@code restore}·
 * {@code reclaimCorrupt}·{@code closeGate}·{@code writeMeta}·{@code readMeta} — 는
 * 요청 경로가 아니라 배치·관리 경로라 위임만 한다. 워커를 즉시 푸는 것이 차단기의 목적인데
 * 거기에는 풀어 줄 요청 스레드가 없다. 대신 Redis 장애 구간에 재구성 배치가 돌면 회차마다
 * 명령 timeout 을 그대로 문다 — 빠뜨린 것이 아니라 그 비용을 받기로 한 것이다.
 */
final class Resilience4jIssuanceGate implements IssuanceGatePort {

    private final IssuanceGatePort delegate;
    private final CircuitBreaker circuitBreaker;

    Resilience4jIssuanceGate(IssuanceGatePort delegate, CircuitBreaker circuitBreaker) {
        this.delegate = delegate;
        this.circuitBreaker = circuitBreaker;
    }

    @Override
    public ClaimResult claim(ClaimCommand command) {
        try {
            return CircuitBreaker.decorateSupplier(circuitBreaker, () -> delegate.claim(command)).get();
        } catch (CallNotPermittedException open) {
            throw new IssuanceGateCircuitOpenException(open);
        }
    }

    @Override public CompleteOutcome complete(long roundId, long memberId, String token) { return delegate.complete(roundId, memberId, token); }
    /**
     * <b>보상은 차단기의 허가 체계에 참여하지 않는다.</b> {@code tryAcquirePermission()} 을 쓰면
     * half-open 의 시험 호출 자리 하나를 보상이 가져가는데, 이 왕복은 표본에 넣지 않으므로
     * 성패를 기록하지 않는다 — 허가만 먹고 결과를 안 주는 조합이라 차단기가 복구를 학습하지
     * 못하고, 그동안 들어온 선점은 전부 즉시 503 이 된다. OPEN 에서 거절될 때
     * {@code not_permitted_calls} 가 요청당 두 번 오르는 부작용도 따라온다.
     *
     * <p>대신 상태를 읽어 분기한다. 읽기와 호출 사이에 open 으로 전이하면 그 순간 진행 중이던
     * 요청 몇 건이 죽은 Redis 로 보상을 보내 명령 timeout 한 번을 더 쓴다 — 전이 시점에만
     * 생기고 요청당 1회로 끝나는 <b>유계 비용</b>이라, 위의 반복되는 손해와 바꾸지 않는다.
     */
    @Override
    public CompensateOutcome compensate(long roundId, long memberId, String token) {
        CircuitBreaker.State state = circuitBreaker.getState();
        if (state == CircuitBreaker.State.OPEN || state == CircuitBreaker.State.FORCED_OPEN) {
            return CompensateOutcome.NOT_ATTEMPTED_CIRCUIT_OPEN;
        }
        // 보상은 선점 요청의 후속 CAS다. 이 왕복까지 CB 표본에 넣으면 "5 요청 실패"가 아니라
        // "claim+보상 명령 5회 실패"가 되어 한 요청이 두 표본을 차지한다.
        return delegate.compensate(roundId, memberId, token);
    }
    @Override public RestoreOutcome restore(long roundId, long count) { return delegate.restore(roundId, count); }
    @Override public ReclaimOutcome reclaimCorrupt(long roundId, long memberId, boolean restoreStock, long total) { return delegate.reclaimCorrupt(roundId, memberId, restoreStock, total); }
    @Override public void closeGate(long roundId) { delegate.closeGate(roundId); }
    @Override public void writeMeta(long roundId, GateMeta meta) { delegate.writeMeta(roundId, meta); }
    @Override public Optional<GateMeta> readMeta(long roundId) { return delegate.readMeta(roundId); }
}
