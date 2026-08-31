package com.kafkick.api.observation.issuance;

import java.util.Objects;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import org.springframework.stereotype.Component;

import com.kafkick.api.observation.MeterNames;

/**
 * v2 발급의 결과 카운터 아홉 종.
 *
 * <p><b>거절 3종 — 합치지 않는다.</b> {@code dupPerMember} 는 다른 시도가 이미 그 회원으로
 * 받은 거절이고 나머지 둘은 같은 멱등키의 재시도다. 한 미터에 태그로 얹으면 대시보드에서는
 * 갈리지만 합산 패널·경보 규칙이 한 번만 잘못 쓰여도 {@code dupPerMember} 급증이 재시도
 * 물결에 묻힌다 — 이상 신호를 놓치는 방향이다.
 *
 * <p><b>괴리 2종 — 게이트가 놓친 것만 센다.</b> {@code databaseStockDivergence}·
 * {@code databaseMemberDivergence} 는 Redis 가 통과시킨 요청을 DB 가 막은 건이다. 복제
 * 유실의 직접 신호라, 게이트가 스스로 거른 건과 같은 카운터에 넣으면 평범한 재요청
 * 물결에 묻힌다. 그래서 {@code dupPerMember} 에는 DB 가 잡은 중복을 넣지 않는다.
 *
 * <p><b>보상 3종 — 재고 결과가 다르면 미터도 다르다.</b> {@code claimLeaked} 는 이 요청의
 * {@code DECR} 이 복구되지 않은 채 끝난 경우, {@code compensationFoundNoClaim} 은 되돌릴
 * 선점이 없던 경우(다른 절차가 먼저 정리했다), {@code compensationOnCompletedClaim} 은 이미
 * 완료 승격된 선점에 보상이 도달한 경우다. 셋을 합치면 <b>{@code claimLeaked} 의 기준선이
 * 0 이 아니게 되어 임계 경보를 걸 수 없다</b> — 가운데 값은 부하 회차에서 상시 발생한다.
 * 합계가 필요하면 질의에서 더한다.
 *
 * <p>이 구분은 CY-781 이 보상 Lua 의 {@code 0} 을 "없다"({@code 2})와 "남의 토큰"({@code 0})
 * 으로 가른 뒤부터 <b>추측이 아니라 사실</b>이다. 판정은
 * {@code CouponIssueObservationCoordinator.leftClaimBehind} 한 곳에서만 한다.
 *
 * <p>⚠️ <b>{@code claimLeaked} 경보는 아직 없다.</b> {@code infra/prometheus/rules} 에는
 * batch 규칙만 있고 api 규칙 파일 자체가 없다 — 누수는 과소 발급 방향이라 응답에 안
 * 드러나므로, 경보가 붙기 전까지는 사람이 대시보드를 봐야 안다. 붙일 때 쓸 식은
 * {@code increase(app_issuance_v2_claim_leaked_total[5m]) > 0} 이고, 기준선이 0 이라
 * 임계를 따로 고를 필요가 없다. 그것이 이 미터를 셋으로 가른 이유다.
 *
 * <p><b>503 1종.</b> {@code redisUnavailable} 은 failover·차단기 개방 구간에만 오른다.
 */
@Component
public final class V2IssuanceOutcomeMeters {

    private final Counter dupPerMember;
    private final Counter replayDone;
    private final Counter replayPending;
    private final Counter databaseStockDivergence;
    private final Counter databaseMemberDivergence;
    private final Counter claimLeaked;
    private final Counter compensationFoundNoClaim;
    private final Counter compensationOnCompletedClaim;
    private final Counter redisUnavailable;

    public V2IssuanceOutcomeMeters(MeterRegistry meterRegistry) {
        Objects.requireNonNull(meterRegistry, "meterRegistry");
        dupPerMember = Counter.builder(MeterNames.ISSUANCE_V2_DUP_PER_MEMBER)
                .description("다른 시도가 이미 그 회원으로 발급받아 거절된 v2 요청")
                .register(meterRegistry);
        replayDone = Counter.builder(MeterNames.ISSUANCE_V2_REPLAY_DONE)
                .description("같은 멱등키의 재시도에 최초 응답을 재사용한 v2 요청")
                .register(meterRegistry);
        replayPending = Counter.builder(MeterNames.ISSUANCE_V2_REPLAY_PENDING)
                .description("같은 멱등키가 아직 처리 중이라 409로 떨어진 v2 요청")
                .register(meterRegistry);
        databaseStockDivergence = Counter.builder(MeterNames.ISSUANCE_V2_DATABASE_STOCK_DIVERGENCE)
                .description("Redis 선점 뒤 DB 매진으로 보상된 요청")
                .register(meterRegistry);
        databaseMemberDivergence = Counter.builder(MeterNames.ISSUANCE_V2_DATABASE_MEMBER_DIVERGENCE)
                .description("Redis 선점을 통과했으나 DB 1인1매 제약이 막은 요청")
                .register(meterRegistry);
        claimLeaked = Counter.builder(MeterNames.ISSUANCE_V2_CLAIM_LEAKED)
                .description("DB 거절 뒤 Redis 선점이 되돌아오지 않아 재고가 낮아진 요청")
                .register(meterRegistry);
        compensationFoundNoClaim = Counter.builder(MeterNames.ISSUANCE_V2_COMPENSATION_NO_CLAIM)
                .description("보상이 되돌릴 선점을 찾지 못한 요청 — 다른 절차가 이미 정리했다. 누수가 아니다")
                .register(meterRegistry);
        compensationOnCompletedClaim = Counter.builder(
                        MeterNames.ISSUANCE_V2_COMPENSATION_ALREADY_DONE)
                .description("이미 완료 승격된 선점에 보상이 도달한 요청 — 누수는 아니나 경보 대상")
                .register(meterRegistry);
        redisUnavailable = Counter.builder(MeterNames.ISSUANCE_V2_REDIS_UNAVAILABLE)
                .description("Redis failover·차단기·보상 불일치로 503을 반환한 v2 요청")
                .register(meterRegistry);
    }

    public void recordDupPerMember() {
        dupPerMember.increment();
    }

    public void recordReplayDone() {
        replayDone.increment();
    }

    public void recordReplayPending() {
        replayPending.increment();
    }

    public void recordDatabaseStockDivergence() {
        databaseStockDivergence.increment();
    }

    public void recordDatabaseMemberDivergence() {
        databaseMemberDivergence.increment();
    }

    public void recordClaimLeaked() {
        claimLeaked.increment();
    }

    public void recordCompensationFoundNoClaim() {
        compensationFoundNoClaim.increment();
    }

    public void recordCompensationOnCompletedClaim() {
        compensationOnCompletedClaim.increment();
    }

    public void recordRedisUnavailable() {
        redisUnavailable.increment();
    }
}
