package com.kafkick.batch.coupon.v2;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafkick.core.coupon.v2.port.GateMeta;
import com.kafkick.core.coupon.v2.port.GateStatus;
import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.core.coupon.v2.port.IssuanceWarmupPort;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.support.TimeProvider;

/**
 * 회차 하나를 Redis 에 올린다 — 설계 §6.2 의 <b>처음 여는 경우</b>만이다.
 *
 * <h2>순서가 전부다</h2>
 *
 * <pre>
 * 1. meta 가 이미 있으면 거절     ← 열린 게이트 뒤에서 카운터를 갈아엎지 않는다
 * 2. DB 집계를 한 트랜잭션으로 읽는다
 * 3. issued Hash · issued_ever · stock   (어댑터가 이 순서로 한 번에)
 * 4. issuance_engine_locked = TRUE  ← 엔진 확정. 게이트를 여는 것과 같은 사건이다
 * 5. coupon_stocks.active_count
 * 6. meta                          ← 게이트를 여는 행위. 반드시 마지막
 * </pre>
 *
 * <p><b>도중에 죽으면 {@code meta} 가 없는 상태로 남는다.</b> 그 회차의 발급은 전부
 * {@code -9} → 503 이고, 그것이 안전한 상태다. 반대로 {@code meta} 를 먼저 쓰면 그 창의 모든
 * 요청이 오경보이거나 — 더 나쁘게 — 낡은 카운터 위에서 성립한다.
 *
 * <h2>batch 가 소유하는 이유</h2>
 *
 * <p>batch 는 1대다. 재구성을 batch 만 수행하면 <b>프로세스 간</b> 겹침이 사라진다(07 의 (a)).
 * api 는 여러 대라 같은 코드를 거기 두면 한쪽이 게이트를 연 뒤 다른 쪽이 {@code stock} 을
 * 덮어쓴다 — 초과 발급 방향이다.
 *
 * <p><b>그것으로 끝이 아니다.</b> 07 의 겹침 시퀀스는 프로세스 수가 아니라 <b>실행의 겹침</b>으로
 * 성립하는데, 트리거가 HTTP 라 이 프로세스 안에도 워커 스레드가 여럿이다. 그래서
 * <b>프로세스 내 차단은 {@link RoundGateWriteGuard} 가 진다</b> — 배포 토폴로지가 대신해 주지
 * 않는 몫이고, 재구성과 <b>같은 가드를 공유한다</b>. 프로세스가 여럿이 되는 날에는 이걸로도
 * 부족하고, 그때는 Redis 락(07 의 (b))이 S8b 가 아니라 선행 조건이다.
 *
 * <h2>여기 없는 것</h2>
 *
 * <p>게이트를 닫고 다시 만드는 경로는 {@link CouponRoundRebuildRunner} 다.
 * <b>이 클래스의 거절은 그대로 옳다</b> — 처음 여는 경로에서 "이미 열려 있음" 은 사고이고,
 * 재구성에서만 정상 입력이다. 이미 열린 회차를 만나면
 * {@link CouponRoundWarmupStatus#GATE_ALREADY_OPEN} 으로 손을 뗀다.
 */
public class CouponRoundWarmupRunner {

    private static final Logger log = LoggerFactory.getLogger(CouponRoundWarmupRunner.class);

    private final CouponRoundGateJdbc roundJdbc;
    private final RoundGateWriteGuard guard;
    private final IssuanceGatePort gate;
    private final IssuanceWarmupPort warmupPort;
    private final TimeProvider timeProvider;

    public CouponRoundWarmupRunner(
            CouponRoundGateJdbc roundJdbc,
            RoundGateWriteGuard guard,
            IssuanceGatePort gate,
            IssuanceWarmupPort warmupPort,
            TimeProvider timeProvider) {
        this.roundJdbc = roundJdbc;
        this.guard = guard;
        this.gate = gate;
        this.warmupPort = warmupPort;
        this.timeProvider = timeProvider;
    }

    public CouponRoundWarmupResult warmUp(long couponRoundId) {
        if (!guard.tryAcquire(couponRoundId)) {
            log.warn("워밍업을 건너뛴다 — 회차 {} 의 게이트 쓰기가 이미 돌고 있다.", couponRoundId);
            return CouponRoundWarmupResult.rejected(
                    couponRoundId, CouponRoundWarmupStatus.WARMUP_IN_PROGRESS);
        }
        try {
            return warmUpExclusively(couponRoundId);
        } finally {
            guard.release(couponRoundId);
        }
    }

    private CouponRoundWarmupResult warmUpExclusively(long couponRoundId) {
        // 1. 게이트가 이미 열려 있으면 손대지 않는다.
        // 여기서 걸리는 것은 "예전에 올라간 회차" 다. 지금 겹쳐 들어온 호출은 위 가드가 잡는다.
        if (gate.readMeta(couponRoundId).isPresent()) {
            log.warn("워밍업을 건너뛴다 — 회차 {} 의 게이트가 이미 열려 있다.", couponRoundId);
            return CouponRoundWarmupResult.rejected(
                    couponRoundId, CouponRoundWarmupStatus.GATE_ALREADY_OPEN);
        }

        Optional<CouponRoundGateJdbc.RoundRow> found = roundJdbc.findRound(couponRoundId);
        if (found.isEmpty()) {
            return CouponRoundWarmupResult.rejected(
                    couponRoundId, CouponRoundWarmupStatus.ROUND_NOT_FOUND);
        }
        CouponRoundGateJdbc.RoundRow round = found.get();
        if (round.totalQuantity() == null) {
            return CouponRoundWarmupResult.rejected(
                    couponRoundId, CouponRoundWarmupStatus.STOCK_ROW_MISSING);
        }
        if (round.engineVersion() != EngineVersion.V2) {
            return CouponRoundWarmupResult.rejected(
                    couponRoundId, CouponRoundWarmupStatus.ENGINE_NOT_V2);
        }
        if (!timeProvider.instant().isBefore(round.openAt())) {
            // 살아있는 회차를 올리는 것은 이 경로가 아니다. 그 경로는 게이트를 닫는 단계와
            // 4′ 재집계를 함께 요구하고, 그것이 CouponRoundRebuildRunner 다.
            return CouponRoundWarmupResult.rejected(
                    couponRoundId, CouponRoundWarmupStatus.ROUND_ALREADY_OPENED);
        }

        // 2. 활성 수·누적 수·회원 목록을 한 트랜잭션으로 읽는다.
        CouponRoundGateJdbc.Aggregate aggregate = roundJdbc.readAggregate(couponRoundId);
        if (aggregate.totalQuantity() == null) {
            // 회차 조회와 이 스냅샷 사이에 재고 행이 사라졌다.
            return CouponRoundWarmupResult.rejected(
                    couponRoundId, CouponRoundWarmupStatus.STOCK_ROW_MISSING);
        }

        // uk_coupon_member 가 회차당 회원 한 행을 강제하므로 이 둘은 같아야 한다(§9.1 I2).
        // 다르면 그 제약이 깨진 것이라, 여기서 조용히 목록 쪽을 택하면 I4 위반을 워밍업이
        // 만들어 낸다. 세지 않은 채 넘기지 않는다.
        CouponRoundGateWrites.requireMemberCountMatchesEverCount(couponRoundId, aggregate);

        long totalQuantity = aggregate.totalQuantity();
        long remainingStock = totalQuantity - aggregate.activeCount();
        if (remainingStock < 0) {
            // DB 에 이미 §9.1 I1 위반이 있다. 선점 Lua 는 음수 stock 을 정상 값으로 읽어
            // -5(매진)를 내므로 초과 발급으로 번지지는 않지만, 그 사고를 못 본 채
            // "워밍업 성공" 으로 보고하면 아무도 다시 안 본다.
            log.error("회차 {} 의 활성 건수({})가 총재고({})를 넘는다. 초과 발급이 이미 있다.",
                    couponRoundId, aggregate.activeCount(), totalQuantity);
            return CouponRoundWarmupResult.rejected(
                    couponRoundId, CouponRoundWarmupStatus.OVER_ISSUED_ROUND);
        }

        // 3. issued Hash · issued_ever · stock 을 한 번에.
        warmupPort.seedCounters(couponRoundId, aggregate.everMembers(), remainingStock);

        // 4. 회차를 V2 로 확정한다. **게이트를 여는 것과 엔진을 정하는 것은 같은 사건**이라,
        //    이 뒤로는 게이트를 여는 쓰기만 남는다. 여기서 잡히는 것은 워밍업이 도는 동안 엔진이
        //    뒤집힌 경우다 — 엔진 변경이 허용되는 조건(NOW < open_at)과 워밍업이 도는 조건이
        //    같아서 그 창은 실재한다. 잠근 뒤 죽으면 meta 가 없어 게이트가 닫힌 채 남고,
        //    잠금이 멱등이라 재실행이 복구한다.
        if (roundJdbc.lockEngineToV2(couponRoundId) != 1) {
            return CouponRoundWarmupResult.rejected(couponRoundId,
                    roundJdbc.roundExists(couponRoundId)
                            ? CouponRoundWarmupStatus.ENGINE_NOT_V2
                            : CouponRoundWarmupStatus.ROUND_NOT_FOUND);
        }

        // 5. DB_COUNTER_GAP(§9.1 I6)까지 정리한다. 이걸 빼면 워밍업 직후부터 그 축이 0 이 아니다.
        roundJdbc.updateActiveCount(couponRoundId, aggregate.activeCount(), timeProvider.instant());

        // 6. 게이트를 연다. 다섯 필드가 한 덩어리라 부분 상태가 남지 않는다.
        gate.writeMeta(couponRoundId, new GateMeta(
                GateStatus.OPEN,
                round.openAt().toEpochMilli(),
                round.closeAt().toEpochMilli(),
                round.gradeMask(),
                totalQuantity));

        log.info("회차 {} 워밍업 완료 — 총재고 {} · 활성 {} · 누적 {} · 잔여 {}",
                couponRoundId, totalQuantity, aggregate.activeCount(),
                aggregate.everCount(), remainingStock);
        return new CouponRoundWarmupResult(
                couponRoundId, CouponRoundWarmupStatus.WARMED, totalQuantity,
                aggregate.activeCount(), aggregate.everCount(), remainingStock);
    }
}
