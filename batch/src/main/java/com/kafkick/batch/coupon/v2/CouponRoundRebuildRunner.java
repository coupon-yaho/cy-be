package com.kafkick.batch.coupon.v2;

import java.time.Duration;
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
 * <b>이미 열린</b> 회차의 게이트를 닫고 카운터를 다시 세운 뒤 다시 연다 — 설계 §6.2 의
 * 재구성 절차다.
 *
 * <h2>이 경로가 없으면 되살릴 방법이 없다</h2>
 *
 * <p>워밍업은 열린 회차를 거절한다({@code ROUND_ALREADY_OPENED}·{@code GATE_ALREADY_OPEN}) —
 * 그 자리에서는 그것이 옳다. 그래서 열린 회차의 {@code cy:v2:*} 키를 잃으면 그 회차는
 * {@code meta} 가 없어 발급이 전량 {@code -9} → 503 인 채로 <b>영구히</b> 남는다.
 * Sentinel 을 얹으면 failover 유실로 이 상태가 정기적으로 생기므로(§6.4), 이 경로는
 * 그 단위의 전제다.
 *
 * <h2>순서가 전부다</h2>
 *
 * <pre>
 * 0. 회차 정의를 읽고 사전 점검      ← 게이트를 닫기 전에 걸릴 것은 여기서 걸린다
 * 1. meta UNLINK                    ← 게이트를 **먼저** 닫는다
 * 2. DB 집계를 한 트랜잭션으로 읽는다
 * 3. issued Hash · issued_ever · stock  (어댑터가 이 순서로 한 번에)
 * 4. issuance_engine_locked = TRUE · coupon_stocks.active_count
 * 4′. **meta 직전에 활성 집계를 다시 읽어 stock 을 갱신한다**
 * 5. meta 작성                       ← 게이트를 **마지막에** 연다
 * </pre>
 *
 * <p>1 과 5 가 안전장치다. 도중에 죽어도 {@code meta} 가 없는 상태로 남아 그 회차의 발급은
 * 전량 {@code -9} 이고, 다시 돌리는 것이 곧 복구다(멱등).
 *
 * <h2>4′ 가 이 클래스의 핵심이다</h2>
 *
 * <p><b>게이트는 발급만 막는다.</b> 취소·사용취소·만료는 계속 돌고 DB 커밋도 계속 된다.
 * 복원 경로는 {@code meta} 부재를 보고 {@code -1} 을 돌려주므로 Redis {@code INCR} 은 건너뛰지만,
 * <b>2번 집계 이후 커밋된 그 복원분은 3번의 {@code stock} 계산값에 안 들어간다</b> — 그만큼
 * 재고가 적게 복구된다. 4′ 를 빼면 재구성 창 동안의 취소가 조용히 유실된다.
 *
 * <p>유실될 뻔한 건수는 {@code V2RestorationMeters} 의 {@code GATE_NOT_READY}({@code -1})
 * 카운터가 세고 있어, 재구성 뒤 그 카운터가 서는지로 수렴을 확인한다.
 *
 <h2>1.5 — 진행 중인 쓰기를 기다린다 (설계에 없던 단계)</h2>
 *
 * <p><b>게이트를 닫는 것으로 이 회차의 쓰기가 멎지는 않는다.</b> 닫기 전에 이미 시작된 두 종류가
 * 남아 있고, 설계 §6.2 에도 07 에도 그것을 기다리는 단계가 없었다.
 *
 * <ul>
 * <li><b>선점만 끝난 발급.</b> v2 발급은 선점 → DB 커밋 → 완료 순서라({@code V2CouponIssueService}),
 *     닫기 직전에 선점한 요청의 {@code issuances} INSERT 는 그 뒤에 커밋된다. 2번 집계 이후에
 *     커밋되면 그 회원은 재작성된 {@code issued} 에 없어 <b>{@code HLEN}·{@code issued_ever} 가
 *     둘 다 DB 보다 작아지고</b>({@code LUA_GAP}), 재오픈 뒤 같은 회원의 재선점이 Lua 를 통과한다
 *     — 1인 1매의 최종 방어선은 {@code uk_coupon_member} 하나만 남는다. 4′ 뒤에 커밋되면
 *     {@code stock} 이 한 장 많다.</li>
 * <li><b>커밋된 취소의 복원.</b> 복원의 Redis {@code INCR} 은 {@code afterCommit} 이라 DB 커밋보다
 *     늦다({@code V2StockRestorationService}). 커밋이 4′ 재집계보다 앞서고 그 {@code INCR} 이
 *     {@code meta} 쓰기보다 뒤에 도착하면 <b>같은 취소가 두 번</b> 반영된다 — 재집계에 한 번,
 *     {@code INCRBY} 로 한 번. 복원 Lua 의 상한은 {@code stock ≤ total} 이라 이걸 못 막는다.</li>
 * </ul>
 *
 * <p>그래서 게이트를 닫은 <b>직후·집계를 읽기 전에</b> {@code coupon.rebuild.drain} 만큼
 * 기다린다. 07 이 복원 중단 표식에 대해 "게이트를 닫고 진행 중인 복원이 빠진 뒤에 시딩해야
 * 한다" 고 적은 요구도 같은 대기 하나로 함께 지켜진다 — 표식을 지우는 것은 시딩이고, 시딩은
 * 이 대기 뒤다.
 *
 * <p><b>이 대기는 barrier 가 아니다.</b> 진행 중인 트랜잭션의 완료를 확인하지 않고 정해진
 * 시간만 잔다 — 그보다 오래 걸린 커밋은 그냥 늦게 도착한다. 대기는 창을 <b>좁힐</b> 뿐이고,
 * 그래서 아래 두 가지가 남는다. 이 문단을 "그 뒤로는 안전하다" 로 읽으면 안 된다.
 *
 * <ul>
 * <li><b>늦은 발급.</b> 4′ 에서 누적 수를 다시 세어 <b>탐지</b>하고, 잡히면 최신 목록으로
 *     <b>한 번 더 시딩</b>한다. 그 재시딩보다도 늦은 커밋은 남고, 그때는 몇 건인지를 결과와
 *     로그로 낸다 — {@code issued} 에 없는 회원은 Redis 층의 1인 1매 방어가 없다.</li>
 * <li><b>늦은 복원.</b> 4′ 재집계에 잡힌 취소의 {@code INCR} 이 {@code meta} 쓰기보다 뒤에
 *     도착하면 그 한 건이 두 번 반영된다(재집계에 한 번, {@code INCRBY} 로 한 번). 4′ 와
 *     {@code meta} 사이는 명령 두 개라 창은 밀리초 아래지만 0 은 아니다. 닫으려면 07 이
 *     검토하고 접은 재구성 세대 필드가 필요하고, {@code meta} 5필드 형식과 Lua 다섯 종이 함께
 *     움직이는 일이라 이 단위에서 하지 않는다. {@code LUA_GAP} 이 그 편차를 잡는다.</li>
 * </ul>
 *
 * <p><b>07 이 함께 적은 "{@code issued} 의 {@code P} 재계수" 는 이 조립에서 항상 0 이다.</b>
 * 3번 시딩이 모든 값을 {@code D}(완료)와 {@code __rebuilt__} 토큰으로 덮으므로, 창 안에 도착한
 * 보상은 토큰 CAS 에서 갈려 {@code 0}(NOT_MINE)을 받고 {@code INCR} 하지 않는다. 세어 봐야
 * 늘 0 인 값을 빼는 대신 그 근거를 여기 적는다 — 시딩의 값 형식이 바뀌면 이 문장부터 깨진다.
 *
 * <h2>동시 실행</h2>
 *
 * <p>워밍업과 <b>같은 {@link RoundGateWriteGuard}</b> 를 공유한다. 다만 그것은 프로세스 안이
 * 전부다 — batch 가 1대라는 compose 의 계약({@code replicas: 1})이 나머지를 진다. 그 전제가
 * 깨지는 날에는 07 의 (b) 회차 단위 Redis 락이 선행 조건이다.
 */
public class CouponRoundRebuildRunner {

    private static final Logger log = LoggerFactory.getLogger(CouponRoundRebuildRunner.class);

    private final CouponRoundGateJdbc roundJdbc;
    private final RoundGateWriteGuard guard;
    private final IssuanceGatePort gate;
    private final IssuanceWarmupPort warmupPort;
    private final TimeProvider timeProvider;
    private final Duration drain;

    public CouponRoundRebuildRunner(
            CouponRoundGateJdbc roundJdbc,
            RoundGateWriteGuard guard,
            IssuanceGatePort gate,
            IssuanceWarmupPort warmupPort,
            TimeProvider timeProvider,
            Duration drain) {
        this.roundJdbc = roundJdbc;
        this.guard = guard;
        this.gate = gate;
        this.warmupPort = warmupPort;
        this.timeProvider = timeProvider;
        this.drain = drain;
    }

    public CouponRoundRebuildResult rebuild(long couponRoundId) {
        if (!guard.tryAcquire(couponRoundId)) {
            log.warn("재구성을 건너뛴다 — 회차 {} 의 게이트 쓰기가 이미 돌고 있다.", couponRoundId);
            return CouponRoundRebuildResult.rejected(
                    couponRoundId, CouponRoundRebuildStatus.REBUILD_IN_PROGRESS);
        }
        try {
            return rebuildExclusively(couponRoundId);
        } finally {
            guard.release(couponRoundId);
        }
    }

    /**
     * 게이트를 닫은 뒤 진행 중인 쓰기가 빠지기를 기다린다.
     *
     * <p>인터럽트되면 <b>던진다.</b> 덜 기다린 채 계속 가면 이 대기가 막으려던 창이 그대로
     * 열리는데, 그 사실은 결과 어디에도 안 남는다. 게이트는 닫힌 채 남고 재실행이 복구한다.
     */
    private void drainInFlightWrites(long couponRoundId) {
        if (drain.isZero()) {
            return;
        }
        log.info("회차 {} — 진행 중인 발급·복원이 빠지기를 {} 기다린다.", couponRoundId, drain);
        try {
            Thread.sleep(drain.toMillis());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "회차 " + couponRoundId + " 재구성의 drain 대기가 끊겼습니다."
                            + " 게이트는 닫힌 채입니다 — 다시 돌리면 복구됩니다.", interrupted);
        }
    }

    private CouponRoundRebuildResult rebuildExclusively(long couponRoundId) {
        // 0. 게이트를 닫기 **전에** 걸릴 것을 먼저 건다. 여기서 거절하면 그 회차의 발급은
        //    한 건도 안 멈춘다. 아래 사전 점검을 게이트를 닫은 뒤로 미루면, 대상을 잘못 고른
        //    호출 하나가 멀쩡한 회차를 세워 놓고 끝난다.
        //
        //    **게이트가 열려 있는지는 보지 않는다.** 이 경로에서 "이미 열려 있음" 은 정상
        //    입력이고, 그것이 워밍업과 갈리는 유일한 진입 조건이다.
        Optional<CouponRoundGateJdbc.RoundRow> found = roundJdbc.findRound(couponRoundId);
        if (found.isEmpty()) {
            return CouponRoundRebuildResult.rejected(
                    couponRoundId, CouponRoundRebuildStatus.ROUND_NOT_FOUND);
        }
        CouponRoundGateJdbc.RoundRow round = found.get();
        if (round.totalQuantity() == null) {
            return CouponRoundRebuildResult.rejected(
                    couponRoundId, CouponRoundRebuildStatus.STOCK_ROW_MISSING);
        }
        if (round.engineVersion() != EngineVersion.V2) {
            return CouponRoundRebuildResult.rejected(
                    couponRoundId, CouponRoundRebuildStatus.ENGINE_NOT_V2);
        }
        // 사전 점검. 이 값은 게이트가 열린 채로 읽은 것이라 그 자체로는 아무것도 확정하지
        // 못하지만, 이미 초과 발급이 있는 회차를 **닫기 전에** 걸러 낸다.
        long preflightActive = roundJdbc.readActiveCount(couponRoundId);
        if (round.totalQuantity() - preflightActive < 0) {
            log.error("회차 {} 는 이미 활성 건수({})가 총재고({})를 넘는다. 게이트를 닫지 않는다.",
                    couponRoundId, preflightActive, round.totalQuantity());
            return CouponRoundRebuildResult.overIssued(couponRoundId, false, round.totalQuantity(),
                    preflightActive, CouponRoundRebuildResult.UNKNOWN);
        }

        // 1. 게이트를 닫는다. 이 뒤로 그 회차의 선점은 전량 -9, 복원은 -1 이다.
        //    UNLINK 라 회수가 다른 스레드로 넘어간다 — DEL 이면 그 시간만큼 발급 전체가 선다.
        log.warn("회차 {} 재구성 시작 — 게이트를 닫는다. 이 회차의 발급은 지금부터 503 이다.",
                couponRoundId);
        gate.closeGate(couponRoundId);

        try {
            return rebuildWithGateClosed(couponRoundId, round);
        } catch (RuntimeException failure) {
            // **예외를 그대로 흘리면 500 이 나가고, 게이트가 닫혔다는 사실이 응답에서 사라진다.**
            // 원인은 스택으로 남기고 응답에는 "닫힌 채 멈췄다" 를 남긴다 — 운영자가 다음에 할
            // 일(다시 돌린다)이 그 둘 중 뒤엣것으로만 정해지기 때문이다.
            log.error("회차 {} 재구성이 예상치 못한 실패로 멈췄다. **게이트는 닫힌 채다** —"
                    + " 그 회차의 발급은 다시 돌릴 때까지 전량 503 이다.", couponRoundId, failure);
            return CouponRoundRebuildResult.rejectedAfterClose(
                    couponRoundId, CouponRoundRebuildStatus.REBUILD_FAILED);
        }
    }

    /**
     * 게이트가 닫힌 창. <b>여기서 나가는 모든 길은 게이트를 다시 열거나
     * {@code gateClosed = true} 를 달고 나간다</b> — 예외로 빠지는 길까지 포함해서다.
     */
    private CouponRoundRebuildResult rebuildWithGateClosed(
            long couponRoundId, CouponRoundGateJdbc.RoundRow round) {

        // 1.5 진행 중인 쓰기가 빠지기를 기다린다. 이 대기가 없으면 닫기 직전에 선점한 발급의
        //     커밋이 아래 집계 뒤에 도착해 그 회원이 issued 에서 통째로 빠지고(LUA_GAP), 취소의
        //     늦은 INCR 이 재구성이 다시 쓴 stock 위에 얹힌다.
        drainInFlightWrites(couponRoundId);

        // 2. 활성 수·누적 수·회원 목록을 한 트랜잭션으로 읽는다. 게이트를 닫은 **뒤에** 읽는
        //    것이 요점이다 — 닫기 전 스냅샷을 쓰면 그 사이의 발급이 카운터에서 통째로 빠진다.
        CouponRoundGateJdbc.Aggregate aggregate = roundJdbc.readAggregate(couponRoundId);
        if (aggregate.totalQuantity() == null) {
            // 여기부터의 거절은 전부 게이트를 닫은 채 끝난다 — 그 사실이 결과에 실려야 한다.
            return closedAndRejected(couponRoundId, CouponRoundRebuildStatus.STOCK_ROW_MISSING);
        }
        CouponRoundGateWrites.requireMemberCountMatchesEverCount(couponRoundId, aggregate);

        long totalQuantity = aggregate.totalQuantity();
        if (totalQuantity - aggregate.activeCount() < 0) {
            // 사전 점검과 이 스냅샷 사이에 넘어섰다. 게이트는 닫힌 채 남는다 — 음수 stock 위에
            // 게이트를 다시 여는 것보다 낫다. 그쪽은 초과 발급을 매진으로 굳힌다.
            log.error("회차 {} 의 활성 건수({})가 총재고({})를 넘는다. 게이트를 닫은 채 멈춘다.",
                    couponRoundId, aggregate.activeCount(), totalQuantity);
            return CouponRoundRebuildResult.overIssued(couponRoundId, true, totalQuantity,
                    aggregate.activeCount(), CouponRoundRebuildResult.UNKNOWN);
        }

        // 3. issued Hash · issued_ever · stock 을 한 번에. issued_ever 를 여기서 빠뜨리면
        //    그 순간 LUA_GAP ≠ 0 이라 재구성 자체가 정합성 사고가 된다 — 그래서 한 시그니처다.
        warmupPort.seedCounters(
                couponRoundId, aggregate.everMembers(), totalQuantity - aggregate.activeCount());

        // 4. 엔진 확정(멱등)과 DB_COUNTER_GAP 정리.
        if (roundJdbc.lockEngineToV2(couponRoundId) != 1) {
            return closedAndRejected(couponRoundId, roundJdbc.roundExists(couponRoundId)
                    ? CouponRoundRebuildStatus.ENGINE_NOT_V2
                    : CouponRoundRebuildStatus.ROUND_NOT_FOUND);
        }

        // 4′. **meta 를 쓰기 직전에 활성 집계를 다시 읽는다.** 게이트는 발급만 막으므로 2번
        //     이후에도 취소·사용취소·만료가 커밋된다. 그 건들의 복원은 meta 부재를 보고 -1 을
        //     받아 Redis INCR 을 건너뛰었고, 3번의 stock 계산에도 안 들어갔다 — 여기서 다시
        //     세지 않으면 그만큼 재고가 적게 복구된 채로 게이트가 열린다.
        //
        //     **이 값이 단조 감소한다고 가정하지 마라.** 게이트를 닫아도 이미 선점을 끝낸
        //     발급의 DB 커밋은 그 뒤에 도착한다 — drain 이 그 대부분을 걷지만 전부는 아니다.
        //     그래서 아래 초과 검사가 2번의 것과 별개로 필요하다.
        //
        //     세는 것과 coupon_stocks 를 쓰는 것은 한 트랜잭션이다 — 나뉘면 그 사이에 커밋된
        //     취소의 감소분을 절대값 UPDATE 가 덮어 없앤다.
        CouponRoundGateJdbc.Recount recount = roundJdbc.recountAndUpdateActiveCount(
                couponRoundId, totalQuantity, timeProvider.instant());

        // 4″. 시딩 뒤에 발급이 커밋됐으면 **최신 목록으로 한 번 더 시딩한다.** 그 회원은 방금
        //     다시 쓴 issued Hash 에 없어 Redis 층의 1인 1매 방어가 없는데, 게이트는 아직 닫혀
        //     있으니 지금이 고칠 수 있는 마지막 순간이다.
        //
        //     **한 번만 돈다.** 다시 돌 때마다 또 늦은 커밋이 올 수 있어 반복은 부하 중에
        //     수렴하지 못하고, 시딩은 O(N) 이라(§3.3) 그 반복이 그대로 Redis 단일 스레드에
        //     쌓인다. 남는 것은 아래에서 세어 보고한다.
        long seededEverCount = aggregate.everCount();
        if (recount.applied() && recount.everCount() > seededEverCount) {
            log.warn("회차 {} — 시딩({})과 4′({}) 사이에 발급이 커밋됐다. 최신 목록으로 다시 시딩한다.",
                    couponRoundId, seededEverCount, recount.everCount());
            CouponRoundGateJdbc.Aggregate late = roundJdbc.readAggregate(couponRoundId);
            if (late.totalQuantity() == null) {
                return closedAndRejected(couponRoundId, CouponRoundRebuildStatus.STOCK_ROW_MISSING);
            }
            CouponRoundGateWrites.requireMemberCountMatchesEverCount(couponRoundId, late);
            if (totalQuantity - late.activeCount() >= 0) {
                // 초과면 다시 쓰지 않는다 — 음수 stock 을 세우게 된다. 아래 재집계가 같은
                // 결론을 내고 게이트를 닫은 채 끝낸다.
                warmupPort.seedCounters(couponRoundId, late.everMembers(),
                        totalQuantity - late.activeCount());
                seededEverCount = late.everCount();
                recount = roundJdbc.recountAndUpdateActiveCount(
                        couponRoundId, totalQuantity, timeProvider.instant());
            }
        }
        long recountedActive = recount.activeCount();
        long remainingStock = totalQuantity - recountedActive;
        if (!recount.applied()) {
            // 재집계값을 쓰지 못했다 — active_count 는 옛 값 그대로다.
            // drain 을 넘겨 도착한 선점분이 총재고를 넘겼다. **음수를 Redis 에 쓰지도 않는다** —
            // 선점 Lua 는 음수 stock 을 -5(매진)로 읽어 초과 발급을 정상 상태로 굳히고, 그 뒤
            // 취소가 들어와도 INCR 이 0 을 넘길 때까지 재고가 한 장도 안 돌아온다.
            // 게이트가 닫힌 채 남는 것이 그것보다 낫다. DB 를 고친 뒤 다시 돌리는 것이 복구다.
            log.error("회차 {} 의 재집계 활성 건수({})가 총재고({})를 넘는다."
                            + " 게이트를 닫은 채 멈춘다 — 집계 시점에는 {} 였다.",
                    couponRoundId, recountedActive, totalQuantity, aggregate.activeCount());
            return CouponRoundRebuildResult.overIssued(
                    couponRoundId, true, totalQuantity, aggregate.activeCount(), recountedActive);
        }
        warmupPort.setRemainingStock(couponRoundId, remainingStock);

        if (recount.everCount() > seededEverCount) {
            // 시딩 뒤에 발급이 커밋됐다 — drain 을 넘겨 도착한 선점분이다. 그 회원은 방금 다시
            // 쓴 issued Hash 에 없어 **Redis 층의 1인 1매 방어가 없다**(DB 의 uk_coupon_member
            // 하나만 남는다). stock 은 4′ 가 바로잡았으므로 게이트는 연다 — 닫아 두면 부하 중
            // 재구성이 영원히 수렴하지 못한다. 대신 그 사실을 여기서 말한다. 지금까지 이 상태의
            // 유일한 신호는 사후 LUA_GAP 뿐이었다.
            log.error("회차 {} — 다시 시딩한 뒤({})에도 발급 {}건이 더 커밋됐다({})."
                            + " 그 회원들은 issued Hash 에 없다. 다시 돌려 채워라.",
                    couponRoundId, seededEverCount,
                    recount.everCount() - seededEverCount, recount.everCount());
        }

        // 5. 게이트를 연다. 다섯 필드가 한 덩어리라 부분 상태가 남지 않는다.
        gate.writeMeta(couponRoundId, new GateMeta(
                GateStatus.OPEN,
                round.openAt().toEpochMilli(),
                round.closeAt().toEpochMilli(),
                round.gradeMask(),
                totalQuantity));

        log.warn("회차 {} 재구성 완료 — 총재고 {} · 활성 {}→{} · 누적 {} · 잔여 {}",
                couponRoundId, totalQuantity, aggregate.activeCount(), recountedActive,
                seededEverCount, remainingStock);
        return new CouponRoundRebuildResult(
                couponRoundId, CouponRoundRebuildStatus.REBUILT, false, totalQuantity,
                aggregate.activeCount(), recountedActive, seededEverCount,
                recount.everCount(), remainingStock);
    }

    /**
     * 게이트를 닫은 뒤의 거절. <b>{@code meta} 를 다시 쓰지 않는다</b> — 이 시점에 다시 열면
     * 카운터가 절반만 선 상태 위에서 발급이 돈다. 그 회차는 다시 돌릴 때까지 503 이고, 그
     * 사실이 결과에 실려 나가는 것이 이 메서드의 전부다.
     */
    private CouponRoundRebuildResult closedAndRejected(
            long couponRoundId, CouponRoundRebuildStatus status) {
        log.error("회차 {} 재구성이 {} 로 멈췄다. **게이트는 닫힌 채다** — 그 회차의 발급은"
                + " 다시 돌릴 때까지 전량 503 이다.", couponRoundId, status);
        return CouponRoundRebuildResult.rejectedAfterClose(couponRoundId, status);
    }
}
