package com.kafkick.core.coupon.service;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.kafkick.core.coupon.v2.port.CouponRoundIssuanceDefinitionRepository;
import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.core.coupon.v2.port.RestorationHaltStore;
import com.kafkick.core.coupon.v2.port.RestoreOutcome;
import com.kafkick.core.coupon.v2.port.V2RestorationMeters;
import com.kafkick.core.observation.EngineVersion;

/** V2 취소·만료의 Redis 재고 복원을 DB 커밋 뒤에 실행한다. */
@Service
public class V2StockRestorationService {

    private static final Logger log = LoggerFactory.getLogger(V2StockRestorationService.class);

    /**
     * 표식을 <b>Redis 에 못 남겼을 때만</b> 쓰는 폴백. 본체는 프로세스 밖이다(그래야 취소와
     * 만료가 같은 표식을 본다) — 이 집합은 그 쓰기가 실패한 회차를 <b>이번 실행 동안만</b>
     * 붙잡는다. 없으면 Redis 순단 한 번에 남은 청크가 전부 돌아 이 장치가 통째로 무효가 된다.
     *
     * <p><b>여기 있는 회차를 저장소에 다시 쓰지 않는다.</b> 폴백은 "표식이 없다" 와 구별되지
     * 않으므로, 다시 쓰면 그 사이 재구성이 지운 표식까지 되살아난다 — 재고는 정상인데 만료만
     * 영구 정지고, 그건 아무 절차로도 못 푸는 상태다. 대신 다음 실행이 저장소의 현재 답을
     * 따른다. {@code -2} 는 이벤트가 아니라 상태라, 재고가 여전히 틀어져 있으면 그 실행이
     * 곧바로 다시 발견한다.
     */
    private final Set<Long> locallyHalted = ConcurrentHashMap.newKeySet();

    private final CouponRoundIssuanceDefinitionRepository definitions;
    private final ObjectProvider<IssuanceGatePort> gateProvider;
    private final ObjectProvider<RestorationHaltStore> haltStoreProvider;
    private final ObjectProvider<V2RestorationMeters> metersProvider;

    public V2StockRestorationService(
            CouponRoundIssuanceDefinitionRepository definitions,
            ObjectProvider<IssuanceGatePort> gateProvider,
            ObjectProvider<RestorationHaltStore> haltStoreProvider,
            ObjectProvider<V2RestorationMeters> metersProvider
    ) {
        this.definitions = Objects.requireNonNull(definitions, "definitions");
        this.gateProvider = Objects.requireNonNull(gateProvider, "gateProvider");
        this.haltStoreProvider = Objects.requireNonNull(haltStoreProvider, "haltStoreProvider");
        this.metersProvider = Objects.requireNonNull(metersProvider, "metersProvider");
    }

    public void restoreAfterCommit(long couponRoundId, long count) {
        if (definitions.findById(couponRoundId)
                .map(definition -> definition.engineVersion() == EngineVersion.V2)
                .orElse(false)) {
            if (!TransactionSynchronizationManager.isSynchronizationActive()) {
                // 조용히 넘어가면 비트랜잭션 호출부가 새로 생겨도 아무 테스트도 안 깨진 채
                // 재고만 증발한다. 그 사실이 드러나는 유일한 자리가 여기다.
                throw new IllegalStateException(
                        "V2 재고 복원은 트랜잭션 안에서만 호출할 수 있습니다. couponRoundId="
                                + couponRoundId + ", count=" + count);
            }
            IssuanceGatePort gate = gateProvider.getIfAvailable();
            RestorationHaltStore haltStore = haltStoreProvider.getIfAvailable();
            if (gate == null || haltStore == null) {
                // 커밋 뒤에 알아채면 늦다 — DB 는 이미 재고를 반납했고 Redis 는 모른 채로
                // 남아 그 회차가 영구 과소가 된다. 아직 트랜잭션 안이라 여기서 막는다.
                throw new IllegalStateException(
                        "V2 회차인데 재고 복원 게이트나 중단 표식 저장소가 없습니다. couponRoundId="
                                + couponRoundId);
            }
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    restore(gate, haltStore, couponRoundId, count);
                }
            });
        }
    }

    /**
     * 만료 배치 한 회차(run)의 시작을 알린다. 실행 사이에 재구성이 끼면 폴백이 낡은 답이
     * 되므로 여기서 걷는다 — 폴백의 수명을 실행 하나로 못박는 자리다.
     */
    public void beginExpirationRun() {
        locallyHalted.clear();
    }

    /**
     * 그 회차의 복원이 상한 초과로 멈춰 있는지. 만료 배치가 <b>남은 청크를 돌리기 전에</b>
     * 묻는다 — 계속 돌리면 DB 만 {@code EXPIRED} 로 전이되고 Redis 는 하나도 안 받아
     * 어긋남이 청크마다 커진다.
     *
     * <p>표식을 못 읽으면 <b>멈추지 않는다.</b> Redis 가 끊긴 상황에서는 복원 자체도 실패하고,
     * 그건 재고가 안 돌아오는 과소 방향이라 안전하다. 반대로 읽기 실패를 "멈춤" 으로 해석하면
     * Redis 순단 한 번이 전 회차의 만료를 세운다.
     */
    public boolean isRestorationHalted(long couponRoundId) {
        if (locallyHalted.contains(couponRoundId)) {
            return true;
        }
        try {
            RestorationHaltStore haltStore = haltStoreProvider.getIfAvailable();
            return haltStore != null && haltStore.isHalted(couponRoundId);
        } catch (RuntimeException failure) {
            // 해석 실패까지 여기서 받는다. 밖에 두면 그 예외가 만료 배치를 통째로 끊어
            // 그 틱의 <b>모든 회차</b>가 멈춘다 — 한 회차의 표식을 못 읽은 대가로는 과하다.
            log.error("복원 중단 표식을 읽지 못했습니다. 만료를 계속합니다. couponRoundId={}",
                    couponRoundId, failure);
            return false;
        }
    }

    /**
     * 계측은 없어도 도는 것이라 조립 시점에 요구하지 않는다. 해석이 실패해도 마찬가지다 —
     * <b>미터 때문에 복원 결과 처리가 끊기면 안 된다.</b> 표식을 세우는 일이 계측보다 앞선다.
     */
    private V2RestorationMeters meters() {
        try {
            V2RestorationMeters resolved = metersProvider.getIfAvailable();
            return resolved == null ? V2RestorationMeters.NONE : resolved;
        } catch (RuntimeException failure) {
            log.error("복원 계측을 해석하지 못했습니다. 카운터 없이 진행합니다.", failure);
            return V2RestorationMeters.NONE;
        }
    }

    /**
     * 계측 기록은 삼킨다. {@code restore} 는 {@code afterCommit} 안에서 도는데 Spring 은
     * 그 예외를 커밋 밖으로 전파한다 — 배치에서는 틱이 죽고, 취소 경로에서는 이미 커밋된
     * 취소가 500 이 된다. <b>미터 때문에 복원 결과 처리가 끊기면 안 된다</b>는 규칙이
     * 해석에만 걸려 있었다.
     */
    private static void record(Runnable recording) {
        try {
            recording.run();
        } catch (RuntimeException failure) {
            log.error("복원 계측을 기록하지 못했습니다.", failure);
        }
    }

    private void restore(
            IssuanceGatePort gate,
            RestorationHaltStore haltStore,
            long couponRoundId,
            long count
    ) {
        // 미터는 진입에서 한 번만 해석한다. catch 안에서 해석하면 그 조회가 던졌을 때
        // 원 실패가 통째로 덮여 무엇이 왜 실패했는지가 사라진다.
        V2RestorationMeters meters = meters();
        RestoreOutcome outcome;
        try {
            outcome = gate.restore(couponRoundId, count);
        } catch (RuntimeException failure) {
            record(meters::recordCallFailure);
            log.error("V2 재고 복원 호출에 실패했습니다. 수동 재동기화가 필요합니다. couponRoundId={}, count={}",
                    couponRoundId, count, failure);
            return;
        }
        record(() -> meters.recordOutcome(outcome));
        if (outcome == RestoreOutcome.OVER_CAP) {
            // 상한 초과는 그 시점에 이미 재고가 틀어져 있다는 신호다. 나머지 거절과
            // 뭉치지 않는다 — GATE_NOT_READY 까지 멈추면 재구성 창에서 만료가 통째로 선다.
            halt(haltStore, couponRoundId, meters);
            log.error("V2 재고 복원이 상한을 넘어 거절됐습니다. 이 회차의 만료를 멈춥니다. "
                    + "수동 재동기화가 필요합니다. couponRoundId={}, count={}",
                    couponRoundId, count);
        } else if (outcome != RestoreOutcome.RESTORED) {
            log.error("V2 재고 복원이 거절됐습니다. 수동 재동기화가 필요합니다. "
                    + "couponRoundId={}, count={}, outcome={}",
                    couponRoundId, count, outcome);
        }
    }

    /**
     * 표식 쓰기는 복원 호출과 <b>다른 {@code try} 다.</b> 한 덩어리로 두면 쓰기 실패가
     * "복원 호출 실패" 로 뭉뚱그려져 표식이 안 서고, 남은 청크가 전부 돈다 — 막으려던 그
     * 어긋남이 그대로 자란다. 게다가 스크립트는 답을 냈는데 {@code CALL_FAILED} 까지 올라가
     * 경보 판독이 뒤바뀐다.
     */
    private void halt(
            RestorationHaltStore haltStore, long couponRoundId, V2RestorationMeters meters) {
        try {
            haltStore.halt(couponRoundId);
            // 남긴 사실을 이 실행 동안은 로컬로도 안다. 없으면 Redis 순단 한 번에 읽기가
            // 던져 false 로 떨어지고, -2 를 이미 본 회차인데 남은 청크가 전부 돈다 —
            // 방어가 쓰기 실패에만 있고 읽기 실패에는 없는 비대칭이 된다.
            // 저장소에 되쓰지 않으므로(폴백 주석) 재구성이 푼 표식을 부활시키지 않는다.
            locallyHalted.add(couponRoundId);
            return;
        } catch (RuntimeException first) {
            // 한 번 더 시도한다. 여기서 포기하면 <b>타임아웃 한 번이 곧 방어 상실</b>이다 —
            // 취소 경로(api)에는 폴백을 읽는 쪽이 없어 그 회차가 멈춘 사실이 통째로 사라진다.
            try {
                haltStore.halt(couponRoundId);
                log.warn("복원 중단 표식을 재시도로 남겼습니다. couponRoundId={}", couponRoundId);
                return;
            } catch (RuntimeException second) {
                // 같은 인스턴스면 addSuppressed 가 IllegalArgumentException 을 던져 원 실패를
                // 통째로 덮는다(V2CouponIssueService.suppress 와 같은 이유).
                if (first != second) {
                    first.addSuppressed(second);
                }
            }
            locallyHalted.add(couponRoundId);
            record(meters::recordHaltWriteFailure);
            // 폴백은 만료 배치 프로세스에서만 읽힌다. 취소 경로에서는 이 카운터가 유일한 신호다.
            log.error("복원 중단 표식을 남기지 못했습니다. 만료 배치 프로세스라면 이번 실행만 "
                    + "멈추고, 그 외에는 아무도 이 회차를 멈추지 못합니다. couponRoundId={}",
                    couponRoundId, first);
        }
    }

}
