package com.kafkick.core.coupon.service;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.kafkick.core.coupon.v2.port.CouponRoundIssuanceDefinitionRepository;
import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.core.coupon.v2.port.RestoreOutcome;
import com.kafkick.core.observation.EngineVersion;

/** V2 취소·만료의 Redis 재고 복원을 DB 커밋 뒤에 실행한다. */
@Service
public class V2StockRestorationService {

    private static final Logger log = LoggerFactory.getLogger(V2StockRestorationService.class);

    private final CouponRoundIssuanceDefinitionRepository definitions;
    private final ObjectProvider<IssuanceGatePort> gateProvider;

    public V2StockRestorationService(
            CouponRoundIssuanceDefinitionRepository definitions,
            ObjectProvider<IssuanceGatePort> gateProvider
    ) {
        this.definitions = Objects.requireNonNull(definitions, "definitions");
        this.gateProvider = Objects.requireNonNull(gateProvider, "gateProvider");
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
            if (gate == null) {
                // 커밋 뒤에 알아채면 늦다 — DB 는 이미 재고를 반납했고 Redis 는 모른 채로
                // 남아 그 회차가 영구 과소가 된다. 아직 트랜잭션 안이라 여기서 막는다.
                throw new IllegalStateException(
                        "V2 회차인데 재고 복원 게이트가 없습니다. couponRoundId=" + couponRoundId);
            }
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    restore(gate, couponRoundId, count);
                }
            });
        }
    }

    private void restore(IssuanceGatePort gate, long couponRoundId, long count) {
        try {
            RestoreOutcome outcome = gate.restore(couponRoundId, count);
            if (outcome != RestoreOutcome.RESTORED) {
                log.error("V2 재고 복원이 거절됐습니다. 수동 재동기화가 필요합니다. "
                        + "couponRoundId={}, count={}, outcome={}",
                        couponRoundId, count, outcome);
            }
        } catch (RuntimeException failure) {
            log.error("V2 재고 복원 호출에 실패했습니다. 수동 재동기화가 필요합니다. couponRoundId={}, count={}",
                    couponRoundId, count, failure);
        }
    }
}
