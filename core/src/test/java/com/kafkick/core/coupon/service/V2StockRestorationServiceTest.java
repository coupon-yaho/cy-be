package com.kafkick.core.coupon.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.kafkick.core.coupon.v2.CouponRoundIssuanceDefinition;
import com.kafkick.core.coupon.v2.port.CouponRoundIssuanceDefinitionRepository;
import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.core.coupon.v2.port.RestoreOutcome;
import com.kafkick.core.observation.EngineVersion;

class V2StockRestorationServiceTest {

    private final CouponRoundIssuanceDefinitionRepository definitions = mock(
            CouponRoundIssuanceDefinitionRepository.class);
    private final IssuanceGatePort gate = mock(IssuanceGatePort.class);
    @SuppressWarnings("unchecked")
    private final ObjectProvider<IssuanceGatePort> gateProvider = mock(ObjectProvider.class);
    private final V2StockRestorationService service = new V2StockRestorationService(
            definitions, gateProvider);

    @AfterEach
    void clearSynchronization() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    void restoresV2StockOnlyAfterDatabaseCommit() {
        when(definitions.findById(10L)).thenReturn(Optional.of(
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2)));
        when(gateProvider.getIfAvailable()).thenReturn(gate);
        when(gate.restore(10L, 2L)).thenReturn(RestoreOutcome.RESTORED);
        TransactionSynchronizationManager.initSynchronization();

        service.restoreAfterCommit(10L, 2L);

        verify(gate, never()).restore(10L, 2L);
        for (TransactionSynchronization synchronization
                : TransactionSynchronizationManager.getSynchronizations()) {
            synchronization.afterCommit();
        }
        verify(gate).restore(10L, 2L);
    }

    @Test
    void doesNotCallRedisForV1Round() {
        when(definitions.findById(10L)).thenReturn(Optional.of(
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V1)));
        TransactionSynchronizationManager.initSynchronization();

        service.restoreAfterCommit(10L, 1L);

        verify(gate, never()).restore(10L, 1L);
    }

    /**
     * 조용히 넘어가면 <b>비트랜잭션 호출부가 새로 생겨도 테스트가 전부 통과한다</b> — 복원이
     * 통째로 사라진 채 재고만 조용히 증발한다. 조립 시점에 깨지는 쪽을 택한다.
     */
    @Test
    void failsFastWhenNoTransactionSynchronizationIsActive() {
        when(definitions.findById(10L)).thenReturn(Optional.of(
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2)));

        assertThatThrownBy(() -> service.restoreAfterCommit(10L, 1L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("트랜잭션");

        verify(gate, never()).restore(10L, 1L);
    }

    /**
     * 게이트 빈이 없는 인스턴스에서 V2 회차 취소가 들어온 경우다. 커밋된 뒤에 알아채면
     * 늦다 — DB 는 이미 재고를 반납했고 Redis 는 모른 채로 남아 그 회차가 영구 과소가 된다.
     * 등록 시점(커밋 전)에 막아 취소 자체를 롤백시킨다.
     */
    @Test
    void failsFastWhenTheGateBeanIsMissing() {
        when(definitions.findById(10L)).thenReturn(Optional.of(
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2)));
        when(gateProvider.getIfAvailable()).thenReturn(null);
        TransactionSynchronizationManager.initSynchronization();

        assertThatThrownBy(() -> service.restoreAfterCommit(10L, 1L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("게이트");

        assertThat(TransactionSynchronizationManager.getSynchronizations()).isEmpty();
    }

    /**
     * 회차 정의가 사라진 뒤 도착한 취소다. 엔진을 모르므로 Redis 를 건드리지 않는다 —
     * 여기서 복원하면 게이트가 없는 회차에 재고를 얹는 초과 발급 방향이다.
     */
    @Test
    void staysSilentWhenTheRoundDefinitionIsGone() {
        when(definitions.findById(10L)).thenReturn(Optional.empty());

        service.restoreAfterCommit(10L, 1L);

        verify(gate, never()).restore(10L, 1L);
    }

    /** V1 회차는 Redis 를 안 쓰므로 트랜잭션 밖에서 불려도 아무 일도 없어야 한다. */
    @Test
    void staysSilentForV1RoundWithoutTransaction() {
        when(definitions.findById(10L)).thenReturn(Optional.of(
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V1)));

        service.restoreAfterCommit(10L, 1L);

        verify(gate, never()).restore(10L, 1L);
    }
}
