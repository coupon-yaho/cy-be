package com.kafkick.core.coupon.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.kafkick.core.coupon.v2.CouponRoundIssuanceDefinition;
import com.kafkick.core.coupon.v2.port.CouponRoundIssuanceDefinitionRepository;
import com.kafkick.core.coupon.v2.port.IssuanceGatePort;
import com.kafkick.core.coupon.v2.port.RestorationHaltStore;
import com.kafkick.core.coupon.v2.port.RestoreOutcome;
import com.kafkick.core.coupon.v2.port.V2RestorationMeters;
import com.kafkick.core.observation.EngineVersion;

class V2StockRestorationServiceTest {

    private final CouponRoundIssuanceDefinitionRepository definitions = mock(
            CouponRoundIssuanceDefinitionRepository.class);
    private final IssuanceGatePort gate = mock(IssuanceGatePort.class);
    @SuppressWarnings("unchecked")
    private final ObjectProvider<IssuanceGatePort> gateProvider = mock(ObjectProvider.class);
    private final RestorationHaltStore haltStore = new InMemoryHaltStore();
    @SuppressWarnings("unchecked")
    private final ObjectProvider<RestorationHaltStore> haltStoreProvider = mock(ObjectProvider.class);
    private final RecordingMeters meters = new RecordingMeters();
    @SuppressWarnings("unchecked")
    private final ObjectProvider<V2RestorationMeters> metersProvider = mock(ObjectProvider.class);
    private final V2StockRestorationService service = new V2StockRestorationService(
            definitions, gateProvider, haltStoreProvider, metersProvider);

    @org.junit.jupiter.api.BeforeEach
    void wireProviders() {
        org.mockito.Mockito.lenient().when(haltStoreProvider.getIfAvailable()).thenReturn(haltStore);
        org.mockito.Mockito.lenient().when(metersProvider.getIfAvailable()).thenReturn(meters);
    }

    /** 표식 저장소는 프로세스 밖(Redis)이 본체다. 여기서는 그 계약만 흉내 낸다. */
    private static final class InMemoryHaltStore implements RestorationHaltStore {

        private final java.util.Set<Long> halted = java.util.concurrent.ConcurrentHashMap.newKeySet();

        @Override
        public void halt(long couponRoundId) {
            halted.add(couponRoundId);
        }

        @Override
        public boolean isHalted(long couponRoundId) {
            return halted.contains(couponRoundId);
        }

        @Override
        public void clear(long couponRoundId) {
            halted.remove(couponRoundId);
        }
    }

    private static final class RecordingMeters implements V2RestorationMeters {

        private final java.util.List<RestoreOutcome> outcomes = new java.util.ArrayList<>();
        private int callFailures;
        private int haltWriteFailures;

        @Override
        public void recordOutcome(RestoreOutcome outcome) {
            outcomes.add(outcome);
        }

        @Override
        public void recordCallFailure() {
            callFailures++;
        }

        @Override
        public void recordHaltWriteFailure() {
            haltWriteFailures++;
        }
    }

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

    /**
     * 상한 초과(`-2`)는 그 시점에 이미 재고가 틀어져 있다는 신호다. 다음 청크를 계속
     * 돌리면 DB 만 EXPIRED 로 전이되고 Redis 는 하나도 안 받아 <b>어긋남이 청크마다
     * 커진다.</b> 회차를 표시해 호출부가 멈출 수 있게 한다.
     */
    @Test
    void marksTheRoundHaltedWhenRestoreExceedsTheCap() {
        when(definitions.findById(10L)).thenReturn(Optional.of(
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2)));
        when(gateProvider.getIfAvailable()).thenReturn(gate);
        when(gate.restore(10L, 2L)).thenReturn(RestoreOutcome.OVER_CAP);
        TransactionSynchronizationManager.initSynchronization();

        service.restoreAfterCommit(10L, 2L);
        assertThat(service.isRestorationHalted(10L)).isFalse();
        for (TransactionSynchronization synchronization
                : TransactionSynchronizationManager.getSynchronizations()) {
            synchronization.afterCommit();
        }

        assertThat(service.isRestorationHalted(10L)).isTrue();
        assertThat(service.isRestorationHalted(11L)).isFalse();
    }

    /**
     * 나머지 거절은 처리가 다르다 — `-1` 은 재구성 수렴의 입력, `-3` 은 버그, `-11` 은
     * 운영 개입이다. 넷을 한 덩어리로 뭉쳐 전부 멈추면 재구성 창에서 만료가 통째로 선다.
     */
    @ParameterizedTest
    @EnumSource(value = RestoreOutcome.class,
            names = {"RESTORED", "GATE_NOT_READY", "BAD_ARGUMENT", "STOCK_MISSING"})
    void doesNotHaltTheRoundForOtherOutcomes(RestoreOutcome outcome) {
        when(definitions.findById(10L)).thenReturn(Optional.of(
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2)));
        when(gateProvider.getIfAvailable()).thenReturn(gate);
        when(gate.restore(10L, 1L)).thenReturn(outcome);
        TransactionSynchronizationManager.initSynchronization();

        service.restoreAfterCommit(10L, 1L);
        for (TransactionSynchronization synchronization
                : TransactionSynchronizationManager.getSynchronizations()) {
            synchronization.afterCommit();
        }

        assertThat(service.isRestorationHalted(10L)).isFalse();
    }

    /** Redis 호출 자체가 터진 것은 과소 방향이라 회차를 멈추지 않는다. */
    @Test
    void doesNotHaltTheRoundWhenRestoreThrows() {
        when(definitions.findById(10L)).thenReturn(Optional.of(
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2)));
        when(gateProvider.getIfAvailable()).thenReturn(gate);
        when(gate.restore(10L, 1L)).thenThrow(new IllegalStateException("redis down"));
        TransactionSynchronizationManager.initSynchronization();

        service.restoreAfterCommit(10L, 1L);
        for (TransactionSynchronization synchronization
                : TransactionSynchronizationManager.getSynchronizations()) {
            synchronization.afterCommit();
        }

        assertThat(service.isRestorationHalted(10L)).isFalse();
    }

    /** 재구성이 표식을 지우면 그 회차 만료가 다시 돈다 — 서버를 껐다 켜지 않는다. */
    @Test
    void clearingTheHaltResumesTheRound() {
        haltStore.halt(10L);
        assertThat(service.isRestorationHalted(10L)).isTrue();

        haltStore.clear(10L);

        assertThat(service.isRestorationHalted(10L)).isFalse();
    }

    /**
     * 표식을 못 읽는 것은 Redis 가 끊긴 상황이다. 그때 복원도 어차피 실패하고 그건 재고가
     * 안 돌아오는 과소 방향이다 — 읽기 실패를 "멈춤" 으로 읽으면 순단 한 번이 전 회차의
     * 만료를 세운다.
     */
    @Test
    void doesNotHaltWhenTheHaltMarkCannotBeRead() {
        RestorationHaltStore broken = mock(RestorationHaltStore.class);
        when(broken.isHalted(10L)).thenThrow(new IllegalStateException("redis down"));
        when(haltStoreProvider.getIfAvailable()).thenReturn(broken);

        assertThat(service.isRestorationHalted(10L)).isFalse();
    }

    /** 결과가 카운터로 남는다 — 로그만 있으면 경보를 걸 자리가 없다(06). */
    @Test
    void countsEveryRestoreOutcome() {
        when(definitions.findById(10L)).thenReturn(Optional.of(
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2)));
        when(gateProvider.getIfAvailable()).thenReturn(gate);
        when(gate.restore(10L, 1L))
                .thenReturn(RestoreOutcome.RESTORED)
                .thenReturn(RestoreOutcome.GATE_NOT_READY)
                .thenThrow(new IllegalStateException("redis down"));

        for (int attempt = 0; attempt < 3; attempt++) {
            TransactionSynchronizationManager.initSynchronization();
            service.restoreAfterCommit(10L, 1L);
            for (TransactionSynchronization synchronization
                    : TransactionSynchronizationManager.getSynchronizations()) {
                synchronization.afterCommit();
            }
            TransactionSynchronizationManager.clearSynchronization();
        }

        assertThat(meters.outcomes).containsExactly(
                RestoreOutcome.RESTORED, RestoreOutcome.GATE_NOT_READY);
        assertThat(meters.callFailures)
                .as("호출 실패는 거절과 다른 원인이라 따로 센다")
                .isEqualTo(1);
    }

    /**
     * ① 표식 쓰기가 실패해도 <b>이번 실행은 멈춰야 한다.</b> 한 {@code try} 로 뭉치면
     * "복원 호출 실패" 로그만 남고 표식이 안 서서, 남은 청크가 전부 돈다 — 이 장치의 목적이
     * 통째로 무효화된다. {@code OVER_CAP} 과 호출 실패가 같이 올라가 경보 판독도 어긋난다.
     */
    @Test
    void fallsBackToProcessLocalHaltWhenTheMarkCannotBeWritten() {
        RestorationHaltStore broken = mock(RestorationHaltStore.class);
        org.mockito.Mockito.doThrow(new IllegalStateException("redis down"))
                .when(broken).halt(10L);
        when(broken.isHalted(10L)).thenReturn(false);
        when(haltStoreProvider.getIfAvailable()).thenReturn(broken);
        when(definitions.findById(10L)).thenReturn(Optional.of(
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2)));
        when(gateProvider.getIfAvailable()).thenReturn(gate);
        when(gate.restore(10L, 1L)).thenReturn(RestoreOutcome.OVER_CAP);
        TransactionSynchronizationManager.initSynchronization();

        service.restoreAfterCommit(10L, 1L);
        for (TransactionSynchronization synchronization
                : TransactionSynchronizationManager.getSynchronizations()) {
            synchronization.afterCommit();
        }

        assertThat(service.isRestorationHalted(10L))
                .as("표식을 못 남겼어도 이 프로세스에서는 멈춘다")
                .isTrue();
        // 한 번의 타임아웃이 곧 방어 상실이면 안 된다 — 즉시 한 번 더 시도한다.
        verify(broken, org.mockito.Mockito.times(2)).halt(10L);
        assertThat(meters.haltWriteFailures)
                .as("취소 경로에는 폴백을 읽는 쪽이 없다 — 이 카운터가 유일한 신호다")
                .isEqualTo(1);
        assertThat(meters.outcomes).containsExactly(RestoreOutcome.OVER_CAP);
        assertThat(meters.callFailures)
                .as("스크립트는 답을 냈다 — 호출 실패로 세면 경보 판독이 뒤바뀐다")
                .isZero();
    }

    /** 폴백은 이번 실행뿐이다. 다른 회차까지 멈추면 안 된다. */
    @Test
    void theProcessLocalFallbackIsPerRound() {
        RestorationHaltStore broken = mock(RestorationHaltStore.class);
        org.mockito.Mockito.doThrow(new IllegalStateException("redis down"))
                .when(broken).halt(10L);
        when(broken.isHalted(org.mockito.ArgumentMatchers.anyLong())).thenReturn(false);
        when(haltStoreProvider.getIfAvailable()).thenReturn(broken);
        when(definitions.findById(10L)).thenReturn(Optional.of(
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2)));
        when(gateProvider.getIfAvailable()).thenReturn(gate);
        when(gate.restore(10L, 1L)).thenReturn(RestoreOutcome.OVER_CAP);
        TransactionSynchronizationManager.initSynchronization();

        service.restoreAfterCommit(10L, 1L);
        for (TransactionSynchronization synchronization
                : TransactionSynchronizationManager.getSynchronizations()) {
            synchronization.afterCommit();
        }

        assertThat(service.isRestorationHalted(11L)).isFalse();
    }

    /**
     * ⑥-a 계측 조회가 던져도 원 실패 로그는 남아야 한다. {@code catch} 안에서 미터를
     * 해석하면 그 예외가 원 실패를 덮어 <b>무엇이 왜 실패했는지가 사라진다.</b>
     */
    @Test
    void aBrokenMeterProviderDoesNotSwallowTheRestoreFailure() {
        when(metersProvider.getIfAvailable()).thenThrow(new IllegalStateException("no registry"));
        when(definitions.findById(10L)).thenReturn(Optional.of(
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2)));
        when(gateProvider.getIfAvailable()).thenReturn(gate);
        when(gate.restore(10L, 1L)).thenThrow(new IllegalStateException("redis down"));
        TransactionSynchronizationManager.initSynchronization();

        service.restoreAfterCommit(10L, 1L);

        assertThatCode(() -> {
            for (TransactionSynchronization synchronization
                    : TransactionSynchronizationManager.getSynchronizations()) {
                synchronization.afterCommit();
            }
        }).doesNotThrowAnyException();
    }

    /**
     * <b>폴백이 재구성을 되돌리면 안 된다.</b> 표식을 못 쓴 회차를 다음 조회에서 다시 쓰면,
     * 그 사이 재구성이 지운 표식이 되살아나 <b>재고는 정상인데 만료만 영구 정지</b>가 된다 —
     * 아무 절차로도 못 푸는 상태이고, 이 장치가 막으려던 사고보다 나쁘다.
     *
     * <p>폴백은 <b>이번 실행 동안만</b> 산다. 다음 실행은 저장소의 현재 답을 따른다.
     */
    @Test
    void theProcessLocalFallbackNeverWritesTheMarkBack() {
        RestorationHaltStore flaky = mock(RestorationHaltStore.class);
        org.mockito.Mockito.doThrow(new IllegalStateException("redis down"))
                .when(flaky).halt(10L);
        when(flaky.isHalted(10L)).thenReturn(false);
        when(haltStoreProvider.getIfAvailable()).thenReturn(flaky);
        haltOnce();

        assertThat(service.isRestorationHalted(10L))
                .as("이번 실행에서는 멈춘다")
                .isTrue();
        assertThat(service.isRestorationHalted(10L)).isTrue();

        // 표식 쓰기는 halt 시점의 재시도 두 번뿐이다 — 조회가 다시 쓰지 않는다.
        verify(flaky, org.mockito.Mockito.times(2)).halt(10L);
    }

    /**
     * 실행이 바뀌면 폴백은 사라지고 저장소의 현재 답을 따른다 — 재구성이 그 사이에 표식을
     * 풀었다면 그대로 재개된다. {@code -2} 는 이벤트가 아니라 상태라, 재고가 여전히 틀어져
     * 있으면 다음 실행이 곧바로 다시 발견한다.
     */
    @Test
    void theFallbackLastsOnlyForOneRun() {
        RestorationHaltStore flaky = mock(RestorationHaltStore.class);
        org.mockito.Mockito.doThrow(new IllegalStateException("redis down"))
                .when(flaky).halt(10L);
        when(flaky.isHalted(10L)).thenReturn(false);
        when(haltStoreProvider.getIfAvailable()).thenReturn(flaky);
        haltOnce();
        assertThat(service.isRestorationHalted(10L)).isTrue();

        service.beginExpirationRun();

        assertThat(service.isRestorationHalted(10L))
                .as("다음 실행은 저장소가 답한 대로 간다")
                .isFalse();
    }

    /** 새 실행이 시작돼도 <b>저장소에 실제로 선 표식</b>은 그대로여야 한다. */
    @Test
    void aRealMarkSurvivesTheNextRun() {
        haltStore.halt(10L);

        service.beginExpirationRun();

        assertThat(service.isRestorationHalted(10L)).isTrue();
    }

    /** 표식 저장소 해석이 던져도 만료 배치 전체가 죽지 않는다 — 그 틱의 모든 회차가 멈춘다. */
    @Test
    void aBrokenHaltStoreProviderDoesNotKillTheWholeRun() {
        when(haltStoreProvider.getIfAvailable()).thenThrow(new IllegalStateException("no bean"));

        assertThat(service.isRestorationHalted(10L)).isFalse();
    }

    /**
     * 표식을 <b>성공적으로 남긴</b> 회차는 이 프로세스도 기억한다. 안 그러면 Redis 순단
     * 한 번에 {@code isHalted} 가 던지고 {@code false} 로 떨어져, {@code -2} 를 이미 본
     * 회차인데 남은 청크가 전부 돈다 — 어긋남이 한 묶음에서 회차 전체로 커진다.
     * 방어가 쓰기 실패에만 있고 읽기 실패에는 없는 비대칭을 없앤다.
     */
    @Test
    void remembersASuccessfullyMarkedRoundSoAReadFailureDoesNotResumeExpiry() {
        RestorationHaltStore flaky = mock(RestorationHaltStore.class);
        when(haltStoreProvider.getIfAvailable()).thenReturn(flaky);
        haltOnce();
        verify(flaky).halt(10L);

        when(flaky.isHalted(10L)).thenThrow(new IllegalStateException("redis down"));

        assertThat(service.isRestorationHalted(10L))
                .as("표식을 남긴 사실을 이 실행 동안은 이 프로세스도 안다")
                .isTrue();
    }

    /**
     * 계측 <b>기록</b>이 던져도 복원 결과 처리가 끊기면 안 된다. {@code restore} 는
     * {@code afterCommit} 안에서 도는데 Spring 은 그 예외를 커밋 밖으로 전파한다 —
     * 배치에서는 틱이 죽고, 취소 경로에서는 이미 커밋된 취소가 500 이 된다.
     * 해석에만 걸어 둔 가드를 기록에도 건다.
     */
    @Test
    void aThrowingMeterDoesNotEscapeAfterCommit() {
        V2RestorationMeters exploding = new V2RestorationMeters() {
            @Override
            public void recordOutcome(RestoreOutcome outcome) {
                throw new IllegalStateException("registry down");
            }

            @Override
            public void recordCallFailure() {
                throw new IllegalStateException("registry down");
            }

            @Override
            public void recordHaltWriteFailure() {
                throw new IllegalStateException("registry down");
            }
        };
        when(metersProvider.getIfAvailable()).thenReturn(exploding);
        when(definitions.findById(10L)).thenReturn(Optional.of(
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2)));
        when(gateProvider.getIfAvailable()).thenReturn(gate);
        when(gate.restore(10L, 1L)).thenReturn(RestoreOutcome.OVER_CAP);
        TransactionSynchronizationManager.initSynchronization();

        service.restoreAfterCommit(10L, 1L);

        assertThatCode(() -> {
            for (TransactionSynchronization synchronization
                    : TransactionSynchronizationManager.getSynchronizations()) {
                synchronization.afterCommit();
            }
        }).doesNotThrowAnyException();
        assertThat(haltStore.isHalted(10L))
                .as("미터가 터져도 표식은 선다 — 기록보다 중단이 앞선다")
                .isTrue();
    }

    private void haltOnce() {
        when(definitions.findById(10L)).thenReturn(Optional.of(
                new CouponRoundIssuanceDefinition(10L, 7, EngineVersion.V2)));
        when(gateProvider.getIfAvailable()).thenReturn(gate);
        when(gate.restore(10L, 1L)).thenReturn(RestoreOutcome.OVER_CAP);
        TransactionSynchronizationManager.initSynchronization();
        try {
            service.restoreAfterCommit(10L, 1L);
            for (TransactionSynchronization synchronization
                    : TransactionSynchronizationManager.getSynchronizations()) {
                synchronization.afterCommit();
            }
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }
}
