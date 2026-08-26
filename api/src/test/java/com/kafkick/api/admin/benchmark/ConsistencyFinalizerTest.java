package com.kafkick.api.admin.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.EnumMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.kafkick.core.benchmark.BenchmarkRun;
import com.kafkick.core.benchmark.BenchmarkRunService;
import com.kafkick.core.consistency.ConsistencyEvaluation;
import com.kafkick.core.consistency.ConsistencyFinalStore;
import com.kafkick.core.consistency.ConsistencyGapType;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.consistency.GapValue;
import com.kafkick.core.consistency.Verdict;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.support.TimeProvider;

class ConsistencyFinalizerTest {
    private final BenchmarkRunService runs = mock(BenchmarkRunService.class);
    private final ConsistencyFinalStore store = mock(ConsistencyFinalStore.class);
    private final BatchConsistencyFinalClient batch = mock(BatchConsistencyFinalClient.class);
    private static final Instant FINALIZED_AT = Instant.parse("2026-08-26T00:00:00Z");
    private static final Instant NOW = FINALIZED_AT.plusSeconds(3);
    private final ConsistencyFinalizer finalizer = new ConsistencyFinalizer(runs, store, batch,
            new TimeProvider(Clock.fixed(NOW, ZoneOffset.UTC)),
            Duration.ofMinutes(5), Duration.ofMinutes(15));

    @Test
    void batchFailureIsFencedAndRecordedWithoutChangingRunLifecycle() {
        BenchmarkRun run = run();
        when(runs.get(7L)).thenReturn(run);
        when(store.claim(7L, Duration.ofMinutes(5))).thenReturn(Optional.of("token"));
        when(batch.evaluate(11L, EngineVersion.V3, FINALIZED_AT))
                .thenThrow(new IllegalStateException("batch down"));
        when(store.fail(7L, "token", "batch down")).thenReturn(true);

        assertThatThrownBy(() -> finalizer.calculate(7L))
                .hasMessageContaining("batch down");
        verify(store).fail(7L, "token", "batch down");
        verify(runs).get(7L);
    }

    @Test
    void finalizedAtIsStoredAsEvaluatedAtSoRetryDoesNotMoveIt() {
        BenchmarkRun run = run();
        ConsistencyEvaluation evaluation = evaluation(Instant.parse("2026-08-26T00:29:00Z"));
        AtomicReference<Instant> now = new AtomicReference<>(NOW);
        ConsistencyFinalizer twice = new ConsistencyFinalizer(runs, store, batch,
                new TimeProvider(movingClock(now)), Duration.ofMinutes(5), Duration.ofHours(2));
        when(runs.get(7L)).thenReturn(run);
        // 재실행은 claim 을 다시 받는다 — 실제 claim 은 매번 새 UUID 를 박는다.
        when(store.claim(7L, Duration.ofMinutes(5)))
                .thenReturn(Optional.of("token-1"), Optional.of("token-2"));
        when(batch.evaluate(11L, EngineVersion.V3, FINALIZED_AT)).thenReturn(evaluation);
        when(store.complete(eq(7L), any(), eq(11L), eq(EngineVersion.V3), any(),
                eq(evaluation))).thenReturn(true);

        twice.calculate(7L);
        now.set(FINALIZED_AT.plusSeconds(600));
        twice.calculate(7L);

        // 10분 뒤 재실행해도 저장되는 시각은 그대로여야 캠페인별 최신 선택이 흔들리지 않는다.
        ArgumentCaptor<Instant> stored = ArgumentCaptor.forClass(Instant.class);
        ArgumentCaptor<String> tokens = ArgumentCaptor.forClass(String.class);
        verify(store, times(2)).complete(eq(7L), tokens.capture(), eq(11L), eq(EngineVersion.V3),
                stored.capture(), eq(evaluation));
        assertThat(stored.getAllValues()).containsExactly(FINALIZED_AT, FINALIZED_AT);
        // 두 번째 실행이 옛 token 으로 쓰면 운영에서는 소유권을 잃고 FAILED 로 떨어진다.
        assertThat(tokens.getAllValues()).containsExactly("token-1", "token-2");
    }

    @Test
    void longFailureReasonIsTruncatedAtColumnLimitWithoutSplittingSurrogatePair() {
        BenchmarkRun run = run();
        String reason = "a".repeat(ConsistencyFinalStore.FAILURE_REASON_MAX - 1) + "\uD83D\uDCA5x";
        when(runs.get(7L)).thenReturn(run);
        when(store.claim(7L, Duration.ofMinutes(5))).thenReturn(Optional.of("token"));
        when(batch.evaluate(11L, EngineVersion.V3, FINALIZED_AT)).thenThrow(new IllegalStateException(reason));

        assertThatThrownBy(() -> finalizer.calculate(7L)).isInstanceOf(IllegalStateException.class);

        ArgumentCaptor<String> stored = ArgumentCaptor.forClass(String.class);
        verify(store).fail(eq(7L), eq("token"), stored.capture());
        assertThat(stored.getValue())
                .hasSize(ConsistencyFinalStore.FAILURE_REASON_MAX - 1)
                .doesNotContain("\uD83D");
    }

    @Test
    void staleRetryIsExpiredAndKeepsTheOriginalFailureReason() {
        BenchmarkRun run = run();
        when(run.consistencyFailureReason()).thenReturn("batch down");
        ConsistencyFinalizer stale = new ConsistencyFinalizer(runs, store, batch,
                new TimeProvider(Clock.fixed(FINALIZED_AT.plusSeconds(1800), ZoneOffset.UTC)),
                Duration.ofMinutes(5), Duration.ofMinutes(15));
        when(runs.get(7L)).thenReturn(run);
        when(store.claim(7L, Duration.ofMinutes(5))).thenReturn(Optional.of("token"));

        // 확정 00:00 + 15분 창을 00:30 이 넘겼다. 회차와 무관한 시점의 값이다.
        assertThatThrownBy(() -> stale.calculate(7L)).hasMessageContaining("finalize window");
        verifyNoInteractions(batch);

        // FAILED 로 남기면 다시 claim 되어 재실행이 원래 원인을 덮어쓴다.
        ArgumentCaptor<String> reason = ArgumentCaptor.forClass(String.class);
        verify(store).expire(eq(7L), eq("token"), reason.capture());
        verify(store, never()).fail(anyLong(), any(), any());
        assertThat(reason.getValue())
                .contains("finalize window expired")
                .contains("previousReason=batch down");
    }

    private static Clock movingClock(AtomicReference<Instant> now) {
        return new Clock() {
            @Override public java.time.ZoneId getZone() { return ZoneOffset.UTC; }
            @Override public Clock withZone(java.time.ZoneId zone) { return this; }
            @Override public Instant instant() { return now.get(); }
        };
    }

    private static ConsistencyEvaluation evaluation(Instant observedAt) {
        var gaps = new EnumMap<ConsistencyGapType, GapValue>(ConsistencyGapType.class);
        for (ConsistencyGapType type : ConsistencyGapType.values()) {
            gaps.put(type, new GapValue(0L, SourceStatus.VALID, observedAt));
        }
        return new ConsistencyEvaluation(gaps, new GapValue(0L, SourceStatus.VALID, observedAt),
                ConsistencyPhase.FINAL, Verdict.PASS, Severity.NONE);
    }

    private static BenchmarkRun run() {
        BenchmarkRun run = mock(BenchmarkRun.class);
        when(run.couponId()).thenReturn(11L);
        when(run.engineVersion()).thenReturn(EngineVersion.V3);
        when(run.finalizedAt()).thenReturn(FINALIZED_AT);
        return run;
    }
}
