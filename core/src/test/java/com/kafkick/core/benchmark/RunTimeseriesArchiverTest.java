package com.kafkick.core.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.eq;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.mockito.ArgumentCaptor;

import com.kafkick.core.benchmark.RunTimeseriesArchiver.Metric;
import com.kafkick.core.benchmark.RunTimeseriesArchiver.Sample;
import com.kafkick.core.benchmark.RunTimeseriesArchiver.State;
import com.kafkick.core.support.exception.BusinessException;

@ExtendWith(OutputCaptureExtension.class)
class RunTimeseriesArchiverTest {

    private static final Instant START = Instant.parse("2026-08-23T00:00:00Z");
    private static final Instant OBSERVATION_STOP = Instant.parse("2026-08-23T00:01:05Z");
    private static final java.time.Duration CLAIM_LEASE = java.time.Duration.ofMinutes(5);
    private static final String CLAIM_TOKEN = "00000000-0000-4000-8000-000000000001";
    private static final int MAX_SAMPLES = 10_000;

    @Test
    void usesObservationStopAndDoesNotInventMissingZeroes() {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        BenchmarkRun run = run(BenchmarkArchiveStatus.NONE);
        when(runs.findById(7)).thenReturn(Optional.of(run));
        CapturingSource source = new CapturingSource();
        RecordingStore store = new RecordingStore();

        when(runs.claimArchive(eq(7L), eq(CLAIM_LEASE))).thenReturn(Optional.of(CLAIM_TOKEN));
        new RunTimeseriesArchiver(runs, source, store, CLAIM_LEASE, MAX_SAMPLES).archive(7);

        assertThat(source.starts).containsOnly(START);
        assertThat(source.ends).containsOnly(OBSERVATION_STOP);
        assertThat(source.steps).containsOnly(1);
        // 빈 range 를 0 으로 메우지 않는다. 대신 <이유> 한 줄만 남긴다 — 지연 성공 축이
        // 빈 회차라 나머지 세 종이 두 점씩(6행) + 빈 축의 표시 한 줄이다.
        assertThat(store.inserted).hasSize(7);
        assertThat(store.inserted).filteredOn(sample -> sample.metric() == Metric.LATENCY_P99)
            .singleElement()
            .satisfies(marker -> {
                assertThat(marker.value()).as("메우지 않는다 — 0 이 아니라 값 없음이다").isNull();
                assertThat(marker.state()).as("미터는 있었으니 '그런 일이 없었다' 다").isEqualTo(State.N_A);
            });
        assertThat(store.inserted.get(0).value()).isEqualTo(0d);
    }

    @Test
    void passesConfiguredWriteChunkSizeToTheStore() {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        BenchmarkRun benchmarkRun = run(BenchmarkArchiveStatus.NONE);
        when(runs.findById(7)).thenReturn(Optional.of(benchmarkRun));
        when(runs.claimArchive(eq(7L), eq(CLAIM_LEASE))).thenReturn(Optional.of(CLAIM_TOKEN));
        RecordingStore store = new RecordingStore();

        new RunTimeseriesArchiver(
            runs, new CapturingSource(), store, CLAIM_LEASE, MAX_SAMPLES, 37).archive(7);

        assertThat(store.chunkSize).isEqualTo(37);
    }

    @Test
    void completedArchiveCannotBeReplacedByASecondInvocation() {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        BenchmarkRun run = run(BenchmarkArchiveStatus.NONE);
        when(runs.findById(7)).thenReturn(Optional.of(run));
        RecordingStore store = new RecordingStore();
        when(runs.claimArchive(eq(7L), eq(CLAIM_LEASE)))
            .thenReturn(Optional.of(CLAIM_TOKEN), Optional.empty());
        RunTimeseriesArchiver archiver = new RunTimeseriesArchiver(
            runs, new CapturingSource(), store, CLAIM_LEASE, MAX_SAMPLES);

        archiver.archive(7);
        assertThatThrownBy(() -> archiver.archive(7)).isInstanceOf(BusinessException.class);

        assertThat(store.inserted).hasSize(7);
    }

    /**
     * 시스템 실패가 한 건도 없는 회차가 정상이다. 비었다고 회차 archive 를 실패시키면
     * 장애 없이 끝난 회차가 하나도 안 남는다 — 지연 축은 둘 다 빌 수 있다.
     *
     * <p><b>0 행으로 두지 않고 이유를 한 줄 남긴다.</b> 행이 없으면 "그런 일이 없었다" 와
     * "아예 못 쟀다" 가 같은 모양이고, archive 는 불변이라 나중에 못 가른다.</p>
     */
    @Test
    void emptyLatencyAxesAreRecordedAsNotApplicableWhenTheMeterWasThere() {
        RecordingStore store = archiveWithEmptyLatency(true);

        assertThat(store.inserted).hasSize(4);
        assertThat(latencyMarkers(store)).allSatisfy(marker -> {
            assertThat(marker.value()).isNull();
            assertThat(marker.state())
                .as("미터가 있었으면 축이 빈 것은 사실이다 — 해당 없음이다")
                .isEqualTo(State.N_A);
        });
    }

    /**
     * 같은 "0 행" 이라도 미터 자체가 없었으면 뜻이 반대다 — 못 쟀다는 뜻이라
     * 그 회차를 "깨끗했다" 로 읽으면 안 된다.
     */
    @Test
    void emptyLatencyAxesAreRecordedAsUnavailableWhenTheMeterWasMissing() {
        RecordingStore store = archiveWithEmptyLatency(false);

        assertThat(store.inserted).hasSize(4);
        assertThat(latencyMarkers(store)).allSatisfy(marker -> {
            assertThat(marker.value()).isNull();
            assertThat(marker.state())
                .as("미터가 없었으면 재지 못한 것이다 — 0 건과 반대 뜻이다")
                .isEqualTo(State.UNAVAILABLE);
        });
    }

    /** 원천 존재 질의에 답할 수 없으면 추측하지 않고 회차 archive 를 실패시킨다. */
    @Test
    void unanswerableSourceProbeFailsTheRunInsteadOfGuessing() {
        assertThatThrownBy(() -> archiveWithEmptyLatency(null))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("원천 존재를 물을 수 없다");
    }

    private static List<Sample> latencyMarkers(RecordingStore store) {
        return store.inserted.stream()
            .filter(sample -> sample.metric().allowsEmptySamples())
            .toList();
    }

    /**
     * 지연 두 축이 모두 빈 회차를 archive 한다.
     *
     * @param sourcePresent 원천 존재 질의의 답. null 이면 물어볼 수 없는 상태다
     */
    private static RecordingStore archiveWithEmptyLatency(Boolean sourcePresent) {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        BenchmarkRun benchmarkRun = run(BenchmarkArchiveStatus.NONE);
        when(runs.findById(7)).thenReturn(Optional.of(benchmarkRun));
        when(runs.claimArchive(eq(7L), eq(CLAIM_LEASE))).thenReturn(Optional.of(CLAIM_TOKEN));
        RunTimeseriesArchiver.RangeSource source = new RunTimeseriesArchiver.RangeSource() {
            @Override
            public List<Sample> queryRange(Metric metric, Instant start, Instant end, int step) {
                return metric.allowsEmptySamples()
                    ? List.of()
                    : List.of(new Sample(metric, 0, start, 0d, State.VALID, null));
            }

            @Override
            public boolean sourceExists(Metric metric, Instant start, Instant end) {
                if (sourcePresent == null) {
                    throw new IllegalStateException("원천 존재를 물을 수 없다");
                }
                return sourcePresent;
            }
        };
        RecordingStore store = new RecordingStore();
        new RunTimeseriesArchiver(runs, source, store, CLAIM_LEASE, MAX_SAMPLES).archive(7);
        return store;
    }

    @Test
    void missingRequiredSeriesFailsBeforeReplacingExistingArchive() {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        BenchmarkRun benchmarkRun = run(BenchmarkArchiveStatus.NONE);
        when(runs.findById(7)).thenReturn(Optional.of(benchmarkRun));
        RunTimeseriesArchiver.RangeSource source = rangeOnly(
            (metric, start, end, step) -> List.of());
        RunTimeseriesArchiver.ArchiveStore store = mock(RunTimeseriesArchiver.ArchiveStore.class);

        when(runs.claimArchive(eq(7L), eq(CLAIM_LEASE))).thenReturn(Optional.of(CLAIM_TOKEN));
        assertThatThrownBy(() -> new RunTimeseriesArchiver(
            runs, source, store, CLAIM_LEASE, MAX_SAMPLES).archive(7))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("STOCK_REMAINING");

        verify(store, never()).replaceForRun(
            eq(7L), eq(CLAIM_TOKEN), org.mockito.ArgumentMatchers.anyList(),
            eq(RunTimeseriesArchiver.DEFAULT_WRITE_CHUNK_SIZE));
        verify(runs).failArchive(eq(7L), eq(CLAIM_TOKEN),
                org.mockito.ArgumentMatchers.contains("STOCK_REMAINING"));
    }

    @Test
    void excessiveArchiveSamplesFailBeforeOpeningTheWriteTransaction() {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        BenchmarkRun benchmarkRun = run(BenchmarkArchiveStatus.NONE);
        when(runs.findById(7)).thenReturn(Optional.of(benchmarkRun));
        when(runs.claimArchive(eq(7L), eq(CLAIM_LEASE))).thenReturn(Optional.of(CLAIM_TOKEN));
        List<Sample> excessive = java.util.stream.IntStream
            .rangeClosed(0, MAX_SAMPLES)
            .mapToObj(sequence -> new Sample(
                Metric.STOCK_REMAINING, sequence, START, 1d, State.VALID, null))
            .toList();
        RunTimeseriesArchiver.RangeSource source = rangeOnly(
            (metric, start, end, step) -> excessive);
        RunTimeseriesArchiver.ArchiveStore store = mock(RunTimeseriesArchiver.ArchiveStore.class);

        assertThatThrownBy(() -> new RunTimeseriesArchiver(
            runs, source, store, CLAIM_LEASE, MAX_SAMPLES).archive(7))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("표본 상한");
        verifyNoInteractions(store);
    }

    @Test
    void failedRetryRemainsFailedWhenPrometheusFailsAgain() {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        BenchmarkRun failedRun = run(BenchmarkArchiveStatus.FAILED);
        when(runs.findById(7)).thenReturn(Optional.of(failedRun));
        when(runs.claimArchive(eq(7L), org.mockito.ArgumentMatchers.any()))
            .thenReturn(Optional.of(CLAIM_TOKEN));
        RunTimeseriesArchiver.RangeSource source = rangeOnly((metric, start, end, step) -> {
            if (metric == Metric.LATENCY_P99) throw new IllegalStateException("prometheus down");
            return List.of(
                new Sample(metric, 0, start, 0d, State.VALID, null),
                new Sample(metric, end.getEpochSecond() - start.getEpochSecond(), end, 0d, State.VALID, null));
        });
        RunTimeseriesArchiver archiver = new RunTimeseriesArchiver(
            runs, source, new RecordingStore(), CLAIM_LEASE, MAX_SAMPLES);

        assertThatThrownBy(() -> archiver.retry(7)).hasMessageContaining("prometheus down");
        ArgumentCaptor<String> reason = ArgumentCaptor.forClass(String.class);
        verify(runs).failArchive(eq(7L), eq(CLAIM_TOKEN), reason.capture());
        assertThat(reason.getValue()).contains("prometheus down");
    }

    @Test
    void failureReasonFitsTheDatabaseColumnAndPreservesTheOriginalFailure() {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        BenchmarkRun benchmarkRun = run(BenchmarkArchiveStatus.NONE);
        when(runs.findById(7)).thenReturn(Optional.of(benchmarkRun));
        when(runs.claimArchive(eq(7L), eq(CLAIM_LEASE))).thenReturn(Optional.of(CLAIM_TOKEN));
        IllegalStateException original = new IllegalStateException("x".repeat(500));
        RunTimeseriesArchiver.RangeSource source = rangeOnly(
            (metric, start, end, step) -> { throw original; });
        when(runs.failArchive(eq(7L), eq(CLAIM_TOKEN), org.mockito.ArgumentMatchers.anyString()))
            .thenThrow(new IllegalStateException("status write failed"));

        assertThatThrownBy(() -> new RunTimeseriesArchiver(
            runs, source, new RecordingStore(), CLAIM_LEASE, MAX_SAMPLES).archive(7))
            .isSameAs(original)
            .satisfies(failure -> assertThat(failure.getSuppressed()).hasSize(1));
        ArgumentCaptor<String> reason = ArgumentCaptor.forClass(String.class);
        verify(runs).failArchive(eq(7L), eq(CLAIM_TOKEN), reason.capture());
        assertThat(reason.getValue()).hasSize(200);
    }

    @Test
    void blankFailureMessageUsesTheExceptionType() {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        BenchmarkRun benchmarkRun = run(BenchmarkArchiveStatus.NONE);
        when(runs.findById(7)).thenReturn(Optional.of(benchmarkRun));
        when(runs.claimArchive(eq(7L), eq(CLAIM_LEASE))).thenReturn(Optional.of(CLAIM_TOKEN));
        RunTimeseriesArchiver.RangeSource source = rangeOnly(
            (metric, start, end, step) -> { throw new IllegalStateException("   "); });

        assertThatThrownBy(() -> new RunTimeseriesArchiver(
            runs, source, new RecordingStore(), CLAIM_LEASE, MAX_SAMPLES).archive(7))
            .isInstanceOf(IllegalStateException.class);
        verify(runs).failArchive(7L, CLAIM_TOKEN, "IllegalStateException");
    }

    @Test
    void lostFailureClaimIsLoggedWithoutTheFullFencingToken(CapturedOutput output) {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        BenchmarkRun benchmarkRun = run(BenchmarkArchiveStatus.NONE);
        when(runs.findById(7)).thenReturn(Optional.of(benchmarkRun));
        when(runs.claimArchive(eq(7L), eq(CLAIM_LEASE))).thenReturn(Optional.of(CLAIM_TOKEN));
        when(runs.failArchive(eq(7L), eq(CLAIM_TOKEN), org.mockito.ArgumentMatchers.anyString()))
            .thenReturn(false);
        RunTimeseriesArchiver.RangeSource source = rangeOnly(
            (metric, start, end, step) -> { throw new IllegalStateException("prometheus down"); });

        assertThatThrownBy(() -> new RunTimeseriesArchiver(
            runs, source, new RecordingStore(), CLAIM_LEASE, MAX_SAMPLES).archive(7))
            .isInstanceOf(IllegalStateException.class);

        assertThat(output).contains("archive failure status was not recorded")
            .contains(CLAIM_TOKEN.substring(0, 8))
            .doesNotContain(CLAIM_TOKEN);
    }

    @Test
    void failedRunCanBeRetriedToDone() {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        BenchmarkRun failedRun = run(BenchmarkArchiveStatus.FAILED);
        when(runs.findById(7)).thenReturn(Optional.of(failedRun));
        when(runs.claimArchive(eq(7L), org.mockito.ArgumentMatchers.any()))
            .thenReturn(Optional.of(CLAIM_TOKEN));

        RecordingStore store = new RecordingStore();
        new RunTimeseriesArchiver(
            runs, new CapturingSource(), store, CLAIM_LEASE, MAX_SAMPLES).retry(7);

        assertThat(store.claimToken).isEqualTo(CLAIM_TOKEN);
        assertThat(store.inserted).hasSize(7);
    }

    @Test
    void concurrentRetryCannotArchiveWithoutOwningFailedStatus() {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        BenchmarkRun failedRun = run(BenchmarkArchiveStatus.FAILED);
        when(runs.findById(7)).thenReturn(Optional.of(failedRun));
        when(runs.claimArchive(eq(7L), org.mockito.ArgumentMatchers.any())).thenReturn(Optional.empty());
        RunTimeseriesArchiver.RangeSource source = mock(RunTimeseriesArchiver.RangeSource.class);

        assertThatThrownBy(() -> new RunTimeseriesArchiver(
                runs, source, new RecordingStore(), CLAIM_LEASE, MAX_SAMPLES).retry(7))
                .isInstanceOfSatisfying(BusinessException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(BenchmarkErrorCode.ILLEGAL_TRANSITION));

        verifyNoInteractions(source);
    }

    @Test
    void rejectsArchiveBeforeObservationStops() {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        BenchmarkRun running = mock(BenchmarkRun.class);
        when(running.observationStoppedAt()).thenReturn(null);
        when(runs.findById(7)).thenReturn(Optional.of(running));

        assertThatThrownBy(() -> new RunTimeseriesArchiver(
                runs, new CapturingSource(), new RecordingStore(), CLAIM_LEASE, MAX_SAMPLES).archive(7))
                .isInstanceOfSatisfying(BusinessException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(BenchmarkErrorCode.ILLEGAL_TRANSITION));
    }

    @Test
    void reportsMissingRunAsNotFound() {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        when(runs.findById(404)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> new RunTimeseriesArchiver(
                runs, new CapturingSource(), new RecordingStore(), CLAIM_LEASE, MAX_SAMPLES).archive(404))
                .isInstanceOfSatisfying(BusinessException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(BenchmarkErrorCode.RUN_NOT_FOUND));
    }

    @Test
    void doneRunCannotBeRearchived() {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        BenchmarkRun done = run(BenchmarkArchiveStatus.DONE);
        when(runs.findById(7)).thenReturn(Optional.of(done));
        RunTimeseriesArchiver.RangeSource source = mock(RunTimeseriesArchiver.RangeSource.class);

        assertThatThrownBy(() -> new RunTimeseriesArchiver(
                runs, source, new RecordingStore(), CLAIM_LEASE, MAX_SAMPLES).retry(7))
                .isInstanceOfSatisfying(BusinessException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(BenchmarkErrorCode.ILLEGAL_TRANSITION));

        verifyNoInteractions(source);
        verify(runs, never()).claimArchive(eq(7L), org.mockito.ArgumentMatchers.any());
    }

    @Test
    void noneRunCanBeRetriedWhenTheInitialClaimFailed() {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        BenchmarkRun none = run(BenchmarkArchiveStatus.NONE);
        when(runs.findById(7)).thenReturn(Optional.of(none));
        when(runs.claimArchive(eq(7L), eq(CLAIM_LEASE))).thenReturn(Optional.of(CLAIM_TOKEN));

        RecordingStore store = new RecordingStore();
        new RunTimeseriesArchiver(
                runs, new CapturingSource(), store, CLAIM_LEASE, MAX_SAMPLES).retry(7);

        assertThat(store.claimToken).isEqualTo(CLAIM_TOKEN);
        assertThat(store.inserted).hasSize(7);
    }

    @Test
    void constructorRejectsInvalidLeaseAndSampleLimit() {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        RunTimeseriesArchiver.RangeSource source = mock(RunTimeseriesArchiver.RangeSource.class);
        RunTimeseriesArchiver.ArchiveStore store = mock(RunTimeseriesArchiver.ArchiveStore.class);

        assertThatThrownBy(() -> new RunTimeseriesArchiver(
            runs, source, store, java.time.Duration.ZERO, MAX_SAMPLES))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("1초");
        assertThatThrownBy(() -> new RunTimeseriesArchiver(
            runs, source, store, java.time.Duration.ofSeconds(1).plusMillis(500), MAX_SAMPLES))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("정수 초");
        assertThatThrownBy(() -> new RunTimeseriesArchiver(
            runs, source, store, CLAIM_LEASE, 0))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("양수");
        assertThatThrownBy(() -> new RunTimeseriesArchiver(
            runs, source, store, java.time.Duration.ofDays(366), MAX_SAMPLES))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("상한");
    }

    @Test
    void retryProceedsWhenArchiveIsStillInProgress() {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        BenchmarkRun inProgress = run(BenchmarkArchiveStatus.IN_PROGRESS);
        when(runs.findById(7)).thenReturn(Optional.of(inProgress));
        when(runs.claimArchive(eq(7L), org.mockito.ArgumentMatchers.any()))
            .thenReturn(Optional.of(CLAIM_TOKEN));

        RecordingStore store = new RecordingStore();
        new RunTimeseriesArchiver(
            runs, new CapturingSource(), store, CLAIM_LEASE, MAX_SAMPLES).retry(7);

        assertThat(store.claimToken).isEqualTo(CLAIM_TOKEN);
        assertThat(store.inserted).hasSize(7);
    }

    @Test
    void rejectsMetricMismatch() {
        assertInvalidSample(new Sample(Metric.DB_POOL_USAGE, 0, START, 1d, State.VALID, null));
    }

    @Test
    void rejectsReversedSequence() {
        assertInvalidSamples(List.of(
                new Sample(Metric.STOCK_REMAINING, 1, START, 1d, State.VALID, null),
                new Sample(Metric.STOCK_REMAINING, 0, START.plusSeconds(1), 1d, State.VALID, null)));
    }

    @Test
    void rejectsSampleOutsideRunRange() {
        assertInvalidSample(new Sample(
                Metric.STOCK_REMAINING, 0, START.minusSeconds(1), 1d, State.VALID, null));
    }

    private static void assertInvalidSample(Sample sample) {
        assertInvalidSamples(List.of(sample));
    }

    private static void assertInvalidSamples(List<Sample> invalid) {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        BenchmarkRun benchmarkRun = run(BenchmarkArchiveStatus.NONE);
        when(runs.findById(7)).thenReturn(Optional.of(benchmarkRun));
        RunTimeseriesArchiver.RangeSource source = rangeOnly(
            (metric, start, end, step) ->
                metric == Metric.STOCK_REMAINING ? invalid : List.of());

        when(runs.claimArchive(eq(7L), eq(CLAIM_LEASE))).thenReturn(Optional.of(CLAIM_TOKEN));
        assertThatThrownBy(() -> new RunTimeseriesArchiver(
            runs, source, new RecordingStore(), CLAIM_LEASE, MAX_SAMPLES).archive(7))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("회차 구간/순서");
    }

    private static BenchmarkRun run(BenchmarkArchiveStatus status) {
        BenchmarkRun run = mock(BenchmarkRun.class);
        when(run.startedAt()).thenReturn(START);
        when(run.loadStoppedAt()).thenReturn(START.plusSeconds(5));
        when(run.observationStoppedAt()).thenReturn(OBSERVATION_STOP);
        when(run.archiveStatus()).thenReturn(status);
        return run;
    }

    /**
     * 구간 질의만 하는 원천입니다.
     *
     * <p><b>원천 존재 질의가 오면 테스트를 깨뜨립니다.</b> 조용히 true/false 를 돌려주면 그
     * 테스트가 무엇을 검증하는지 흐려지고, 빈 축 판정이 뒤집혀도 초록으로 남습니다 — 그 판정을
     * 보는 테스트는 따로 있습니다.</p>
     */
    private static RunTimeseriesArchiver.RangeSource rangeOnly(RangeOnly rangeQuery) {
        return new RunTimeseriesArchiver.RangeSource() {
            @Override
            public List<Sample> queryRange(Metric metric, Instant start, Instant end, int stepSeconds) {
                return rangeQuery.queryRange(metric, start, end, stepSeconds);
            }

            @Override
            public boolean sourceExists(Metric metric, Instant start, Instant end) {
                throw new AssertionError("이 테스트는 원천 존재 질의를 예상하지 않습니다: " + metric);
            }
        };
    }

    @FunctionalInterface
    private interface RangeOnly {
        List<Sample> queryRange(Metric metric, Instant start, Instant end, int stepSeconds);
    }

    private static final class CapturingSource implements RunTimeseriesArchiver.RangeSource {
        private final List<Instant> starts = new ArrayList<>();
        private final List<Instant> ends = new ArrayList<>();
        private final List<Integer> steps = new ArrayList<>();

        @Override
        public List<Sample> queryRange(Metric metric, Instant start, Instant end, int stepSeconds) {
            starts.add(start); ends.add(end); steps.add(stepSeconds);
            if (metric == Metric.LATENCY_P99) return List.of();
            return List.of(
                new Sample(metric, 0, start, 0d, State.VALID, null),
                new Sample(metric, end.getEpochSecond() - start.getEpochSecond(), end, 0d, State.VALID, null));
        }

        /** 미터는 있었고 그 축만 비었다는 답. 빈 축이 N_A 한 줄로 남는다. */
        @Override
        public boolean sourceExists(Metric metric, Instant start, Instant end) {
            return true;
        }
    }

    private static final class RecordingStore implements RunTimeseriesArchiver.ArchiveStore {
        private final List<Sample> inserted = new ArrayList<>();
        private String claimToken;
        private int chunkSize;
        @Override public void replaceForRun(
            long benchmarkRunId, String claimToken, List<Sample> samples, int chunkSize
        ) {
            this.claimToken = claimToken;
            this.chunkSize = chunkSize;
            inserted.clear();
            inserted.addAll(samples);
        }
    }
}
