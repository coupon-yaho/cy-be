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
        // 빈 range는 0이나 UNAVAILABLE 행으로 메우지 않는다.
        assertThat(store.inserted).hasSize(4);
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

        assertThat(store.inserted).hasSize(4);
    }

    @Test
    void missingRequiredSeriesFailsBeforeReplacingExistingArchive() {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        BenchmarkRun benchmarkRun = run(BenchmarkArchiveStatus.NONE);
        when(runs.findById(7)).thenReturn(Optional.of(benchmarkRun));
        RunTimeseriesArchiver.RangeSource source = (metric, start, end, step) -> List.of();
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
        RunTimeseriesArchiver.RangeSource source = (metric, start, end, step) -> excessive;
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
        RunTimeseriesArchiver.RangeSource source = (metric, start, end, step) -> {
            if (metric == Metric.LATENCY_P99) throw new IllegalStateException("prometheus down");
            return List.of(
                new Sample(metric, 0, start, 0d, State.VALID, null),
                new Sample(metric, end.getEpochSecond() - start.getEpochSecond(), end, 0d, State.VALID, null));
        };
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
        RunTimeseriesArchiver.RangeSource source = (metric, start, end, step) -> { throw original; };
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
        RunTimeseriesArchiver.RangeSource source =
            (metric, start, end, step) -> { throw new IllegalStateException("   "); };

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
        RunTimeseriesArchiver.RangeSource source =
            (metric, start, end, step) -> { throw new IllegalStateException("prometheus down"); };

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
        assertThat(store.inserted).hasSize(4);
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
        assertThat(store.inserted).hasSize(4);
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
        assertThat(store.inserted).hasSize(4);
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
        RunTimeseriesArchiver.RangeSource source = (metric, start, end, step) ->
                metric == Metric.STOCK_REMAINING ? invalid : List.of();

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
