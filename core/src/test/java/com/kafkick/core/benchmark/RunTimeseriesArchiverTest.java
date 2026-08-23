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
import org.mockito.ArgumentCaptor;

import com.kafkick.core.benchmark.RunTimeseriesArchiver.Metric;
import com.kafkick.core.benchmark.RunTimeseriesArchiver.Sample;
import com.kafkick.core.benchmark.RunTimeseriesArchiver.State;
import com.kafkick.core.support.exception.BusinessException;

class RunTimeseriesArchiverTest {

    private static final Instant START = Instant.parse("2026-08-23T00:00:00Z");
    private static final Instant OBSERVATION_STOP = Instant.parse("2026-08-23T00:01:05Z");

    @Test
    void usesObservationStopAndDoesNotInventMissingZeroes() {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        BenchmarkRun run = run(BenchmarkArchiveStatus.NONE);
        when(runs.findById(7)).thenReturn(Optional.of(run));
        CapturingSource source = new CapturingSource();
        RecordingStore store = new RecordingStore();

        new RunTimeseriesArchiver(runs, source, store).archive(7);

        assertThat(source.ends).containsOnly(OBSERVATION_STOP);
        assertThat(source.steps).containsOnly(1);
        // 빈 range는 0이나 UNAVAILABLE 행으로 메우지 않는다.
        assertThat(store.inserted).hasSize(2);
        assertThat(store.inserted.get(0).value()).isEqualTo(0d);
        verify(runs).updateArchiveStatus(7, BenchmarkArchiveStatus.DONE, null);
    }

    @Test
    void missingRequiredSeriesFailsBeforeReplacingExistingArchive() {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        BenchmarkRun benchmarkRun = run(BenchmarkArchiveStatus.NONE);
        when(runs.findById(7)).thenReturn(Optional.of(benchmarkRun));
        RunTimeseriesArchiver.RangeSource source = (metric, start, end, step) -> List.of();
        RunTimeseriesArchiver.ArchiveStore store = mock(RunTimeseriesArchiver.ArchiveStore.class);

        assertThatThrownBy(() -> new RunTimeseriesArchiver(runs, source, store).archive(7))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("STOCK_REMAINING");

        verify(store, never()).deleteForRun(7);
        verify(runs).updateArchiveStatus(eq(7L), eq(BenchmarkArchiveStatus.FAILED),
                org.mockito.ArgumentMatchers.contains("STOCK_REMAINING"));
    }

    @Test
    void failedRetryRemainsFailedWhenPrometheusFailsAgain() {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        BenchmarkRun failedRun = run(BenchmarkArchiveStatus.FAILED);
        when(runs.findById(7)).thenReturn(Optional.of(failedRun));
        when(runs.claimFailedArchive(7)).thenReturn(true);
        RunTimeseriesArchiver.RangeSource source = (metric, start, end, step) -> {
            if (metric == Metric.LATENCY_P99) throw new IllegalStateException("prometheus down");
            return List.of(new Sample(metric, 0, start, 0d, State.VALID, null));
        };
        RunTimeseriesArchiver archiver = new RunTimeseriesArchiver(runs, source, new RecordingStore());

        assertThatThrownBy(() -> archiver.retry(7)).hasMessageContaining("prometheus down");
        ArgumentCaptor<String> reason = ArgumentCaptor.forClass(String.class);
        verify(runs).updateArchiveStatus(eq(7L), eq(BenchmarkArchiveStatus.FAILED), reason.capture());
        assertThat(reason.getValue()).contains("prometheus down");
    }

    @Test
    void failedRunCanBeRetriedToDone() {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        BenchmarkRun failedRun = run(BenchmarkArchiveStatus.FAILED);
        when(runs.findById(7)).thenReturn(Optional.of(failedRun));
        when(runs.claimFailedArchive(7)).thenReturn(true);

        new RunTimeseriesArchiver(runs, new CapturingSource(), new RecordingStore()).retry(7);

        verify(runs).updateArchiveStatus(7, BenchmarkArchiveStatus.DONE, null);
    }

    @Test
    void concurrentRetryCannotArchiveWithoutOwningFailedStatus() {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        BenchmarkRun failedRun = run(BenchmarkArchiveStatus.FAILED);
        when(runs.findById(7)).thenReturn(Optional.of(failedRun));
        when(runs.claimFailedArchive(7)).thenReturn(false);
        RunTimeseriesArchiver.RangeSource source = mock(RunTimeseriesArchiver.RangeSource.class);

        assertThatThrownBy(() -> new RunTimeseriesArchiver(
                runs, source, new RecordingStore()).retry(7))
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
                runs, new CapturingSource(), new RecordingStore()).archive(7))
                .isInstanceOfSatisfying(BusinessException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(BenchmarkErrorCode.ILLEGAL_TRANSITION));
    }

    @Test
    void reportsMissingRunAsNotFound() {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        when(runs.findById(404)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> new RunTimeseriesArchiver(
                runs, new CapturingSource(), new RecordingStore()).archive(404))
                .isInstanceOfSatisfying(BusinessException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(BenchmarkErrorCode.RUN_NOT_FOUND));
    }

    @Test
    void reportsRetryOfNonFailedRunAsIllegalTransition() {
        BenchmarkRunRepository runs = mock(BenchmarkRunRepository.class);
        BenchmarkRun done = run(BenchmarkArchiveStatus.DONE);
        when(runs.findById(7)).thenReturn(Optional.of(done));

        assertThatThrownBy(() -> new RunTimeseriesArchiver(
                runs, new CapturingSource(), new RecordingStore()).retry(7))
                .isInstanceOfSatisfying(BusinessException.class, exception ->
                        assertThat(exception.getErrorCode()).isEqualTo(BenchmarkErrorCode.ILLEGAL_TRANSITION));
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

        assertThatThrownBy(() -> new RunTimeseriesArchiver(runs, source, new RecordingStore()).archive(7))
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
        private final List<Instant> ends = new ArrayList<>();
        private final List<Integer> steps = new ArrayList<>();

        @Override
        public List<Sample> queryRange(Metric metric, Instant start, Instant end, int stepSeconds) {
            ends.add(end); steps.add(stepSeconds);
            if (metric == Metric.LATENCY_P99) return List.of();
            return List.of(new Sample(metric, 0, start, 0d, State.VALID, null));
        }
    }

    private static final class RecordingStore implements RunTimeseriesArchiver.ArchiveStore {
        private final List<Sample> inserted = new ArrayList<>();
        @Override public void deleteForRun(long benchmarkRunId) { }
        @Override public void insert(long benchmarkRunId, List<Sample> samples) { inserted.addAll(samples); }
    }
}
