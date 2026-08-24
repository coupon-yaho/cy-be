package com.kafkick.api.admin.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verifyNoInteractions;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import com.kafkick.core.admin.BenchmarkRunState;
import com.kafkick.core.benchmark.BenchmarkRun;
import com.kafkick.core.benchmark.BenchmarkRunService;
import com.kafkick.core.benchmark.RunTimeseriesArchiver;
import com.kafkick.core.benchmark.BenchmarkArchiveStatus;
import com.kafkick.core.benchmark.BenchmarkErrorCode;
import com.kafkick.core.support.exception.BusinessException;

class BenchmarkFinalizeOrchestratorTest {

    private final BenchmarkRunService runs = mock(BenchmarkRunService.class);
    private final RunTimeseriesArchiver archiver = mock(RunTimeseriesArchiver.class);
    private final BenchmarkFinalizeOrchestrator orchestrator =
        new BenchmarkFinalizeOrchestrator(runs, archiver);

    @Test
    void finalizedRunIsArchivedAfterFinalization() {
        BenchmarkRun finalized = mock(BenchmarkRun.class);
        when(finalized.id()).thenReturn(7L);
        when(finalized.finalizedAt()).thenReturn(Instant.parse("2026-08-23T00:01:06Z"));
        when(runs.finalizeRun(7L)).thenReturn(finalized);

        var response = orchestrator.finalizeRun(7L);

        InOrder order = inOrder(runs, archiver);
        order.verify(runs).finalizeRun(7L);
        order.verify(archiver).archive(7L);
        assertThat(response.state()).isEqualTo(BenchmarkRunState.FINALIZED);
        assertThat(response.archiveStatus()).isEqualTo(BenchmarkArchiveStatus.DONE);
    }

    @Test
    void archiveFailureDoesNotTurnSuccessfulFinalizeIntoApiFailure() {
        BenchmarkRun finalized = mock(BenchmarkRun.class);
        when(finalized.id()).thenReturn(7L);
        when(finalized.finalizedAt()).thenReturn(Instant.parse("2026-08-23T00:01:06Z"));
        when(runs.finalizeRun(7L)).thenReturn(finalized);
        org.mockito.Mockito.doThrow(new IllegalStateException("prometheus down"))
            .when(archiver).archive(7L);
        BenchmarkRun persisted = mock(BenchmarkRun.class);
        when(persisted.archiveStatus()).thenReturn(BenchmarkArchiveStatus.NONE);
        when(runs.get(7L)).thenReturn(persisted);

        var response = orchestrator.finalizeRun(7L);

        assertThat(response.state()).isEqualTo(BenchmarkRunState.FINALIZED);
        assertThat(response.archiveStatus()).isEqualTo(BenchmarkArchiveStatus.NONE);
        org.mockito.Mockito.verify(runs).finalizeRun(7L);
        org.mockito.Mockito.verify(runs).get(7L);
    }

    @Test
    void archiveConflictAfterFinalizeReturnsThePersistedArchiveStatus() {
        BenchmarkRun finalized = mock(BenchmarkRun.class);
        when(finalized.id()).thenReturn(7L);
        when(finalized.finalizedAt()).thenReturn(Instant.parse("2026-08-23T00:01:06Z"));
        when(runs.finalizeRun(7L)).thenReturn(finalized);
        BenchmarkRun persisted = mock(BenchmarkRun.class);
        when(persisted.archiveStatus()).thenReturn(BenchmarkArchiveStatus.IN_PROGRESS);
        when(runs.get(7L)).thenReturn(persisted);
        org.mockito.Mockito.doThrow(new BusinessException(
            BenchmarkErrorCode.ILLEGAL_TRANSITION, "archive already completed"))
            .when(archiver).archive(7L);

        var response = orchestrator.finalizeRun(7L);

        assertThat(response.state()).isEqualTo(BenchmarkRunState.FINALIZED);
        assertThat(response.archiveStatus()).isEqualTo(BenchmarkArchiveStatus.IN_PROGRESS);
    }

    @Test
    void missingDependenciesAreReportedAsNotImplemented() {
        BenchmarkFinalizeOrchestrator unavailable = new BenchmarkFinalizeOrchestrator(null, archiver);

        assertThatThrownBy(() -> unavailable.finalizeRun(7L))
            .isInstanceOfSatisfying(BusinessException.class, exception ->
                assertThat(exception.getErrorCode())
                    .isEqualTo(com.kafkick.api.admin.support.AdminApiErrorCode.NOT_IMPLEMENTED));
    }

    @Test
    void finalizeFailureDoesNotStartArchive() {
        when(runs.finalizeRun(7L)).thenThrow(new IllegalStateException("database down"));

        assertThatThrownBy(() -> orchestrator.finalizeRun(7L))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("database down");
        verifyNoInteractions(archiver);
    }

    @Test
    void archiveAndStatusReloadFailuresBothRemainDiagnosable() {
        BenchmarkRun finalized = mock(BenchmarkRun.class);
        when(runs.finalizeRun(7L)).thenReturn(finalized);
        IllegalStateException archiveFailure = new IllegalStateException("prometheus down");
        org.mockito.Mockito.doThrow(archiveFailure).when(archiver).archive(7L);
        IllegalStateException reloadFailure = new IllegalStateException("database down");
        when(runs.get(7L)).thenThrow(reloadFailure);

        assertThatThrownBy(() -> orchestrator.finalizeRun(7L))
            .isSameAs(reloadFailure)
            .satisfies(failure -> assertThat(failure.getSuppressed()).contains(archiveFailure));
    }
}
