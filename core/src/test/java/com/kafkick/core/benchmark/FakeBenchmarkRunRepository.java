package com.kafkick.core.benchmark;

import java.time.Instant;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.kafkick.core.support.exception.BusinessException;

/**
 * {@link BenchmarkRunRepository} 의 인메모리 대역.
 *
 * <p>전이 메서드가 <b>조건부 UPDATE 한 문장</b>과 같게 동작해야 한다 — 현재 상태를 확인하고
 * 맞을 때만 바꾸며, 안 맞으면 예외가 아니라 {@code false} 를 돌려준다. 여기서 그 계약을 느슨하게
 * 하면 서비스 테스트가 실제 어댑터와 다른 세계를 검증하게 된다.
 *
 * <p>중복 판정도 실제와 같이 <b>저장소가</b> 한다. 서비스가 미리 조회해 막는 구조였다면 api 가
 * N 대일 때 세 인스턴스가 같은 창을 통과하는데, 그 구조를 테스트가 승인해 버린다.
 */
class FakeBenchmarkRunRepository implements BenchmarkRunRepository {

    private final Map<Long, Row> rows = new LinkedHashMap<>();
    private final Clock clock;
    private long sequence;

    FakeBenchmarkRunRepository() {
        this(Clock.systemUTC());
    }

    FakeBenchmarkRunRepository(Clock clock) {
        this.clock = java.util.Objects.requireNonNull(clock);
    }

    /**
     * {@code markFinalized} 가 0행을 낸 <b>직후</b>에 끼어드는 다른 인스턴스를 흉내 낸다.
     * 서비스가 다시 읽기 전에 행이 바뀌는 창이 실재하는데, 순차 테스트로는 그 창을 만들 수 없다.
     */
    private java.util.function.LongConsumer onFinalizeRejected = id -> { };

    void onFinalizeRejected(java.util.function.LongConsumer hook) {
        this.onFinalizeRejected = hook;
    }

    @Override
    public long open(StartBenchmarkRunCommand command, Instant startedAt) {
        if (rows.values().stream().anyMatch(it -> it.runKey.equals(command.runKey()))) {
            throw new BusinessException(BenchmarkErrorCode.RUN_KEY_DUPLICATED, "runKey=" + command.runKey());
        }
        if (rows.values().stream().anyMatch(it -> it.status == BenchmarkRunStatus.RUNNING)) {
            throw new BusinessException(BenchmarkErrorCode.RUN_ALREADY_RUNNING, "runKey=" + command.runKey());
        }
        long id = ++sequence;
        rows.put(id, new Row(id, command, startedAt));
        return id;
    }

    @Override
    public Optional<BenchmarkRun> findById(long id) {
        return Optional.ofNullable(rows.get(id)).map(Row::toRun);
    }

    @Override
    public Optional<BenchmarkRun> findByRunKey(String runKey) {
        return rows.values().stream().filter(it -> it.runKey.equals(runKey)).findFirst().map(Row::toRun);
    }

    @Override
    public Optional<BenchmarkRun> findRunning() {
        return rows.values().stream()
                .filter(it -> it.status == BenchmarkRunStatus.RUNNING).findFirst().map(Row::toRun);
    }

    @Override
    public List<BenchmarkRun> findRecent(int limit) {
        List<BenchmarkRun> found = new ArrayList<>(rows.values().stream().map(Row::toRun).toList());
        found.sort(Comparator.comparing(BenchmarkRun::startedAt).reversed());
        return found.subList(0, Math.min(limit, found.size()));
    }

    @Override
    public boolean markLoadStopped(long id, Instant loadStoppedAt, String reason) {
        Row row = rows.get(id);
        if (row == null || row.status != BenchmarkRunStatus.RUNNING) {
            return false;
        }
        row.status = BenchmarkRunStatus.LOAD_STOPPED;
        row.loadStoppedAt = loadStoppedAt;
        row.loadStopReason = reason;
        return true;
    }

    @Override
    public boolean markObserved(long id, Instant observationStoppedAt, long observedLagTotal) {
        Row row = rows.get(id);
        if (row == null || row.status != BenchmarkRunStatus.LOAD_STOPPED) {
            return false;
        }
        row.status = BenchmarkRunStatus.OBSERVED;
        row.observationStoppedAt = observationStoppedAt;
        row.observedLagTotal = observedLagTotal;
        return true;
    }

    @Override
    public boolean markFinalized(long id, Instant finalizedAt) {
        Row row = rows.get(id);
        if (row == null || row.status != BenchmarkRunStatus.OBSERVED || row.client == null) {
            onFinalizeRejected.accept(id);
            return false;
        }
        row.status = BenchmarkRunStatus.FINALIZED;
        row.finalizedAt = finalizedAt;
        return true;
    }

    @Override
    public boolean updateClientSummary(long id, ClientLoadSummary summary) {
        Row row = rows.get(id);
        if (row == null || row.status == BenchmarkRunStatus.FINALIZED) {
            return false;
        }
        row.client = summary;
        return true;
    }

    @Override
    public boolean updateServerSummary(long id, ServerLoadSummary summary) {
        Row row = rows.get(id);
        if (row == null || row.status == BenchmarkRunStatus.FINALIZED) {
            return false;
        }
        row.server = summary;
        return true;
    }

    @Override
    public boolean updateArchiveStatus(long id, BenchmarkArchiveStatus status, String failureReason) {
        Row row = rows.get(id);
        if (row == null || row.archiveStatus != BenchmarkArchiveStatus.NONE
                || status != BenchmarkArchiveStatus.FAILED) {
            return false;
        }
        row.archiveStatus = status;
        row.archiveFailureReason = failureReason;
        return true;
    }

    @Override
    public java.util.Optional<String> claimArchive(long id, java.time.Duration lease) {
        if (lease == null || lease.compareTo(java.time.Duration.ofSeconds(1)) < 0) {
            throw new IllegalArgumentException("archive claim lease는 1초 이상이어야 한다");
        }
        if (lease.getNano() != 0) {
            throw new IllegalArgumentException("archive claim lease는 정수 초여야 한다");
        }
        if (lease.compareTo(java.time.Duration.ofDays(365)) > 0) {
            throw new IllegalArgumentException("archive claim lease가 지원 상한을 넘었다");
        }
        Row row = rows.get(id);
        Instant now = clock.instant();
        boolean expired = row != null && row.archiveStatus == BenchmarkArchiveStatus.IN_PROGRESS
            && row.archiveClaimedAt != null && row.archiveClaimedAt.isBefore(now.minus(lease));
        if (row == null || (row.archiveStatus != BenchmarkArchiveStatus.NONE
                && row.archiveStatus != BenchmarkArchiveStatus.FAILED && !expired)) {
            return java.util.Optional.empty();
        }
        String token = java.util.UUID.randomUUID().toString();
        row.archiveStatus = BenchmarkArchiveStatus.IN_PROGRESS;
        row.archiveFailureReason = null;
        row.archiveClaimedAt = now;
        row.archiveClaimToken = token;
        return java.util.Optional.of(token);
    }

    @Override
    public boolean failArchive(long id, String claimToken, String failureReason) {
        Row row = rows.get(id);
        if (row == null || row.archiveStatus != BenchmarkArchiveStatus.IN_PROGRESS
                || !java.util.Objects.equals(row.archiveClaimToken, claimToken)) {
            return false;
        }
        row.archiveStatus = BenchmarkArchiveStatus.FAILED;
        row.archiveFailureReason = failureReason;
        row.archiveClaimedAt = null;
        row.archiveClaimToken = null;
        return true;
    }

    private static final class Row {

        private final long id;
        private final StartBenchmarkRunCommand command;
        private final String runKey;
        private final Instant startedAt;

        private BenchmarkRunStatus status = BenchmarkRunStatus.RUNNING;
        private BenchmarkArchiveStatus archiveStatus = BenchmarkArchiveStatus.NONE;
        private String archiveFailureReason;
        private Instant archiveClaimedAt;
        private String archiveClaimToken;
        private Instant loadStoppedAt;
        private Instant observationStoppedAt;
        private Instant finalizedAt;
        private String loadStopReason;
        private ClientLoadSummary client;
        private ServerLoadSummary server;
        private Long observedLagTotal;

        private Row(long id, StartBenchmarkRunCommand command, Instant startedAt) {
            this.id = id;
            this.command = command;
            this.runKey = command.runKey();
            this.startedAt = startedAt;
        }

        private BenchmarkRun toRun() {
            return new BenchmarkRun(id, runKey, command.runType(), command.scenarioCode(),
                    command.engineVersion(), command.releaseStage(), command.queueMode(),
                    command.couponId(), status, archiveStatus, archiveFailureReason,
                    startedAt, loadStoppedAt, observationStoppedAt, finalizedAt,
                    command.requestedBy(), loadStopReason,
                    command.topology(), command.loadProfile(), command.toolMeta(),
                    client, server, observedLagTotal);
        }
    }
}
