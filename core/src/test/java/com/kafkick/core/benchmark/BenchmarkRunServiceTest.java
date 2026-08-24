package com.kafkick.core.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.ErrorCode;

/** 회차 운영 명령이 지키는 두 가지 — 회차는 하나만 돌고, 백로그가 0 이어야 관측을 닫는다. */
class BenchmarkRunServiceTest {

    private MutableClock clock;
    private FakeBenchmarkRunRepository repository;
    private BenchmarkRunService service;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(Instant.parse("2026-08-22T00:00:00Z"));
        repository = new FakeBenchmarkRunRepository(clock);
        service = new BenchmarkRunService(repository, new TimeProvider(clock));
    }

    @Nested
    @DisplayName("회차는 동시에 하나만 돈다")
    class SingleRunningRun {

        @Test
        @DisplayName("진행 중인 회차가 있으면 두 번째 start 가 409 로 막힌다")
        void secondStartIsRejectedWhileOneRuns() {
            service.start(command("V3-MAIN-01"));

            // 두 회차가 겹치면 이벤트의 benchmark_run_id 가 섞이고, 섞인 회차는 사후에 분리할 수 없다.
            assertThatThrownBy(() -> service.start(command("V3-MAIN-02")))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(it -> assertThat(codeOf(it)).isEqualTo(BenchmarkErrorCode.RUN_ALREADY_RUNNING));
        }

        @Test
        @DisplayName("같은 회차 키로 두 번 start 하면 409 다")
        void duplicateRunKeyIsRejected() {
            BenchmarkRun first = service.start(command("V3-MAIN-01"));
            service.stopLoad(first.id(), null);

            assertThatThrownBy(() -> service.start(command("V3-MAIN-01")))
                    .satisfies(it -> assertThat(codeOf(it)).isEqualTo(BenchmarkErrorCode.RUN_KEY_DUPLICATED));
        }

        @Test
        @DisplayName("앞 회차의 부하가 끝나면 다음 회차를 열 수 있다")
        void nextRunOpensAfterLoadStops() {
            BenchmarkRun first = service.start(command("V3-WARMUP"));
            service.stopLoad(first.id(), null);

            assertThat(service.start(command("V3-MAIN-01")).runStatus())
                    .isEqualTo(BenchmarkRunStatus.RUNNING);
        }
    }

    @Nested
    @DisplayName("시각 셋이 각자 남는다")
    class ThreeTimestamps {

        @Test
        @DisplayName("관측 종료는 부하 종료보다 뒤에 찍힌다 — v3 의 수렴 구간이 그 사이다")
        void observationOutlastsLoad() {
            BenchmarkRun run = service.start(command("V3-MAIN-01"));

            clock.advance(Duration.ofSeconds(5));
            service.stopLoad(run.id(), null);
            clock.advance(Duration.ofSeconds(60));
            BenchmarkRun observed = service.stopObservation(run.id(), 0);

            assertThat(observed.loadStoppedAt()).isEqualTo(Instant.parse("2026-08-22T00:00:05Z"));
            assertThat(observed.observationStoppedAt()).isEqualTo(Instant.parse("2026-08-22T00:01:05Z"));
            assertThat(observed.observationOutlastedLoad()).isTrue();
        }

        @Test
        @DisplayName("부하가 안 끝난 회차의 관측을 닫을 수 없다")
        void observationCannotCloseBeforeLoad() {
            BenchmarkRun run = service.start(command("V3-MAIN-01"));

            assertThatThrownBy(() -> service.stopObservation(run.id(), 0))
                    .satisfies(it -> assertThat(codeOf(it)).isEqualTo(BenchmarkErrorCode.ILLEGAL_TRANSITION));
        }

        @Test
        @DisplayName("이미 끝난 부하를 다시 끊을 수 없다")
        void loadStopIsNotRepeatable() {
            BenchmarkRun run = service.start(command("V3-MAIN-01"));
            service.stopLoad(run.id(), null);

            assertThatThrownBy(() -> service.stopLoad(run.id(), null))
                    .satisfies(it -> assertThat(codeOf(it)).isEqualTo(BenchmarkErrorCode.ILLEGAL_TRANSITION));
        }

        @Test
        @DisplayName("없는 회차는 전이 실패가 아니라 404 다")
        void missingRunIsNotFound() {
            assertThatThrownBy(() -> service.stopLoad(404L, null))
                    .satisfies(it -> assertThat(codeOf(it)).isEqualTo(BenchmarkErrorCode.RUN_NOT_FOUND));
        }
    }

    @Nested
    @DisplayName("lag=0 이어야 관측을 닫는다")
    class BacklogGate {

        @Test
        @DisplayName("백로그가 남아 있으면 거부하고 상태를 그대로 둔다")
        void backlogBlocksObservationStop() {
            BenchmarkRun run = service.start(command("V3-MAIN-01"));
            service.stopLoad(run.id(), null);

            assertThatThrownBy(() -> service.stopObservation(run.id(), 42))
                    .satisfies(it -> assertThat(codeOf(it)).isEqualTo(BenchmarkErrorCode.BACKLOG_NOT_DRAINED));

            // 거부가 상태를 반쯤 옮겨 놓으면 다음 시도가 ILLEGAL_TRANSITION 으로 바뀌어,
            // 원인이 "백로그" 에서 "상태" 로 조용히 갈린다.
            assertThat(service.get(run.id()).runStatus()).isEqualTo(BenchmarkRunStatus.LOAD_STOPPED);
            assertThat(service.get(run.id()).observationStoppedAt()).isNull();
        }

        /**
         * 500 이 아니라 400 이어야 한다. IllegalArgumentException 으로 두면
         * GlobalExceptionHandler 의 @ExceptionHandler(Exception.class) 가 500 으로 뭉개서,
         * 값을 고쳐 다시 보내면 되는 상황이 서버 장애로 보고된다.
         */
        @Test
        @DisplayName("음수 백로그는 400 이다 — 500 으로 뭉개지지 않는다")
        void negativeBacklogIsAClientError() {
            BenchmarkRun run = service.start(command("V3-MAIN-01"));
            service.stopLoad(run.id(), null);

            assertThatThrownBy(() -> service.stopObservation(run.id(), -1))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(it -> {
                        assertThat(codeOf(it)).isEqualTo(BenchmarkErrorCode.INVALID_RUN_CONDITION);
                        assertThat(codeOf(it).getStatus()).isEqualTo(400);
                    });
        }

        @Test
        @DisplayName("백로그가 0 으로 수렴하면 관측이 닫히고 그 값이 남는다")
        void drainedBacklogClosesObservation() {
            BenchmarkRun observed = drainedRun("V3-MAIN-01");

            assertThat(observed.runStatus()).isEqualTo(BenchmarkRunStatus.OBSERVED);
            assertThat(observed.observedLagTotal()).isZero();
        }
    }

    @Nested
    @DisplayName("확정은 공식 결과를 요구한다")
    class Finalize {

        @Test
        @DisplayName("공식 결과가 없으면 확정할 수 없다")
        void officialSummaryIsRequired() {
            BenchmarkRun observed = drainedRun("V3-MAIN-01");

            assertThatThrownBy(() -> service.finalizeRun(observed.id()))
                    .satisfies(it -> assertThat(codeOf(it))
                            .isEqualTo(BenchmarkErrorCode.OFFICIAL_SUMMARY_MISSING));
        }

        @Test
        @DisplayName("관측이 안 끝난 회차의 확정 실패는 전이 오류다")
        void notObservedYetIsATransitionError() {
            BenchmarkRun run = service.start(command("V3-MAIN-01"));
            service.recordClientSummary(run.id(), clientSummary(0));

            assertThatThrownBy(() -> service.finalizeRun(run.id()))
                    .satisfies(it -> assertThat(codeOf(it)).isEqualTo(BenchmarkErrorCode.ILLEGAL_TRANSITION));
        }

        /**
         * markFinalized 가 0행을 낸 뒤 다시 읽는 사이에 다른 인스턴스가 요약을 붙일 수 있다.
         * 그때 "공식 결과가 없다" 고 말하면 거짓이다 — 다시 시도하면 되는 상황이라 그렇게 알린다.
         */
        @Test
        @DisplayName("확정 도중 다른 인스턴스가 요약을 붙이면 요약 없음이라고 하지 않는다")
        void concurrentSummaryIsNotReportedAsMissing() {
            BenchmarkRun observed = drainedRun("V3-MAIN-01");
            repository.onFinalizeRejected(id -> service.recordClientSummary(id, clientSummary(0)));

            assertThatThrownBy(() -> service.finalizeRun(observed.id()))
                    .satisfies(it -> assertThat(codeOf(it))
                            .isEqualTo(BenchmarkErrorCode.ILLEGAL_TRANSITION));
        }

        @Test
        @DisplayName("확정된 회차의 결과는 고칠 수 없다")
        void finalizedSummaryIsLocked() {
            BenchmarkRun finalized = finalizedRun("V3-MAIN-01");

            assertThatThrownBy(() -> service.recordClientSummary(finalized.id(), clientSummary(0)))
                    .satisfies(it -> assertThat(codeOf(it)).isEqualTo(BenchmarkErrorCode.SUMMARY_LOCKED));
            assertThatThrownBy(() -> service.recordServerSummary(finalized.id(), serverSummary()))
                    .satisfies(it -> assertThat(codeOf(it)).isEqualTo(BenchmarkErrorCode.SUMMARY_LOCKED));
        }
    }

    @Nested
    @DisplayName("server_* 와 client_* 는 섞이지 않는다")
    class SeparateSummaries {

        /**
         * 두 요약이 다른 타입이라 자리를 바꿔 넣는 것 자체가 컴파일 오류다. 이 테스트가 지키는 건
         * 저장까지 갔을 때 값이 서로의 자리로 새지 않는다는 것이다.
         */
        @Test
        @DisplayName("한쪽을 기록해도 다른 쪽 값이 바뀌지 않는다")
        void recordingOneSideLeavesTheOther() {
            BenchmarkRun run = service.start(command("V3-MAIN-01"));

            service.recordClientSummary(run.id(), clientSummary(0));
            assertThat(service.get(run.id()).server()).isEmpty();

            BenchmarkRun both = service.recordServerSummary(run.id(), serverSummary());
            assertThat(both.client()).get().extracting(ClientLoadSummary::p99Millis).isEqualTo(412.3);
            assertThat(both.server()).get().extracting(ServerLoadSummary::p99Millis).isEqualTo(180.0);
        }

        @Test
        @DisplayName("도착률이 무너진 회차는 기록은 되고 비교 대상에서는 빠진다")
        void droppedIterationsAreRecordedButNotComparable() {
            BenchmarkRun run = service.start(command("V3-MAIN-01"));
            clock.advance(Duration.ofSeconds(5));
            service.stopLoad(run.id(), null);
            clock.advance(Duration.ofSeconds(60));
            service.stopObservation(run.id(), 0);
            service.recordClientSummary(run.id(), clientSummary(1274));
            BenchmarkRun finalized = service.finalizeRun(run.id());

            assertThat(finalized.client()).get()
                    .extracting(ClientLoadSummary::arrivalRateHeld).isEqualTo(false);
            assertThat(finalized.comparable()).isFalse();
        }

        @Test
        @DisplayName("워밍업 회차는 확정돼도 비교 대상이 아니다")
        void warmupIsNeverComparable() {
            BenchmarkRun warmup = finalizedRun("V3-WARMUP", BenchmarkRunType.WARMUP);

            assertThat(warmup.runStatus()).isEqualTo(BenchmarkRunStatus.FINALIZED);
            assertThat(warmup.comparable()).isFalse();
        }
    }

    @Nested
    @DisplayName("archive 는 회차와 독립이다")
    class Archive {

        @Test
        @DisplayName("archive 가 실패해도 회차는 확정 상태로 남는다")
        void failedArchiveKeepsRunFinalized() {
            BenchmarkRun finalized = finalizedRun("V3-MAIN-01");

            BenchmarkRun archived = service.recordArchiveResult(
                    finalized.id(), BenchmarkArchiveStatus.FAILED, "query_range timeout");

            assertThat(archived.runStatus()).isEqualTo(BenchmarkRunStatus.FINALIZED);
            assertThat(archived.archiveStatus()).isEqualTo(BenchmarkArchiveStatus.FAILED);
            assertThat(archived.archiveFailureReason()).isEqualTo("query_range timeout");
        }

        @Test
        void secondInitialFailureIsAnIllegalTransition() {
            BenchmarkRun finalized = finalizedRun("V3-MAIN-01");
            service.recordArchiveResult(
                finalized.id(), BenchmarkArchiveStatus.FAILED, "first failure");

            assertThatThrownBy(() -> service.recordArchiveResult(
                finalized.id(), BenchmarkArchiveStatus.FAILED, "second failure"))
                .satisfies(it -> assertThat(codeOf(it))
                    .isEqualTo(BenchmarkErrorCode.ILLEGAL_TRANSITION));
        }

        @Test
        void archiveLeaseBelowOneSecondIsRejected() {
            for (Duration invalid : java.util.List.of(
                    Duration.ofMillis(999), Duration.ZERO, Duration.ofSeconds(-1))) {
                assertThatThrownBy(() -> repository.claimArchive(1L, invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("1초 이상");
            }
        }

        @Test
        void archiveLeaseAt365DaysIsAcceptedAnd366DaysIsRejected() {
            assertThat(repository.claimArchive(1L, Duration.ofDays(365))).isEmpty();
            assertThatThrownBy(() -> repository.claimArchive(1L, Duration.ofDays(366)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("지원 상한");
        }

        @Test
        void fractionalSecondArchiveLeaseIsRejected() {
            assertThatThrownBy(() -> repository.claimArchive(
                    1L, Duration.ofSeconds(1).plusMillis(500)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("정수 초");
        }

        @Test
        @DisplayName("DONE 은 표본 적재 트랜잭션 밖에서 기록할 수 없다")
        void doneCannotBypassTheArchiveStoreTransaction() {
            BenchmarkRun finalized = finalizedRun("V3-MAIN-01");

            assertThatThrownBy(() -> service.recordArchiveResult(
                    finalized.id(), BenchmarkArchiveStatus.DONE, null))
                .satisfies(it -> assertThat(codeOf(it))
                    .isEqualTo(BenchmarkErrorCode.INVALID_RUN_CONDITION));
        }

        @Test
        @DisplayName("실패 이유 없는 FAILED 는 받지 않는다")
        void failureNeedsAReason() {
            BenchmarkRun finalized = finalizedRun("V3-MAIN-01");

            assertThatThrownBy(() -> service.recordArchiveResult(
                    finalized.id(), BenchmarkArchiveStatus.FAILED, null))
                    .satisfies(it -> assertThat(codeOf(it))
                            .isEqualTo(BenchmarkErrorCode.INVALID_RUN_CONDITION));
        }

        /**
         * "" 는 {@code != null} 이라 XOR 을 통과한다. 통과시키면 이유 없는 FAILED 가 그대로 남고,
         * 재실행 판단의 유일한 근거가 빈 문자열이 된다.
         */
        @Test
        @DisplayName("공백뿐인 실패 이유는 이유가 없는 것이다")
        void blankReasonIsNoReason() {
            BenchmarkRun finalized = finalizedRun("V3-MAIN-01");

            assertThatThrownBy(() -> service.recordArchiveResult(
                    finalized.id(), BenchmarkArchiveStatus.FAILED, "   "))
                    .satisfies(it -> assertThat(codeOf(it))
                            .isEqualTo(BenchmarkErrorCode.INVALID_RUN_CONDITION));
            assertThatThrownBy(() -> service.recordArchiveResult(
                    finalized.id(), BenchmarkArchiveStatus.FAILED, ""))
                    .satisfies(it -> assertThat(codeOf(it))
                            .isEqualTo(BenchmarkErrorCode.INVALID_RUN_CONDITION));

            // 탭·줄바꿈도 같이 본다. DB 의 ck_run_archive_reason 이 REGEXP_LIKE 로 같은 판정을
            // 하므로, 여기서 통과시키면 두 층의 기준이 갈린다 — TRIM 을 쓰던 동안 실제로 갈려 있었다.
            assertThatThrownBy(() -> service.recordArchiveResult(
                    finalized.id(), BenchmarkArchiveStatus.FAILED, "\t\n"))
                    .satisfies(it -> assertThat(codeOf(it))
                            .isEqualTo(BenchmarkErrorCode.INVALID_RUN_CONDITION));
        }

        /** XOR 의 반대 방향. 성공한 archive 에 실패 이유가 남아 있으면 재실행 판단이 거짓이 된다. */
        @Test
        @DisplayName("DONE 에 실패 이유를 붙이면 받지 않는다")
        void successCannotCarryAReason() {
            BenchmarkRun finalized = finalizedRun("V3-MAIN-01");

            assertThatThrownBy(() -> service.recordArchiveResult(
                    finalized.id(), BenchmarkArchiveStatus.DONE, "timeout"))
                    .satisfies(it -> assertThat(codeOf(it))
                            .isEqualTo(BenchmarkErrorCode.INVALID_RUN_CONDITION));
        }

        @Test
        void doneCannotBypassFencingEvenWhileArchiveIsActive() {
            BenchmarkRun finalized = finalizedRun("V3-MAIN-01");
            assertThat(repository.claimArchive(
                finalized.id(), Duration.ofMinutes(5))).isPresent();

            assertThatThrownBy(() -> service.recordArchiveResult(
                finalized.id(), BenchmarkArchiveStatus.DONE, null))
                .satisfies(it -> assertThat(codeOf(it))
                    .isEqualTo(BenchmarkErrorCode.INVALID_RUN_CONDITION));
        }

        @Test
        void inProgressCanOnlyBeEnteredThroughClaimArchive() {
            BenchmarkRun finalized = finalizedRun("V3-MAIN-01");

            assertThatThrownBy(() -> service.recordArchiveResult(
                finalized.id(), BenchmarkArchiveStatus.IN_PROGRESS, null))
                .satisfies(it -> assertThat(codeOf(it))
                    .isEqualTo(BenchmarkErrorCode.INVALID_RUN_CONDITION));
        }
    }

    // ── 고정값 ───────────────────────────────────────────────────────────────

    private BenchmarkRun drainedRun(String runKey) {
        BenchmarkRun run = service.start(command(runKey));
        clock.advance(Duration.ofSeconds(5));
        service.stopLoad(run.id(), null);
        clock.advance(Duration.ofSeconds(60));
        return service.stopObservation(run.id(), 0);
    }

    private BenchmarkRun finalizedRun(String runKey) {
        return finalizedRun(runKey, BenchmarkRunType.MAIN);
    }

    private BenchmarkRun finalizedRun(String runKey, BenchmarkRunType runType) {
        BenchmarkRun run = service.start(command(runKey, runType));
        clock.advance(Duration.ofSeconds(5));
        service.stopLoad(run.id(), null);
        clock.advance(Duration.ofSeconds(60));
        service.stopObservation(run.id(), 0);
        service.recordClientSummary(run.id(), clientSummary(0));
        return service.finalizeRun(run.id());
    }

    private StartBenchmarkRunCommand command(String runKey) {
        return command(runKey, BenchmarkRunType.MAIN);
    }

    private StartBenchmarkRunCommand command(String runKey, BenchmarkRunType runType) {
        return new StartBenchmarkRunCommand(
                runKey, runType, "LOAD_100K",
                EngineVersion.V3, ReleaseStage.V3, QueueMode.ADAPTIVE,
                7L, "tester",
                new BenchmarkTopology(3, 6, null, null, 180, 8192, 200, 36, 50),
                new LoadProfile(20_000, 5, 60, 10_000, 0.8),
                LoadToolMeta.unknown());
    }

    private ClientLoadSummary clientSummary(long droppedIterations) {
        return new ClientLoadSummary(100_000, 0, droppedIterations, 18_472.5, 206.1, 412.3,
                clock.instant());
    }

    private ServerLoadSummary serverSummary() {
        return new ServerLoadSummary(100_000, 0, 18_500.0, 90.0, 180.0, clock.instant());
    }

    private static ErrorCode codeOf(Throwable throwable) {
        return ((BusinessException) throwable).getErrorCode();
    }

    /** 회차 시각 셋이 실제로 벌어지는지 보려면 시계를 움직일 수 있어야 한다. */
    private static final class MutableClock extends Clock {

        private Instant instant;

        private MutableClock(Instant instant) {
            this.instant = instant;
        }

        private void advance(Duration duration) {
            instant = instant.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
