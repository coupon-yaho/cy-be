package com.kafkick.core.benchmark;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.consistency.ConsistencyFinalStatus;

/**
 * 한 번의 부하 측정 회차. {@code benchmark_runs} 한 행에 대응한다.
 *
 * <p>여기 담기는 것은 <b>회차 메타와 공식 요약뿐</b>이다. 1초 시계열은 Prometheus 가 보관하고
 * (retention 30d), 완료 회차의 MySQL 사본은 OBS-22 가 따로 다룬다.
 *
 * <p><b>여기의 위반은 {@link IllegalArgumentException} 으로 남긴다.</b> 다른 값 객체
 * ({@code StartBenchmarkRunCommand} · {@code BenchmarkTopology} · 요약 둘)는 사람이 넣은 입력이라
 * {@code BENCHMARK-008}(400)로 번역하지만, 이 레코드는 <b>이미 저장된 행에서 조립된다</b>.
 * 여기서 불변식이 깨졌다면 DB CHECK 를 우회한 행이 있다는 뜻이라 데이터 손상이지 요청의 잘못이
 * 아니다. 400·409 로 번역하면 손상을 "값을 고쳐 다시 보내면 되는 상황" 으로 오보하게 된다.
 *
 * <p>불변식을 DB CHECK 와 <b>양쪽에</b> 둔다. DB 쪽은 백필·수동 보정처럼 이 코드를 지나지 않는
 * 경로를 막고, 여기 있는 것은 그 경로를 지나는 코드가 잘못된 행을 만들기 전에 막는다.
 * 한쪽만 두면 다른 쪽 경로가 그대로 뚫린다.
 *
 * @param id 저장된 회차의 식별자
 * @param runKey 사람이 정하는 회차 키. 중복 start 를 여기서 가른다
 * @param runType 회차 성격
 * @param scenarioCode 부하 시나리오 코드
 * @param engineVersion 이 회차의 <b>권위</b> 엔진 버전. 회차 도중 런타임 토글(OBS-19)이 바뀌어도
 *        측정의 기준은 시작할 때 박제된 이 값이다
 * @param releaseStage 릴리스 단계
 * @param queueMode 대기열 모드
 * @param couponId 이 회차가 쓴 쿠폰. FK 는 걸지 않는다 — 회차 기록은 쿠폰보다 오래 살아야 한다
 * @param runStatus 회차 수명주기
 * @param archiveStatus 시계열 archive 결과. 회차 수명주기와 독립이다
 * @param archiveFailureReason archive 실패 이유. {@code FAILED} 일 때만 있다
 * @param startedAt 회차 시작
 * @param loadStoppedAt 부하 종료
 * @param observationStoppedAt 관측 종료. PromQL 질의 범위와 archive 구간의 오른쪽 끝이다
 * @param finalizedAt 확정 시각
 * @param requestedBy 회차를 연 <b>계정 식별자</b>({@code members.id}). 이름·이메일을 넣지 않는다 —
 *        이 값은 회차 목록 응답과 오류 detail 로그를 타고 밖으로 나간다
 * @param loadStopReason 부하를 왜 끊었는지. 계획대로 끝났으면 비어 있다
 * @param topology 자원·토폴로지 조건
 * @param loadProfile 부하 입력
 * @param toolMeta 부하 도구 메타
 * @param clientSummary 종단 공식 요약. 아직 안 올라왔으면 비어 있다
 * @param serverSummary 서버 관측 요약. 아직 안 올라왔으면 비어 있다
 * @param observedLagTotal 관측 종료 시점의 Kafka 백로그
 */
public record BenchmarkRun(
        long id,
        String runKey,
        BenchmarkRunType runType,
        String scenarioCode,
        EngineVersion engineVersion,
        ReleaseStage releaseStage,
        QueueMode queueMode,
        Long couponId,
        BenchmarkRunStatus runStatus,
        BenchmarkArchiveStatus archiveStatus,
        String archiveFailureReason,
        ConsistencyFinalStatus consistencyStatus,
        String consistencyFailureReason,
        Instant startedAt,
        Instant loadStoppedAt,
        Instant observationStoppedAt,
        Instant finalizedAt,
        String requestedBy,
        String loadStopReason,
        BenchmarkTopology topology,
        LoadProfile loadProfile,
        LoadToolMeta toolMeta,
        ClientLoadSummary clientSummary,
        ServerLoadSummary serverSummary,
        Long observedLagTotal
) {

    public BenchmarkRun {
        Objects.requireNonNull(runKey, "runKey");
        Objects.requireNonNull(runType, "runType");
        Objects.requireNonNull(scenarioCode, "scenarioCode");
        Objects.requireNonNull(engineVersion, "engineVersion");
        Objects.requireNonNull(releaseStage, "releaseStage");
        Objects.requireNonNull(queueMode, "queueMode");
        Objects.requireNonNull(runStatus, "runStatus");
        Objects.requireNonNull(archiveStatus, "archiveStatus");
        Objects.requireNonNull(consistencyStatus, "consistencyStatus");
        Objects.requireNonNull(startedAt, "startedAt");
        Objects.requireNonNull(requestedBy, "requestedBy");
        Objects.requireNonNull(topology, "topology");
        Objects.requireNonNull(loadProfile, "loadProfile");
        Objects.requireNonNull(toolMeta, "toolMeta");

        requireTimesMatchStatus(runStatus, loadStoppedAt, observationStoppedAt, finalizedAt);
        requireChronological(startedAt, loadStoppedAt, observationStoppedAt, finalizedAt);
        requireBacklogGate(runStatus, observedLagTotal);
        if (runStatus == BenchmarkRunStatus.FINALIZED && clientSummary == null) {
            throw new IllegalArgumentException("공식 요약 없이 회차를 확정할 수 없다: " + runKey);
        }
        if ((archiveStatus == BenchmarkArchiveStatus.FAILED) != (archiveFailureReason != null)) {
            throw new IllegalArgumentException(
                    "archive 실패 이유는 FAILED 일 때만, 그리고 FAILED 이면 반드시 있어야 한다: " + runKey);
        }
        boolean consistencyTerminated = consistencyStatus == ConsistencyFinalStatus.FAILED
                || consistencyStatus == ConsistencyFinalStatus.EXPIRED;
        if (consistencyTerminated != (consistencyFailureReason != null)) {
            throw new IllegalArgumentException(
                    "정합성 실패 이유는 FAILED·EXPIRED 일 때만, 그리고 그때는 반드시 있어야 한다: " + runKey);
        }
    }

    /**
     * v3 의 증거가 실제로 남았는지. 관측이 부하보다 <b>늦게</b> 끝나야 Kafka 백로그가 0 으로
     * 수렴하는 구간이 시계열에 들어간다 — 두 시각이 같으면 그 구간이 비어 있다는 뜻이다.
     *
     * <p>스키마는 두 시각이 같은 것을 허용한다(v1 · v2 는 사실상 동시에 끝난다). 그래서 이
     * 판정은 제약이 아니라 조회다 — v3 회차를 사람이 확인할 때 쓴다.
     *
     * @return 관측 종료가 부하 종료보다 늦으면 true
     */
    public boolean observationOutlastedLoad() {
        return loadStoppedAt != null && observationStoppedAt != null
                && observationStoppedAt.isAfter(loadStoppedAt);
    }

    /**
     * 이 회차를 v1↔v2↔v3 비교표에 넣어도 되는지. 워밍업이거나 도착률이 목표 아래로 떨어진
     * 회차는 서버 수치가 아니라 생성기 수치라 비교의 근거가 못 된다.
     *
     * @return 확정된 MAIN 회차이고 도착률을 지켰으면 true
     */
    public boolean comparable() {
        return runType == BenchmarkRunType.MAIN
                && runStatus == BenchmarkRunStatus.FINALIZED
                && clientSummary != null
                && clientSummary.arrivalRateHeld();
    }

    public Optional<ClientLoadSummary> client() {
        return Optional.ofNullable(clientSummary);
    }

    public Optional<ServerLoadSummary> server() {
        return Optional.ofNullable(serverSummary);
    }

    private static void requireTimesMatchStatus(BenchmarkRunStatus status, Instant loadStoppedAt,
                                                Instant observationStoppedAt, Instant finalizedAt) {
        boolean loadDone = status != BenchmarkRunStatus.RUNNING;
        boolean observed = status == BenchmarkRunStatus.OBSERVED || status == BenchmarkRunStatus.FINALIZED;
        boolean finalized = status == BenchmarkRunStatus.FINALIZED;
        if ((loadStoppedAt != null) != loadDone
                || (observationStoppedAt != null) != observed
                || (finalizedAt != null) != finalized) {
            throw new IllegalArgumentException("회차 상태와 시각이 어긋난다: " + status);
        }
    }

    private static void requireChronological(Instant startedAt, Instant loadStoppedAt,
                                             Instant observationStoppedAt, Instant finalizedAt) {
        // isBefore 로 비교한다 — 같은 순간은 허용이다. v1 · v2 는 부하 종료와 관측 종료가
        // 사실상 동시라, 순서를 엄격하게 잡으면 정상 회차가 거부된다.
        if (loadStoppedAt != null && loadStoppedAt.isBefore(startedAt)) {
            throw new IllegalArgumentException("loadStoppedAt 이 startedAt 보다 빠르다");
        }
        if (observationStoppedAt != null && loadStoppedAt != null
                && observationStoppedAt.isBefore(loadStoppedAt)) {
            throw new IllegalArgumentException("observationStoppedAt 이 loadStoppedAt 보다 빠르다");
        }
        if (finalizedAt != null && observationStoppedAt != null
                && finalizedAt.isBefore(observationStoppedAt)) {
            throw new IllegalArgumentException("finalizedAt 이 observationStoppedAt 보다 빠르다");
        }
    }

    private static void requireBacklogGate(BenchmarkRunStatus status, Long observedLagTotal) {
        boolean gated = status == BenchmarkRunStatus.OBSERVED || status == BenchmarkRunStatus.FINALIZED;
        if (gated && (observedLagTotal == null || observedLagTotal != 0L)) {
            throw new IllegalArgumentException(
                    "Kafka 백로그가 0 으로 확인되지 않은 회차는 관측 종료로 넘어갈 수 없다: " + observedLagTotal);
        }
        if (observedLagTotal != null && observedLagTotal < 0) {
            throw new IllegalArgumentException("observedLagTotal 은 0 이상이어야 한다: " + observedLagTotal);
        }
    }
}
