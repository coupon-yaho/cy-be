package com.kafkick.core.benchmark;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;

/**
 * 측정 회차의 start · stop · finalize 를 조정한다.
 *
 * <p>이 서비스가 지키는 것은 둘이다.
 *
 * <ol>
 *   <li><b>회차는 동시에 하나만 돈다.</b> 두 회차가 겹치면 이벤트의 {@code benchmark_run_id} 가
 *       섞이고, 섞인 회차는 사후에 분리할 방법이 없다. 판정은 DB 의 유니크 키가 한다 — api 가
 *       N 대라 조회로 막으면 세 인스턴스가 같은 순간에 통과할 수 있다.
 *   <li><b>Kafka 백로그가 0 으로 확인된 회차만 관측 종료로 넘어간다.</b> 수렴 전에 닫으면 v3 의
 *       적재가 잘린 채 공식 결과가 된다.
 * </ol>
 *
 * <p>전이는 전부 조건부 UPDATE 한 문장이다. {@code core} 는 {@code spring-tx} 를 의존하지 않아
 * 트랜잭션으로 묶을 수 없고, 묶는다 해도 "읽고 판단하고 쓰기" 는 인스턴스가 여럿이면 같은 창을
 * 남긴다. 갱신 행 수가 0 이면 그때 다시 읽어 <b>왜</b> 안 됐는지를 가른다.
 *
 * <h2>빈 등록은 이 모듈이 하지 않는다</h2>
 *
 * {@code @Service} 를 붙이지 않는다. core 는 batch 에도 얹히는데, 거기에는
 * {@link BenchmarkRunRepository} 구현이 없다(관측 풀을 안 켜면 어댑터 자체가 안 만들어진다).
 * 스캔 대상으로 두면 회차와 무관한 프로세스가 의존성을 못 찾아 기동에서 죽는다 — 관측 배선이
 * 관측과 무관한 프로세스를 멈춰 세우는, 이 저장소가 이미 두 번 밟은 함정이다.
 * 회차 API 를 노출하는 모듈이 이 빈을 직접 등록한다.
 *
 * <h2>백로그 값의 출처</h2>
 *
 * 지금은 관측 종료를 요청하는 쪽이 잰 값을 받아 그대로 기록한다. Kafka lag 미터가 아직 없기
 * 때문이다({@code DomainMeterNames} 의 OBS-17 후속 TODO — AdminClient 배선이 필요하다).
 * <b>없는 원천을 조회하는 게이트를 만들지 않는다</b> — 값이 영원히 비어서 finalize 가 영구히
 * 막히는데, 겉보기에는 게이트가 도는 것처럼 보인다. 그 미터가 열리면 이 인자를 미터 조회로
 * 바꾸고, 그때 값이 0 인지의 판정은 여기 그대로 남는다.
 */
public class BenchmarkRunService {

    private final BenchmarkRunRepository repository;
    private final TimeProvider timeProvider;

    public BenchmarkRunService(BenchmarkRunRepository repository, TimeProvider timeProvider) {
        this.repository = Objects.requireNonNull(repository);
        this.timeProvider = Objects.requireNonNull(timeProvider);
    }

    /**
     * 회차를 연다.
     *
     * @param command 회차 조건
     * @return 열린 회차
     * @throws BusinessException 같은 키의 회차가 있거나 다른 회차가 진행 중인 경우
     */
    public BenchmarkRun start(StartBenchmarkRunCommand command) {
        Objects.requireNonNull(command, "command");
        long id = repository.open(command, timeProvider.instant());
        return reload(id);
    }

    /**
     * 부하를 끊는다. 관측은 계속된다 — 여기서 회차를 닫으면 v3 의 수렴 구간이 통째로 빠진다.
     *
     * @param id 회차 식별자
     * @param reason 부하를 끊은 이유; 계획대로 끝났으면 null
     * @return 부하가 끝난 회차
     * @throws BusinessException 회차가 없거나 진행 중이 아닌 경우
     */
    public BenchmarkRun stopLoad(long id, String reason) {
        if (!repository.markLoadStopped(id, timeProvider.instant(), reason)) {
            throw transitionFailure(id, BenchmarkRunStatus.RUNNING);
        }
        return reload(id);
    }

    /**
     * 관측을 끊는다. <b>Kafka 백로그가 0 일 때만 넘어간다.</b>
     *
     * @param id 회차 식별자
     * @param observedLagTotal 관측 종료 시점에 잰 Kafka 백로그
     * @return 관측이 끝난 회차
     * @throws BusinessException 백로그가 0 이 아니거나, 회차가 없거나, 부하가 아직 안 끝난 경우
     */
    public BenchmarkRun stopObservation(long id, long observedLagTotal) {
        if (observedLagTotal < 0) {
            throw new IllegalArgumentException("observedLagTotal 은 0 이상이어야 한다: " + observedLagTotal);
        }
        if (observedLagTotal != 0) {
            throw new BusinessException(BenchmarkErrorCode.BACKLOG_NOT_DRAINED,
                    "benchmarkRunId=" + id + " backlog=" + observedLagTotal);
        }
        if (!repository.markObserved(id, timeProvider.instant(), observedLagTotal)) {
            throw transitionFailure(id, BenchmarkRunStatus.LOAD_STOPPED);
        }
        return reload(id);
    }

    /**
     * 종단 공식 요약을 붙인다. 도착률이 목표 아래로 떨어진 회차도 <b>거부하지 않고 기록한다</b> —
     * 그 사실 자체가 회차의 결과이고, 버릴지는 비교표를 만드는 사람이 정한다
     * ({@link BenchmarkRun#comparable()}).
     *
     * @param id 회차 식별자
     * @param summary 부하 생성기 원본 표본으로 계산한 요약
     * @return 갱신된 회차
     * @throws BusinessException 회차가 없거나 이미 확정된 경우
     */
    public BenchmarkRun recordClientSummary(long id, ClientLoadSummary summary) {
        Objects.requireNonNull(summary, "summary");
        if (!repository.updateClientSummary(id, summary)) {
            throw summaryFailure(id);
        }
        return reload(id);
    }

    /**
     * 서버 관측 요약을 붙인다.
     *
     * @param id 회차 식별자
     * @param summary 서버 요약
     * @return 갱신된 회차
     * @throws BusinessException 회차가 없거나 이미 확정된 경우
     */
    public BenchmarkRun recordServerSummary(long id, ServerLoadSummary summary) {
        Objects.requireNonNull(summary, "summary");
        if (!repository.updateServerSummary(id, summary)) {
            throw summaryFailure(id);
        }
        return reload(id);
    }

    /**
     * 회차를 확정한다. 관측이 끝났고 공식 요약이 붙은 회차만 넘어간다.
     *
     * @param id 회차 식별자
     * @return 확정된 회차
     * @throws BusinessException 회차가 없거나, 관측이 안 끝났거나, 공식 요약이 없는 경우
     */
    public BenchmarkRun finalizeRun(long id) {
        if (repository.markFinalized(id, timeProvider.instant())) {
            return reload(id);
        }
        BenchmarkRun run = reload(id);
        if (run.runStatus() != BenchmarkRunStatus.OBSERVED) {
            throw illegalTransition(id, run.runStatus(), BenchmarkRunStatus.OBSERVED);
        }
        // OBSERVED 인데 못 넘어갔다면 남은 이유는 공식 요약이 없다는 것뿐이다.
        throw new BusinessException(BenchmarkErrorCode.OFFICIAL_SUMMARY_MISSING, "benchmarkRunId=" + id);
    }

    /**
     * archive 결과를 남긴다. <b>회차 상태를 바꾸지 않는다</b> — archive 가 실패해도 회차는 완료이고,
     * 나중에 재실행할 수 있어야 한다.
     *
     * @param id 회차 식별자
     * @param status archive 결과
     * @param failureReason 실패 이유; {@code FAILED} 일 때만
     * @return 갱신된 회차
     * @throws BusinessException 회차가 없는 경우
     */
    public BenchmarkRun recordArchiveResult(long id, BenchmarkArchiveStatus status, String failureReason) {
        Objects.requireNonNull(status, "status");
        if ((status == BenchmarkArchiveStatus.FAILED) != (failureReason != null)) {
            throw new IllegalArgumentException(
                    "archive 실패 이유는 FAILED 일 때만, 그리고 FAILED 이면 반드시 있어야 한다: " + status);
        }
        if (!repository.updateArchiveStatus(id, status, failureReason)) {
            throw notFound(id);
        }
        return reload(id);
    }

    public BenchmarkRun get(long id) {
        return reload(id);
    }

    /**
     * 진행 중인 회차. batch 의 관측 대상 회차 선택이 이 값을 본다 — 없으면 관측만 도는 상태다.
     *
     * @return 진행 중인 회차; 없으면 빈 값
     */
    public Optional<BenchmarkRun> running() {
        return repository.findRunning();
    }

    public List<BenchmarkRun> recent(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit 은 0 보다 커야 한다: " + limit);
        }
        return repository.findRecent(limit);
    }

    private BenchmarkRun reload(long id) {
        return repository.findById(id).orElseThrow(() -> notFound(id));
    }

    /**
     * 전이가 0 행을 갱신한 이유를 가른다. 회차가 없는 것과 상태가 다른 것은 다른 오류다 —
     * 하나로 뭉치면 회차 중에 "왜 안 되는지" 를 알 방법이 없다.
     */
    private BusinessException transitionFailure(long id, BenchmarkRunStatus expected) {
        BenchmarkRun run = repository.findById(id).orElse(null);
        if (run == null) {
            return notFound(id);
        }
        return illegalTransition(id, run.runStatus(), expected);
    }

    private BusinessException summaryFailure(long id) {
        BenchmarkRun run = repository.findById(id).orElse(null);
        if (run == null) {
            return notFound(id);
        }
        return new BusinessException(BenchmarkErrorCode.SUMMARY_LOCKED,
                "benchmarkRunId=" + id + " status=" + run.runStatus());
    }

    private static BusinessException illegalTransition(long id, BenchmarkRunStatus actual,
                                                       BenchmarkRunStatus expected) {
        return new BusinessException(BenchmarkErrorCode.ILLEGAL_TRANSITION,
                "benchmarkRunId=" + id + " status=" + actual + " expected=" + expected);
    }

    private static BusinessException notFound(long id) {
        return new BusinessException(BenchmarkErrorCode.RUN_NOT_FOUND, "benchmarkRunId=" + id);
    }
}
