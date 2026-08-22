package com.kafkick.core.benchmark;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import com.kafkick.core.support.exception.BusinessException;

/**
 * 회차 저장소 포트. 어댑터는 {@code storage} 의 JDBC 구현이다.
 *
 * <h2>상태 전이는 조건부 UPDATE 한 문장이어야 한다</h2>
 *
 * 전이 메서드는 모두 <b>현재 상태를 WHERE 절에 넣고</b> 갱신 행 수를 돌려준다. "읽고 판단하고
 * 쓰기" 로 나누면 api 가 N 대라 두 인스턴스가 같은 순간에 같은 판단을 통과할 수 있고, 그때
 * 회차 하나가 두 번 닫힌다. core 는 {@code spring-tx} 를 의존하지 않으므로 여기서 트랜잭션으로
 * 묶을 수도 없다 — 한 문장으로 원자화하는 것이 유일한 방법이자 가장 단순한 방법이다.
 *
 * <p>{@code false} 는 "그 상태가 아니어서 아무 행도 안 바뀌었다" 는 뜻이다. 회차가 없어서인지
 * 상태가 달라서인지는 호출자가 다시 읽어 가른다.
 *
 * <h2>읽기와 쓰기의 풀이 다르다</h2>
 *
 * 조회는 관측 풀({@code @Qualifier("obs")})로, 전이·적재는 운영 풀로 나간다. 둘을 같은 빈으로
 * 받으면 read-only 풀에서 INSERT 가 터진다. 관측 풀은 SELECT 전용 계정 예정이라 나중에 더
 * 시끄럽게 깨지고, 그때는 원인이 이 인터페이스 밖에 있다. 어댑터가 이 분리를 지킨다.
 */
public interface BenchmarkRunRepository {

    /**
     * 새 회차를 {@code RUNNING} 으로 연다.
     *
     * @param command 회차 조건
     * @param startedAt 회차 시작 시각
     * @return 저장된 회차의 식별자
     * @throws BusinessException 같은 키의 회차가 이미 있거나({@link BenchmarkErrorCode#RUN_KEY_DUPLICATED})
     *         다른 회차가 진행 중인 경우({@link BenchmarkErrorCode#RUN_ALREADY_RUNNING}).
     *         둘 다 DB 의 유니크 키가 판정한다 — 앱에서 조회로 막으면 N 대가 같은 순간에 통과한다
     */
    long open(StartBenchmarkRunCommand command, Instant startedAt);

    Optional<BenchmarkRun> findById(long id);

    Optional<BenchmarkRun> findByRunKey(String runKey);

    /**
     * 진행 중인 회차. 정의상 하나뿐이다({@code uk_run_running} 이 지킨다).
     *
     * @return 진행 중인 회차; 없으면 빈 값
     */
    Optional<BenchmarkRun> findRunning();

    /**
     * 최근 회차부터 과거 방향으로 조회한다.
     *
     * @param limit 최대 건수
     * @return 시작 시각 내림차순 회차 목록
     */
    List<BenchmarkRun> findRecent(int limit);

    /**
     * {@code RUNNING} 인 회차만 {@code LOAD_STOPPED} 로 넘긴다.
     *
     * @param id 회차 식별자
     * @param loadStoppedAt 부하 종료 시각
     * @param reason 부하를 끊은 이유; 계획대로 끝났으면 null
     * @return 실제로 전이했으면 true
     */
    boolean markLoadStopped(long id, Instant loadStoppedAt, String reason);

    /**
     * {@code LOAD_STOPPED} 인 회차만 {@code OBSERVED} 로 넘긴다. 백로그가 0 이 아니면 호출하지 않는다 —
     * 값 자체는 0 만 들어오지만, DB 의 {@code ck_run_lag_gate} 가 마지막 방어로 한 번 더 본다.
     *
     * @param id 회차 식별자
     * @param observationStoppedAt 관측 종료 시각
     * @param observedLagTotal 그 시점의 Kafka 백로그
     * @return 실제로 전이했으면 true
     */
    boolean markObserved(long id, Instant observationStoppedAt, long observedLagTotal);

    /**
     * {@code OBSERVED} 이고 공식 요약이 있는 회차만 {@code FINALIZED} 로 넘긴다.
     *
     * @param id 회차 식별자
     * @param finalizedAt 확정 시각
     * @return 실제로 전이했으면 true
     */
    boolean markFinalized(long id, Instant finalizedAt);

    /**
     * 종단 공식 요약을 붙인다. 확정된 회차에는 붙지 않는다.
     *
     * @param id 회차 식별자
     * @param summary 종단 요약
     * @return 실제로 갱신했으면 true
     */
    boolean updateClientSummary(long id, ClientLoadSummary summary);

    /**
     * 서버 관측 요약을 붙인다. 확정된 회차에는 붙지 않는다.
     *
     * @param id 회차 식별자
     * @param summary 서버 요약
     * @return 실제로 갱신했으면 true
     */
    boolean updateServerSummary(long id, ServerLoadSummary summary);

    /**
     * archive 결과를 남긴다. 회차 상태를 보지 않는다 — archive 는 회차 수명주기와 독립이고,
     * 실패해도 회차는 완료다.
     *
     * @param id 회차 식별자
     * @param status archive 결과
     * @param failureReason 실패 이유; {@code FAILED} 가 아니면 null
     * @return 실제로 갱신했으면 true
     */
    boolean updateArchiveStatus(long id, BenchmarkArchiveStatus status, String failureReason);
}
