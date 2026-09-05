// 배치 실행 이력 조회 포트입니다.
package com.kafkick.core.batch;

import java.util.List;

/**
 * 배치 실행 이력 포트. 어댑터는 {@code storage} 의 JDBC 구현이다.
 *
 * <h2>읽기 전용이다</h2>
 *
 * 이 포트에는 쓰기가 없고 앞으로도 없어야 한다. {@code BATCH_JOB_EXECUTION} 을 쓰는 주체는
 * Spring Batch 의 {@code JobRepository} 하나여야 한다 — 우리가 같은 테이블에 끼어들면
 * 그쪽의 낙관적 락({@code VERSION} 컬럼)과 중복 방지가 무너진다.
 *
 * <p>그래서 어댑터는 <b>관측 풀</b>({@code @Qualifier("obs")})로만 읽는다. 부하 회차 중에
 * 이력 조회가 운영 풀의 커넥션을 물면 그것이 곧 측정 오염이다. 관측 계정이 SELECT 전용이라
 * 이 포트에 쓰기가 생기는 날 <b>런타임에 시끄럽게 깨진다</b> — 그것이 의도한 방어선이다.
 *
 * <h2>이 목록에는 상한이 있다 — 그리고 예외가 하나 있다</h2>
 *
 * <p>{@code BATCH_JOB_EXECUTION} 은 무한히 자라지 않는다. 정리 잡이 보존 기간 밖을 걷는다
 * ({@code feature/CY-15} 의 {@code CleanupJobConfig}). 설정은
 * {@code batch.cleanup.metadata-keep-days}(환경변수 {@code CLEANUP_METADATA_KEEP_DAYS}),
 * 기본 30일이고 <b>하한 8 · 상한 365</b> 다.
 *
 * <p>하한이 7 이 아니라 8 인 이유가 등호 하나 차이가 아니다 — 지표 되읽기가
 * {@code END_TIME > NOW() - INTERVAL 7 DAY} 창을 보므로, 보존이 그 창보다 <b>엄격히</b> 길어야
 * 삭제가 창 안의 행을 건드릴 수 없다. 같으면 잡이 하루만 실패해도 마지막 성공이 컷오프 위에 놓인다.
 *
 * <p><b>⚠️ 끝나지 않은 실행은 보존 대상이 아니다.</b> 삭제 술어가 {@code END_TIME} 이
 * {@code NULL} 인 행을 제외한다 — 도는 중이거나 <b>종료 표시를 못 남기고 죽은</b> 행이다.
 * 지우면 시체를 감시하는 알림이 조용해지는데 그건 고친 것이 아니라 증거를 지운 것이다.
 *
 * <p>그래서 <b>화면 최상단에 아주 오래된 {@code STARTED} 항목이 올라올 수 있고, 그것은 버그가
 * 아니라 신호다.</b> 그 행은 사람이 복구 API 로 닫는다(CY-429 의
 * {@code POST /api/v1/admin/expire/runs/{id}/recover}). 걷어낸 실행은 {@code ABANDONED} 가
 * 아니라 <b>{@code FAILED}</b> 로 닫히고 {@code EXIT_CODE} 는 {@code UNKNOWN} 이다 —
 * {@code ABANDONED} 는 {@code COMPLETED} 와 같은 취급이라 그 {@code JobInstance} 를
 * 다시 못 돌리기 때문이다.
 *
 * <p><b>{@link #findRecentByJobName} 도 같은 상한을 받는다.</b> 보존 삭제는 잡 이름을 가리지
 * 않으므로 잡별 조회의 과거 깊이도 전체와 같다.
 *
 * <p>⚠️ <b>이 브랜치에서는 아직 아무것도 삭제되지 않는다.</b> 정리 잡이 {@code feature/CY-15}
 * 소유이고 여기에는 없다. 위 상한은 <b>합류 뒤에 생긴다.</b>
 *
 * <h2>{@code @Scheduled} 배치는 여기 없다</h2>
 *
 * {@code BATCH_JOB_EXECUTION} 은 Spring Batch 가 남기는 것이라, 잡을 끼지 않는
 * {@code @Scheduled} 배치는 <b>이 목록에 한 줄도 안 나온다.</b>
 *
 * <p>다만 확인한 범위에서 <b>그런 배치는 없다</b> — {@code feature/CY-15} 의
 * {@code ExpireScheduler} · {@code CleanupScheduler} 는 {@code @Scheduled} 지만 본문이
 * {@code JobOperator.start(...)} 라 실제 작업은 Spring Batch 잡이 한다. 즉 이 목록이
 * 지금 설계에서는 <b>모든 업무 배치를 덮는다.</b> 잡을 안 끼는 배치가 생기면 그때 원천을
 * 다시 정해야 한다.
 */
public interface BatchExecutionRepository {

    /**
     * 최근 실행을 새 것부터 조회한다.
     *
     * @param limit 최대 건수
     * @return 실행 목록. 없으면 빈 목록
     */
    List<BatchExecution> findRecent(int limit);

    /**
     * 특정 잡의 최근 실행을 새 것부터 조회한다.
     *
     * @param jobName 잡 이름
     * @param limit 최대 건수
     * @return 실행 목록. 그 이름의 잡이 없거나 한 번도 안 돌았으면 빈 목록
     */
    List<BatchExecution> findRecentByJobName(String jobName, int limit);

    /**
     * 한 실행의 스텝들을 <b>실행 순서대로</b> 조회한다.
     *
     * <p><b>목록에 끼워 넣지 않고 따로 받는 이유</b> — 실행 N 건마다 스텝을 조회하면 목록이
     * N+1 이 되고, 한 번에 조인해도 실행당 스텝 수만큼 행이 불어난다. 사람이 실제로 파고드는
     * 것은 <b>한 건</b>이라, 정본(Spring Cloud Data Flow 대시보드)도 목록 → 상세 → 스텝
     * 으로 단계를 나눈다.
     *
     * <p><b>정렬은 {@code STEP_EXECUTION_ID} 다.</b> 시작 시각은 nullable 이고(시작 못 한
     * 스텝), 병렬 스텝이면 같은 시각이 여럿 나온다 — 그때 순서가 흔들리면 <b>같은 실행을
     * 두 번 열었을 때 화면이 달라진다.</b>
     *
     * @param jobExecutionId 잡 실행 식별자
     * @return 스텝 목록. 그 실행이 없거나 스텝이 하나도 안 돌았으면 빈 목록 —
     *         <b>실행이 없는 것과 스텝이 없는 것을 여기서 가르지 않는다.</b> 가르려면
     *         실행을 한 번 더 조회해야 하는데, 그 왕복의 값어치가 이 화면에는 없다
     */
    List<BatchStepExecution> findSteps(long jobExecutionId);
}
