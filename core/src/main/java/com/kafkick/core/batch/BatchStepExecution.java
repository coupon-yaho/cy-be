// 배치 스텝 실행 한 건의 이력입니다.
package com.kafkick.core.batch;

import java.time.Instant;

/**
 * 스텝 실행 한 건. {@code BATCH_STEP_EXECUTION} 한 행에 대응한다.
 *
 * <h2>왜 이것이 "어떻게 돌았나" 의 정본인가</h2>
 *
 * <p>잡 수준 이력은 <b>돌았다/안 돌았다</b> 까지만 답한다. 몇 건 읽고 몇 건 쓰고 몇 건
 * 건너뛰었는지는 전부 이 표에 있고, <b>Spring Batch 가 이미 적고 있다</b> — 우리가 만들 것은
 * 데이터가 아니라 그것을 읽는 통로다.
 *
 * <p>카운터 여덟 개의 뜻은 <b>Spring Batch 가 정의한다.</b> 여기서 다시 해석하지 않고 원값을
 * 그대로 나른다 — 해석을 이 계층에 넣으면 프레임워크가 뜻을 바꾸는 날 <b>화면만 조용히
 * 틀린다.</b>
 *
 * <ul>
 *   <li>{@code readCount} — 읽은 항목 수</li>
 *   <li>{@code writeCount} — <b>쓰고 커밋된</b> 항목 수. 읽은 수와 다른 것이 정상이다</li>
 *   <li>{@code filterCount} — 처리기가 걸러 낸 수</li>
 *   <li>{@code commitCount} — 커밋 횟수. 청크 수와 같은 축이다</li>
 *   <li>{@code rollbackCount} — <b>재시도·스킵 복구로 인한 롤백까지 포함한다</b>(공식 문서
 *       명시). 0 이 아니라고 곧 사고인 것은 아니다</li>
 *   <li>{@code readSkipCount} · {@code processSkipCount} · {@code writeSkipCount} —
 *       어느 단계에서 건너뛰었는지. <b>셋을 합쳐 보여 주면 원인을 못 가른다</b></li>
 * </ul>
 *
 * <h2>비어 있을 수 있는 것들</h2>
 *
 * <p>{@code startedAt}·{@code endedAt} 은 {@link BatchExecution} 과 같은 이유로 {@code null}
 * 일 수 있다 — 만들어졌지만 시작 못 한 스텝, 도는 중인 스텝. <b>0 이나 생성 시각으로 메우지
 * 않는다</b>: 화면이 "즉시 끝났다" 로 읽는다.
 *
 * <p>{@code status} 를 열거형으로 두지 않는 이유도 {@link BatchExecution} 과 같다 — 값의
 * 주인이 프레임워크이고, 상태가 하나 늘면 <b>이력을 보러 온 사람에게 예외가 뜬다.</b>
 *
 * <p><b>{@code failure} 는 원문이 아니다.</b> {@code EXIT_MESSAGE} 에는 스택트레이스가 통째로
 * 들어가고 첫 줄에도 SQL 조각·제약 이름이 섞인다(공식 문서도 2,500자에서 잘린다고 적어 뒀다).
 * {@link FailureSummary} 가 줄인 값만 싣고, 자세한 원인은 서버 로그가 진다.
 *
 * @param stepExecutionId 스텝 실행 식별자
 * @param jobExecutionId 이 스텝이 속한 잡 실행
 * @param stepName 스텝 이름
 * @param status 실행 상태 문자열. 열거형으로 바꾸지 않는다 — 위 설명
 * @param exitCode 종료 코드
 * @param failure {@link FailureSummary} 가 줄인 실패 요약. 성공이면 원인이 없다는 문구가 온다
 * @param createdAt 스텝 실행 행이 만들어진 시각
 * @param startedAt 실제로 시작한 시각. 시작 못 했으면 {@code null}
 * @param endedAt 끝난 시각. 도는 중이면 {@code null}
 * @param readCount 읽은 항목 수
 * @param writeCount 쓰고 커밋된 항목 수
 * @param filterCount 걸러 낸 항목 수
 * @param commitCount 커밋 횟수
 * @param rollbackCount 롤백 횟수(재시도·스킵 복구 포함)
 * @param readSkipCount 읽기에서 건너뛴 수
 * @param processSkipCount 처리에서 건너뛴 수
 * @param writeSkipCount 쓰기에서 건너뛴 수
 */
public record BatchStepExecution(
        long stepExecutionId,
        long jobExecutionId,
        String stepName,
        String status,
        String exitCode,
        String failure,
        Instant createdAt,
        Instant startedAt,
        Instant endedAt,
        long readCount,
        long writeCount,
        long filterCount,
        long commitCount,
        long rollbackCount,
        long readSkipCount,
        long processSkipCount,
        long writeSkipCount) {
}
