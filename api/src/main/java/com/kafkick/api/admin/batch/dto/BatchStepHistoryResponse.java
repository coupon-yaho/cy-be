// 배치 스텝 실행 이력 응답입니다.
package com.kafkick.api.admin.batch.dto;

import java.time.Instant;
import java.util.List;

import com.kafkick.core.batch.BatchStepExecution;

/**
 * 한 실행의 스텝 이력. <b>"어떻게 돌았나" 에 답하는 화면이다.</b>
 *
 * <p>잡 수준 목록은 <b>돌았다/안 돌았다</b> 까지만 답한다. 몇 건 읽고 몇 건 건너뛰고 몇 번
 * 롤백했는지는 여기 있고, 그 값은 <b>Spring Batch 가 이미 적어 둔 것</b>이다.
 *
 * <p><b>카운터를 합치지 않는다.</b> 스킵 셋(read·process·write)을 하나로 더해 보여 주고
 * 싶어지는데, 그러면 <b>어느 단계에서 새는지</b>를 화면이 못 가른다 — 이 화면을 여는 이유가
 * 정확히 그것이다.
 *
 * <p><b>{@link #source} 를 싣는 이유</b> — 목록 응답과 같다. 이것이 "모든 배치" 가 아니라
 * Spring Batch 메타 한 표를 본 결과라는 것을 화면이 알아야 한다.
 *
 * @param source 이 목록의 원천. 지금은 항상 {@code BATCH_STEP_EXECUTION}
 * @param jobExecutionId 어느 실행의 스텝인지
 * @param steps 실행 순서대로의 스텝 목록. 빈 목록일 수 있다 — <b>실행이 없는 것과 스텝이
 *        아직 없는 것을 여기서 가르지 않는다</b>(포트 javadoc 에 근거를 적었다)
 */
public record BatchStepHistoryResponse(String source, long jobExecutionId, List<Step> steps) {

    public static final String SOURCE_SPRING_BATCH = "BATCH_STEP_EXECUTION";

    public static BatchStepHistoryResponse of(long jobExecutionId, List<BatchStepExecution> steps) {
        return new BatchStepHistoryResponse(SOURCE_SPRING_BATCH, jobExecutionId,
                steps.stream().map(Step::of).toList());
    }

    /**
     * @param stepExecutionId 스텝 실행 식별자
     * @param stepName 스텝 이름
     * @param status 실행 상태 문자열. 열거형으로 좁히지 않는다 — 값의 주인이 프레임워크다
     * @param exitCode 종료 코드
     * @param failure 실패 요약. <b>원문이 아니다</b> — 도메인 에러코드나 예외 클래스 이름만
     *        남는다. 성공한 스텝에도 "원인이 기록되지 않았습니다" 가 온다
     * @param createdAt 스텝 실행 행이 만들어진 시각
     * @param startedAt 시작 시각. 시작하지 못했으면 {@code null}
     * @param endedAt 종료 시각. 도는 중이면 {@code null}
     * @param readCount 읽은 항목 수
     * @param writeCount <b>쓰고 커밋된</b> 항목 수. 읽은 수와 다른 것이 정상이다
     * @param filterCount 처리기가 걸러 낸 수
     * @param commitCount 커밋 횟수
     * @param rollbackCount 롤백 횟수. <b>재시도·스킵 복구로 인한 것을 포함한다</b> —
     *        0 이 아니라고 곧 사고인 것은 아니다
     * @param readSkipCount 읽기에서 건너뛴 수
     * @param processSkipCount 처리에서 건너뛴 수
     * @param writeSkipCount 쓰기에서 건너뛴 수
     */
    public record Step(
            long stepExecutionId,
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

        static Step of(BatchStepExecution step) {
            return new Step(
                    step.stepExecutionId(),
                    step.stepName(),
                    step.status(),
                    step.exitCode(),
                    step.failure(),
                    step.createdAt(),
                    step.startedAt(),
                    step.endedAt(),
                    step.readCount(),
                    step.writeCount(),
                    step.filterCount(),
                    step.commitCount(),
                    step.rollbackCount(),
                    step.readSkipCount(),
                    step.processSkipCount(),
                    step.writeSkipCount());
        }
    }
}
