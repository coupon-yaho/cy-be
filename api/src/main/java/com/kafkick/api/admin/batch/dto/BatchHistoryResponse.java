// 배치 실행 이력 목록 응답입니다.
package com.kafkick.api.admin.batch.dto;

import java.time.Instant;
import java.util.List;

import com.kafkick.core.batch.BatchExecution;

/**
 * 배치 실행 이력 목록.
 *
 * <p><b>{@code status} 를 그대로 문자열로 내보낸다.</b> 값의 주인은 Spring Batch 이고, 우리가
 * 열거형으로 좁히면 프레임워크가 상태를 늘리는 날 이력 조회가 통째로 예외로 죽는다.
 *
 * <p><b>이 목록에 {@code @Scheduled} 배치는 나오지 않는다.</b> 원천이
 * {@code BATCH_JOB_EXECUTION} 이고 그것은 Spring Batch 가 남기는 것이다. 화면이 이 목록을
 * "모든 배치" 로 읽으면 안 된다 — 그래서 {@link #source} 로 무엇을 본 목록인지 함께 싣는다.
 *
 * @param source 이 목록의 원천. 지금은 항상 {@code BATCH_JOB_EXECUTION}
 * @param executions 최신 실행부터의 목록
 */
public record BatchHistoryResponse(String source, List<Execution> executions) {

    public static final String SOURCE_SPRING_BATCH = "BATCH_JOB_EXECUTION";

    public static BatchHistoryResponse of(List<BatchExecution> executions) {
        return new BatchHistoryResponse(SOURCE_SPRING_BATCH,
                executions.stream().map(Execution::of).toList());
    }

    /**
     * @param jobExecutionId 실행 식별자
     * @param jobName 잡 이름. 관제 {@code spring_batch_job_name} 라벨과 같은 문자열이다
     * @param status 실행 상태 문자열
     * @param exitCode 종료 코드
     * @param createdAt 실행 행이 만들어진 시각
     * @param startedAt 시작 시각. 시작하지 못했으면 {@code null}
     * @param endedAt 종료 시각. 도는 중이면 {@code null}
     */
    public record Execution(
            long jobExecutionId,
            String jobName,
            String status,
            String exitCode,
            Instant createdAt,
            Instant startedAt,
            Instant endedAt) {

        static Execution of(BatchExecution execution) {
            return new Execution(
                    execution.jobExecutionId(),
                    execution.jobName(),
                    execution.status(),
                    execution.exitCode(),
                    execution.createdAt(),
                    execution.startedAt(),
                    execution.endedAt());
        }
    }
}
