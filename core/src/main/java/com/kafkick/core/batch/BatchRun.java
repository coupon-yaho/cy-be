package com.kafkick.core.batch;

import java.time.LocalDateTime;

/**
 * 배치 실행 하나의 이력. Spring Batch 메타에서 읽는다.
 *
 * <p>세 잡(expire·verify·cleanup)이 공유하는 모양이라 검증 도메인에 두지 않는다.
 */
public record BatchRun(
        long executionId,
        String jobName,
        String status,
        String exitCode,
        String exitMessage,
        LocalDateTime startedAt,
        LocalDateTime finishedAt,
        Long readCount,
        Long writeCount
) {

    /**
     * exitMessage 를 가린다. record 의 자동 toString 은 모든 컴포넌트를 찍는데, 이 값에는
     * 스택트레이스가 통째로 들어간다(실측 2,178자) — log.warn("... run={}", run) 한 줄이면
     * "detail 은 로그에만" 이 아니라 "detail 이 로그로" 가 된다. 응답으로 나가는 것은
     * FailureSummary 가 줄인 값뿐이다.
     */
    @Override
    public String toString() {
        return "BatchRun[executionId=" + executionId + ", jobName=" + jobName
                + ", status=" + status + ", exitCode=" + exitCode
                + ", exitMessage=<가림:" + (exitMessage == null ? 0 : exitMessage.length())
                + "자>, startedAt=" + startedAt + ", finishedAt=" + finishedAt
                + ", readCount=" + readCount + ", writeCount=" + writeCount + "]";
    }
}
