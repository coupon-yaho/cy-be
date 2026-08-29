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
}
