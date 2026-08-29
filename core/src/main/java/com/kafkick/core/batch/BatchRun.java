package com.kafkick.core.batch;

import java.time.LocalDateTime;

/**
 * 배치 실행 하나의 이력. Spring Batch 메타에서 읽는다.
 *
 * <p>세 잡(expire·verify·cleanup)이 공유하는 모양이라 검증 도메인에 두지 않는다.
 *
 * <p><b>시각 필드 이름에 축을 박은 이유.</b> 스프링 배치는 인자 없는
 * {@code LocalDateTime.now()} 로 찍으므로 이 둘은 <b>JVM 기본 존 벽시계</b>다. 반면 옆의
 * {@code VerificationRun.startedAt} 은 {@code TimeProvider} 가 준 <b>UTC</b> 다 — 이름이
 * 같으면 배치 이력과 검증 이력을 한 화면에 올릴 때 두 값을 나란히 빼는 코드가 나오는데,
 * 그 오차는 <b>배포가 UTC 라 로컬에서만</b> 드러난다. 이름이 다르면 그 자리에서 멈춘다.
 * 변환은 {@code BatchTimeAxis.onDomainAxis} 하나뿐이고 {@code BatchRunView} 가 거친다.
 */
public record BatchRun(
        long executionId,
        String jobName,
        String status,
        String exitCode,
        String exitMessage,
        LocalDateTime startedAtInBatchMetaZone,
        LocalDateTime finishedAtInBatchMetaZone,
        Long stepReadTotal,
        Long stepWriteTotal
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
                + "자>, startedAtInBatchMetaZone=" + startedAtInBatchMetaZone
                + ", finishedAtInBatchMetaZone=" + finishedAtInBatchMetaZone
                + ", stepReadTotal=" + stepReadTotal + ", stepWriteTotal=" + stepWriteTotal + "]";
    }
}
