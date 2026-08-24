// 배치 실행 한 건의 이력입니다.
package com.kafkick.core.batch;

import java.time.Instant;

/**
 * 배치 실행 한 건. {@code BATCH_JOB_EXECUTION} 한 행에 대응한다.
 *
 * <p><b>{@code status} 를 열거형으로 두지 않는다.</b> 값의 주인이 Spring Batch 의
 * {@code BatchStatus} 인데 core 는 그 의존을 갖지 않는다. 여기서 같은 이름의 열거형을 만들면
 * 프레임워크가 상태를 하나 늘리는 날 <b>조회가 통째로 예외로 죽는다</b> — 이력을 보러 온
 * 사람에게 가장 나쁜 실패다. 문자열로 그대로 실어 보내고, 뜻을 아는 쪽(화면)이 해석한다.
 *
 * <p><b>{@code STARTED} 는 "돌고 있다" 가 아니라 "끝났다는 기록이 없다" 는 뜻이다.</b>
 * 배치 JVM 이 SIGKILL·OOM·재배포로 죽으면 {@code afterJob} 이 불리지 않아 그 행이
 * {@code STATUS='STARTED', END_TIME=NULL} 로 <b>영원히 남는다.</b> 그 잔여 행을 정리하는
 * 코드는 이 저장소에 <b>없다</b>(실측: 그 상태에서 같은 JobInstance 를 다시 시작하면
 * {@code JobExecutionAlreadyRunningException} 으로 영구 거부된다. 다른 파라미터로 만들어지는
 * 다른 인스턴스는 영향을 받지 않는다).
 *
 * <p>그래서 화면은 이 상태를 <b>"돌고 있음" 으로 단정해서는 안 된다.</b> 오래된
 * {@code createdAt} 을 가진 {@code STARTED} 는 죽은 실행일 가능성이 높다.
 * TODO(후속 티켓 미정): 기동 시 잔여 {@code STARTED} 를 {@code ABANDONED} 로 내린다.
 *   전역 정리는 다중 노드에서 <b>다른 노드가 지금 돌리는 실행</b>까지 죽이므로,
 *   잡 시작 시 노드 식별자를 기록하는 작업이 선행되어야 한다(잡 실행 진입점 = CY-15 소유).
 *
 * <p><b>{@code startedAt} 이 {@code null} 일 수 있다.</b> {@code START_TIME} 은 nullable 이고,
 * 실행이 만들어졌지만 시작 못 한 상태가 실재한다. {@code endedAt} 도 마찬가지로 도는 중이면
 * 비어 있다. 둘을 0 이나 생성 시각으로 메우지 않는다 — 화면이 "즉시 끝났다" 로 읽는다.
 *
 * @param jobExecutionId 실행 식별자
 * @param jobName 잡 이름. 관제의 {@code spring_batch_job_name} 라벨과 같은 문자열이다
 * @param status Spring Batch 의 실행 상태 문자열(COMPLETED · FAILED · STOPPED 등).
 *        <b>{@code STARTED} 를 "지금 돌고 있다" 로 읽으면 안 된다</b> — 아래 설명
 * @param exitCode 종료 코드. 상태보다 세분화된 값이 들어온다
 * @param createdAt 실행 행이 만들어진 시각
 * @param startedAt 실제로 시작한 시각. 시작 못 했으면 {@code null}
 * @param endedAt 끝난 시각. 도는 중이면 {@code null}
 */
public record BatchExecution(
        long jobExecutionId,
        String jobName,
        String status,
        String exitCode,
        Instant createdAt,
        Instant startedAt,
        Instant endedAt) {
}
