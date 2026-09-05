// 배치 실행을 사람이 다시 돌리거나 멈출 때의 거절 사유입니다.
package com.kafkick.core.batch.exception;

import com.kafkick.core.support.exception.ErrorCode;

/**
 * 범용 배치 관제의 <b>쓰기 동작</b>이 거절하는 이유들.
 *
 * <h2>가드를 새로 짜지 않았다 — 프레임워크가 이미 거절한다</h2>
 *
 * <p>{@code JobOperator} 의 시그니처를 실측하니 위험한 전이를 <b>이미 전부 막고 있다</b>
 * (spring-batch-core 6.0.4 바이트코드).
 *
 * <pre>
 *   restart(long) throws JobInstanceAlreadyCompleteException, NoSuchJobExecutionException,
 *                        NoSuchJobException, JobRestartException, InvalidJobParametersException
 *   stop(long)    throws NoSuchJobExecutionException, JobExecutionNotRunningException
 * </pre>
 *
 * <p>그래서 이 코드들이 하는 일은 <b>같은 판정을 다시 내리는 것이 아니라</b>, 그 예외를
 * 사람이 읽을 수 있는 HTTP 응답으로 옮기는 것이다. 판정을 우리가 다시 쓰면 프레임워크가
 * 조건을 바꾸는 날 <b>둘이 갈리고, 우리 쪽이 틀린 답을 낸다.</b>
 *
 * <h2>왜 {@code core} 인가</h2>
 *
 * <p>{@code batch} 모듈에만 두면 나중에 관제 화면이 이 코드를 문자열로 옮겨 적게 된다.
 * 에러코드는 화면과의 계약이라 <b>양쪽이 같은 상수를 봐야</b> 한다 —
 * {@code DomainMeterNames} 가 같은 이유로 {@code core} 에 있다.
 */
public enum BatchControlErrorCode implements ErrorCode {

    /**
     * 그 실행이 없다.
     *
     * <p>실행을 꺼낼 때 {@code EmptyResultDataAccessException} 이 온다(실측 —
     * {@code JobRepository.getJobExecution} 은 {@code null} 이 아니라 던진다).
     * 목록에서 눌러 들어오는 경로라 흔하지 않지만, <b>정리 잡이 걷어간 뒤</b>에 눌렀을 때
     * 실재한다.
     */
    EXECUTION_NOT_FOUND(
            404,
            "BATCH-001",
            "해당 배치 실행을 찾을 수 없습니다."
    ),

    /**
     * 이미 성공으로 끝난 인스턴스라 다시 못 돌린다.
     *
     * <p>{@code JobInstanceAlreadyCompleteException} 이 온다. <b>이것이 재시작의 핵심
     * 안전장치다</b> — 성공한 실행을 다시 돌리면 같은 일이 두 번 처리된다.
     *
     * <p>같은 조건으로 또 돌리고 싶으면 재시작이 아니라 <b>다른 파라미터로 새 인스턴스</b>를
     * 만들어야 한다. 무엇이 인스턴스를 가르는지는 관제의 파라미터 화면이
     * {@code identifying} 으로 보여 준다(CY-911).
     */
    ALREADY_COMPLETED(
            409,
            "BATCH-002",
            "이미 성공으로 끝난 실행은 다시 시작할 수 없습니다."
    ),

    /**
     * 재시작 자체가 거부됐다.
     *
     * <p>{@code JobRestartException} 이 온다 — 잡이 재시작을 허용하지 않게 정의됐거나
     * ({@code preventRestart}), 재시작할 수 없는 상태다.
     */
    RESTART_REFUSED(
            409,
            "BATCH-003",
            "이 잡은 다시 시작할 수 없습니다."
    ),

    /**
     * 돌고 있지 않은 실행을 멈추려 했다.
     *
     * <p>{@code JobExecutionNotRunningException} 이 온다.
     *
     * <p><b>{@code STARTED} 시체는 여기로 안 온다.</b> 한때 그렇게 적어 뒀는데 <b>틀렸다</b> —
     * {@code BatchStatus.STARTED.isRunning()} 이 참이라 {@code JobOperator} 가 거절하지 않고
     * 신호를 받아들인다(실측). 그 신호를 읽을 프로세스가 없어 영영 안 멈출 뿐이다.
     * 이 코드가 뜨는 것은 <b>이미 끝난</b>({@code COMPLETED}·{@code FAILED}) 실행을 멈추려
     * 할 때다.
     */
    NOT_RUNNING(
            409,
            "BATCH-004",
            "돌고 있지 않은 실행은 멈출 수 없습니다."
    ),

    /**
     * 잡 정의를 못 찾는다.
     *
     * <p>{@code NoSuchJobException} 이 온다. <b>실행 이력은 남았는데 그 잡이 이제 없는
     * 경우</b>다 — 잡 이름을 바꾸거나 없앤 뒤 옛 실행을 재시작하면 여기로 온다.
     * 이 관제가 범용이라 <b>모르는 잡의 이력도 보여 주기 때문에</b> 실재하는 경로다.
     */
    JOB_NOT_FOUND(
            409,
            "BATCH-005",
            "그 실행의 잡 정의를 찾을 수 없습니다."
    );

    private final int status;
    private final String code;
    private final String message;

    BatchControlErrorCode(int status, String code, String message) {
        this.status = status;
        this.code = code;
        this.message = message;
    }

    @Override
    public int getStatus() {
        return status;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
