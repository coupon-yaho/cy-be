package com.kafkick.core.benchmark;

import com.kafkick.core.support.exception.ErrorCode;

/**
 * 회차 운영 명령이 거부되는 이유. 전부 사람이 누른 명령에 대한 응답이라 원인을 구분해서 낸다 —
 * 하나로 뭉치면 "왜 안 되는지" 를 알 방법이 회차 중에 없다.
 */
public enum BenchmarkErrorCode implements ErrorCode {

    /**
     * 같은 회차 키로 두 번 열었다. start 를 두 번 누른 경우가 대부분이라 첫 회차는 그대로 살아 있다.
     */
    RUN_KEY_DUPLICATED(409, "BENCHMARK-001", "이미 같은 키의 측정 회차가 있습니다."),

    /**
     * 다른 회차가 아직 돌고 있다. 두 회차가 동시에 돌면 이벤트의 {@code benchmark_run_id} 가
     * 섞이고, <b>섞인 회차는 사후에 분리할 방법이 없다.</b>
     */
    RUN_ALREADY_RUNNING(409, "BENCHMARK-002", "이미 진행 중인 측정 회차가 있습니다."),

    RUN_NOT_FOUND(404, "BENCHMARK-003", "측정 회차를 찾을 수 없습니다."),

    /** 건너뛰거나 되돌리는 전이를 요청했다. */
    ILLEGAL_TRANSITION(409, "BENCHMARK-004", "현재 상태에서 요청한 전이를 할 수 없습니다."),

    /**
     * Kafka 백로그가 아직 0 이 아니다. 수렴 전에 회차를 닫으면 v3 의 적재가 잘린 채 공식 결과가 된다.
     */
    BACKLOG_NOT_DRAINED(409, "BENCHMARK-005", "Kafka 백로그가 0 으로 수렴하지 않았습니다."),

    /** 공식 요약 없이 확정하려 했다. 확정된 회차의 수치가 곧 비교표의 값이다. */
    OFFICIAL_SUMMARY_MISSING(409, "BENCHMARK-006", "공식 결과가 아직 등록되지 않았습니다."),

    /**
     * 확정된 회차의 요약을 고치려 했다. 고치면 이미 그 수치로 내린 판단과 어긋나는데,
     * 어긋났다는 사실이 어디에도 안 남는다. 다시 재려면 새 회차다.
     */
    SUMMARY_LOCKED(409, "BENCHMARK-007", "확정된 회차의 결과는 변경할 수 없습니다."),

    /**
     * 회차 조건·요약 값이 규칙을 벗어났다. 사람이 넣은 값이라 400 이다.
     *
     * <p>{@code IllegalArgumentException} 으로 두면 {@code GlobalExceptionHandler} 의
     * {@code @ExceptionHandler(Exception.class)} 가 500 으로 뭉갠다 — 값을 고쳐서 다시 보내면
     * 되는 상황을 서버 장애로 알리게 된다.
     */
    INVALID_RUN_CONDITION(400, "BENCHMARK-008", "측정 회차 조건이 올바르지 않습니다."),

    /**
     * 저장소가 회차 행을 거부했다. 서비스와 값 객체를 지나고도 DB CHECK 에 걸린 경우다.
     *
     * <p>도달 경로가 실재한다 — 회차를 연 인스턴스와 부하를 끊는 인스턴스가 다르면 <b>시계 차이</b>로
     * {@code load_stopped_at < started_at} 이 만들어져 {@code ck_run_time_order} 에 걸린다.
     * 번역하지 않으면 회차 도중에 원인이 안 적힌 500 이 뜬다.
     */
    RUN_VALUE_REJECTED(409, "BENCHMARK-009", "저장소가 측정 회차 값을 거부했습니다.");

    private final int status;
    private final String code;
    private final String message;

    BenchmarkErrorCode(int status, String code, String message) {
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
