// 검증 배치의 도메인 에러입니다. core.support.ErrorCode 규약(<도메인>-<3자리>)을 따릅니다.
package com.kafkick.core.verification.exception;

import com.kafkick.core.support.exception.ErrorCode;

/**
 * 배치는 HTTP 서버가 아니지만 관리 포트로 verify 트리거 API 를 열기 때문에 status 를 채웁니다.
 * 배치 내부에서만 발생하는 오류는 500 을 씁니다.
 */
public enum VerificationErrorCode implements ErrorCode {

    INVALID_AS_OF(
            400,
            "VERIFICATION-001",
            "검증 기준 시각이 올바르지 않습니다."
    ),
    INVALID_RUN_PARAMS(
            400,
            "VERIFICATION-002",
            "검증 실행 파라미터가 올바르지 않습니다."
    ),

    /** 조회용 400대다. 배치 내부 경로에서는 {@link #RUN_ROW_VANISHED} 를 쓴다 — verify 트리거 API 몫이다. */
    RUN_NOT_FOUND(
            404,
            "VERIFICATION-003",
            "검증 실행을 찾을 수 없습니다."
    ),
    ASOF_STATE_MISSING(
            500,
            "VERIFICATION-004",
            "asOf 시점 상태가 준비되지 않았습니다."
    ),
    ILLEGAL_REPLAY_STATE(
            500,
            "VERIFICATION-005",
            "이력 리플레이 중 처리할 수 없는 상태를 만났습니다."
    ),
    DATASET_MUTATED_DURING_RUN(
            500,
            "VERIFICATION-006",
            "검증 실행 중에 대상 데이터가 바뀌어 이 실행의 검출을 신뢰할 수 없습니다."
    ),
    MANIFEST_ABSENT(
            500,
            "VERIFICATION-007",
            "오염셋 정답 매니페스트가 없습니다. 시드 주입을 먼저 실행하십시오."
    ),

    /**
     * 실행 중에 {@code verification_runs} 행이 사라졌다. 클라이언트가 뭘 잘못 던진 것이 아니라
     * 데이터 정합 사고라 500 이다 — 404 로 두면 재고 소진 같은 정상 흐름 예외와 같은 취급을 받는다.
     */
    RUN_ROW_VANISHED(
            500,
            "VERIFICATION-008",
            "검증 실행 행이 실행 도중에 사라졌습니다."
    ),

    /**
     * 스케줄러·런타임이 안 멈췄다. 던진 파라미터가 아니라 <b>기동 설정</b>이 문제라 400 이 아니다 —
     * 400 이면 자동화가 "파라미터를 고쳐 재시도" 루프에 빠져 같은 자리에서 무한히 죽는다.
     */
    RUNTIME_NOT_QUIESCED(
            500,
            "VERIFICATION-009",
            "런타임·스케줄러가 멈추지 않아 검증을 시작할 수 없습니다."
    ),

    /**
     * 발급건의 {@code ISSUE} 이력이 정확히 하나가 아니다(없거나 둘 이상).
     * 데이터 정합 사고이므로 500 이다.
     *
     * <p><b>{@code DATASET_MUTATED_DURING_RUN} 과 갈라야 한다.</b> 그쪽은 <b>재시도로 낫는다</b> —
     * 쓰기를 멈추고 다시 돌리면 통과한다. 이쪽은 멈춰도 같은 자리에서 영원히 죽는다.
     * 원인이 데이터 변동이 아니라 구조 파손이라, 데이터를 고치는 것 말고는 길이 없다.
     *
     * <p><b>메시지 문구로 가르지 않는다.</b> 던지는 자리가 아홉이고 문구에 통일된 규칙이
     * 없다 — 어느 자리는 조치를 적고 어느 자리는 왜 이 실행을 믿을 수 없는지만 적는다.
     * 가르는 기준은 문구가 아니라 <b>재시도 가능성</b>이다.
     */
    ISSUE_HISTORY_NOT_EXACTLY_ONE(
            500,
            "VERIFICATION-010",
            "ISSUE 이력이 정확히 하나가 아닌 발급건이 있습니다."
    ),

    /**
     * 기동 시점 가드다. 잡 실행 중이 아니라 <b>컨텍스트가 뜨는 동안</b> 나므로 HTTP 로 나갈 일이
     * 없지만, 규약상 status 를 비워 둘 수 없어 500 을 쓴다.
     */
    SCHEMA_NOT_MIGRATED(
            500,
            "VERIFICATION-011",
            "배치가 보는 스키마에 핵심 테이블이 없습니다."
    );

    private final int status;
    private final String code;
    private final String message;

    VerificationErrorCode(int status, String code, String message) {
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
