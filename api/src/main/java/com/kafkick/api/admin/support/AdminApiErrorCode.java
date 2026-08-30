package com.kafkick.api.admin.support;

import com.kafkick.core.support.exception.ErrorCode;

/**
 * 관리자 HTTP API 선구축 단계에서 사용하는 API 계층 오류 코드입니다.
 *
 * <p>도메인 규칙 위반이 아니라 Controller의 실제 Use Case가 아직 연결되지 않았음을 표현하므로
 * core 도메인 오류와 분리합니다. Controller는 이 코드를 담은 {@code BusinessException}만 발생시키고,
 * 공통 {@code GlobalExceptionHandler}가 HTTP 상태와 실패 {@code ResponseEnvelope}를 생성합니다.</p>
 */
public enum AdminApiErrorCode implements ErrorCode {

    /** URL·입력·응답 타입은 존재하지만 실제 조회 또는 명령 구현이 연결되지 않은 상태입니다. */
    NOT_IMPLEMENTED(
            501,
            "ADMIN-001",
            "관리자 API 구현이 아직 연결되지 않았습니다."
    ),

    /** 관리자 역할이 없거나 정확한 {@code ADMIN} 값이 아닌 요청입니다. */
    FORBIDDEN(
            403,
            "ADMIN-002",
            "관리자 권한이 필요합니다."
    ),

    /**
     * 관측이 꺼져 있어 지금은 답할 수 없는 조회입니다.
     *
     * <p><b>404 와 갈라야 하는 상태입니다.</b> 404 는 "그런 기능이 없다"(배포가 잘못됐다)이고
     * 이것은 "사람이 스위치를 내렸다"(설정을 되돌리면 된다)입니다. 조치가 정반대라
     * 응답에서 구분되어야 합니다.
     *
     * <p>빈 목록(200)으로 답하는 선택지도 있었지만 쓰지 않습니다 — 그러면
     * "배치가 한 번도 안 돌았다" 로 읽힙니다.
     */
    OBSERVATION_DISABLED(
            503,
            "ADMIN-003",
            "관측이 꺼져 있어 조회할 수 없습니다."
    ),

    /**
     * 이 경로가 아직 지원하지 않는 관측 범위를 지정한 요청입니다.
     *
     * <p><b>조용히 무시하면 안 되는 상태입니다.</b> 무시하고 200 을 주면 화면은 회차로 좁혀진
     * 값을 받았다고 믿고 전역 값을 그립니다 — 깨지지 않고 틀린 숫자가 나가므로 발견이 늦습니다.
     *
     * <p>지금 이 코드를 내는 것은 {@code GET /metrics/series} 의 {@code benchmarkRunId} 하나뿐입니다.
     * 회차 경계는 라벨이 아니라 시간 범위라 원천이 DB 이고, 그 경로는 Prometheus 만 읽습니다.
     * {@code couponId} 는 OBS-34 에서 열렸습니다.
     *
     * <p>범위가 열리면 이 코드가 나오던 요청이 200 이 됩니다. 완화 방향이라 화면을 깨지 않습니다.
     */
    SCOPE_NOT_SUPPORTED(
            400,
            "ADMIN-004",
            "이 조회는 아직 Benchmark 회차 범위 지정을 지원하지 않습니다."
    ),

    NOTIFICATION_NOT_FOUND(
            404,
            "ADMIN-005",
            "해당 알림을 찾을 수 없습니다."
    ),

    NOTIFICATION_RESEND_CONFLICT(
            409,
            "ADMIN-006",
            "알림을 재발송할 수 없는 상태이거나 중복 요청입니다."
    ),

    NOTIFICATION_RESEND_LIMIT_EXCEEDED(
            409,
            "ADMIN-007",
            "알림 재발송 횟수 상한을 초과했습니다."
    );

    private final int status;
    private final String code;
    private final String message;

    AdminApiErrorCode(int status, String code, String message) {
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
