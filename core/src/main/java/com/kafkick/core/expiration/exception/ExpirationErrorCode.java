// 만료 배치가 멈춰야 하는 상황과, 만료 복구 API 의 거절 사유입니다.
package com.kafkick.core.expiration.exception;

import com.kafkick.core.support.exception.ErrorCode;

/**
 * <b>검증 코드를 빌려 쓰지 않는다.</b> {@code DATASET_MUTATED_DURING_RUN} 은
 * <i>재시도로 낫는다</i> 로 정의된 코드다 — 쓰기를 멈추고 다시 돌리면 통과한다.
 * 아래 <b>잡 실패 다섯</b> 중 {@code EXPIRE_HISTORY_COUNT_MISMATCH}·{@code STOCK_UNDERFLOW}·
 * {@code EXPIRE_ASOF_IN_FUTURE}·{@code EXPIRE_ON_CORRUPT_SCHEMA} 넷은 다시 돌려도 같은
 * 자리에서 죽는다 — 원인이 데이터 구조이거나(앞 둘) 넘긴 파라미터(셋째) 또는
 * 접속 설정(넷째)이기 때문이다. {@code STOCK_ROW_MISSING} 만 예외다(그 항목의 설명 참조).
 *
 * <p>가르는 기준이 문구가 아니라 <b>재시도 가능성</b>이라는 것은
 * {@code VerificationErrorCode} 가 스스로 못 박아 둔 것이다. 그 규칙을 여기서 지킨다.
 *
 * <p><b>006·007 은 잡 실패가 아니다.</b> {@code ExpireAdminController}(CY-429)의 거절
 * 사유이고, 위 재시도 가능성 분류는 그 둘에 적용되지 않는다 — 고쳐야 하는 것이 데이터도
 * 파라미터도 아니라 <b>부른 대상이거나 시점</b>이기 때문이다. 잡이 낼 수 있는 코드를
 * 순회하는 쪽(예: 지표 라벨)은 그 둘을 빼야 한다.
 *
 * <p><b>CY-347 이후 재고 코드 둘의 뜻이 바뀌었다.</b> 재고가 어긋난 회차는
 * {@code ExpirationRepository.blockedCoupons} 가 <b>창 밖으로 미리 뺀다</b> — 그것은 이제
 * 예외가 아니라 {@code cy_expire_blocked_coupons} 게이지로 나가는 <b>판정</b>이고,
 * 배치는 나머지 회차로 정상 종료한다. 그래서 {@code STOCK_ROW_MISSING}·
 * {@code STOCK_UNDERFLOW} 가 실제로 뜨면 그것은 <i>"흔한 데이터 오염"</i> 이 아니라
 * <b>제외 논리가 샜거나 실행 중 재고가 움직인 사고</b>다. 두 항목의 설명이 그 자리를 적는다.
 */
public enum ExpirationErrorCode implements ErrorCode {

    /**
     * 넘긴 건수와 이력 수가 다르다.
     *
     * <p>리플레이가 이력으로 상태를 재구성하므로, 이력이 모자라면 검증이 그 발급건을
     * <i>"ISSUE 이력이 정확히 하나가 아닌 발급건"</i> 으로 잡는다 — 원인이 이 잡이라는 것은
     * 거기서 안 드러난다. 그래서 이 자리에서 멈춘다.
     */
    EXPIRE_HISTORY_COUNT_MISMATCH(
            500,
            "EXPIRATION-001",
            "만료 이력 수가 만료 건수와 다릅니다."
    ),

    /**
     * 재고 행이 없는 회차의 발급건이 만료로 넘어갔다.
     *
     * <p>{@code JOIN} 이 그 회차를 조용히 건너뛰므로 되돌릴 재고가 없다.
     *
     * <p><b>CY-347 이후로는 정상 경로에서 여기 오지 않는다.</b> 재고 행이 없는 회차는
     * {@code blockedCoupons} 의 {@code LEFT JOIN … IS NULL} 이 애초에 제외한다.
     * 그래도 도달했다면 원인은 둘 중 하나다 — <b>제외 논리가 샜거나</b>, 제외를 판정한 뒤
     * 실행 중에 상태가 움직였거나. <b>먼저 볼 것은 그 회차의 재고 데이터가 아니라 코드다.</b>
     *
     * <p><b>다만 이 코드만 재시도 불가 집합의 예외다.</b> 누가 지금 그 재고 행을 만들고 있는
     * 중일 수도 있다 — 만료 Step 은 READ COMMITTED 라 문장마다 스냅샷이 갱신되므로 그 창이
     * 실제로 열린다. 그때는 다음 주기가 알아서 지나간다. 그래서 던지는 메시지도
     * <i>"다시 돌려도 없다"</i> 로 단정하지 않는다 — 단정하면 운영자가 방금 자기가 넣은 행을
     * 다시 의심한다. 알림 규칙에서 즉시 호출로 올릴 때 이 예외를 함께 봐야 한다.
     */
    STOCK_ROW_MISSING(
            500,
            "EXPIRATION-002",
            "재고를 되돌리지 못한 회차가 있습니다."
    ),

    /**
     * 뺄 재고가 만료 건수보다 적은 회차가 있다.
     *
     * <p>재고가 이미 어긋난 상태라는 뜻이다. 그대로 빼면 {@code active_count} 가 음수가 되고
     * 그 뒤로 발급 가능 수량 계산이 전부 틀어진다.
     *
     * <p><b>{@code ck_stock_range} 와 나눠 두는 이유가 있다.</b> 그 CHECK 는 CLEAN 스키마에만
     * 걸린다 — 오염셋은 제약을 떼어 내고 만들기 때문에, 거기서는 DB 가 안 막아 준다.
     * SQL 조건({@code active_count >= 차감량})으로 거르고 이 코드로 세우면 스키마와 무관하게
     * 같은 자리에서 멈춘다.
     *
     * <p>코드를 {@code STOCK_ROW_MISSING} 과 나누는 것은 <b>사람이 볼 곳이 다르기</b>
     * 때문이다. 하나는 없는 행이, 하나는 모자란 수량이 원인이다.
     *
     * <p><b>CY-347 이후로는 정상 경로에서 여기 오지 않는다.</b> 만료분을 빼면 음수가 되는
     * 회차는 {@code blockedCoupons} 의 {@code active_count < pending} 이 애초에 제외한다.
     * 그래도 도달했다면 <b>제외 논리가 샜거나 실행 중 {@code active_count} 가 움직인 것</b>이다.
     * 그 회차의 재고를 손으로 맞추고 재시작하면 원인이 그대로라 다음 주기에 또 뜬다 —
     * <b>먼저 볼 것은 제외 질의와 동시 쓰기 경로다.</b>
     *
     * <p>어긋난 재고 자체는 실패가 아니라 {@code cy_expire_blocked_coupons} 로 나간다.
     */
    STOCK_UNDERFLOW(
            500,
            "EXPIRATION-003",
            "만료분을 빼면 재고가 음수가 되는 회차가 있습니다."
    ),

    /**
     * {@code asOf} 가 현재보다 <b>미래</b>다.
     *
     * <p><b>이것이 이 잡에서 가장 되돌리기 어려운 사고다.</b> {@code asOf} 는 만료 여부를
     * 가르는 컷이라, 미래로 주면 기한이 남은 {@code ISSUED} 가 전부 컷 안에 들어온다.
     * 잡은 <b>정상으로 완료되고</b> 상태 변경·이력·재고 차감이 커밋된다.
     * {@code EXPIRED} 는 종단 상태라 되돌리는 전이가 없다.
     *
     * <p>파라미터 검증기는 키 존재만 보므로 값의 범위는 아무도 안 봤다. 스케줄러가 주는 값은
     * 항상 과거지만, 밀린 만료를 따라잡으려고 손으로 트리거하는 순간 이 자리가 열린다 —
     * 설정 파일이 그 운영 절차를 스스로 권하고 있다.
     *
     * <p>재시도 가능성으로는 <b>다시 돌려도 같은 값이면 또 막힌다</b> — 고쳐야 할 것은
     * 데이터가 아니라 넘긴 파라미터다.
     */
    EXPIRE_ASOF_IN_FUTURE(
            500,
            "EXPIRATION-004",
            "asOf 가 현재보다 미래입니다."
    ),

    /**
     * <b>오염 스키마를 보고 있다.</b> 이 배치는 CLEAN 스키마에서만 돈다.
     *
     * <p><b>왜 여기서 죽어야 하나.</b> 만료는 원본을 <b>쓰는</b> 유일한 배치다. 오염셋을 보게
     * 띄우면 오염 유형 2·7(둘 다 {@code status = 'ISSUED'})의 발급건을 {@code EXPIRED} 로
     * 넘기고 {@code EXPIRE} 이력까지 붙인다. 그러면 리플레이가 {@code USED → EXPIRED} 라는
     * 전이표에 없는 조합을 만나 <b>{@code expected_findings} 에 없는 검출이 생기고</b>,
     * {@code dataset_fingerprint} 도 함께 움직인다 — 누락 0 · 오탐 0 이 합격 조건인데
     * 그것이 <b>검증기 버그처럼 보이는 모양</b>으로 깨진다.
     *
     * <p><b>회차 격리가 이 위험을 넓혔다.</b> 예전에는 첫 오염 회차에서 잡이 죽어 그 뒤로는
     * 아무것도 안 건드렸다. 지금은 막힌 회차만 빼고 <b>나머지 전부</b>를 넘긴다 —
     * 그 폭을 넓힌 변경이 그 폭을 막는 가드도 함께 진다.
     *
     * <p><b>이것은 판정이 아니다.</b> <i>"데이터가 틀렸다"</i> 가 아니라 <i>"여기서 돌면
     * 안 되는 배치가 돌았다"</i> 이고, 원인은 데이터가 아니라 접속 설정이라 실패가 맞다.
     * {@code verifyJob} 의 {@code rejectDatasetMismatch} 와 같은 자리이고 같은 근거
     * ({@code uk_coupon_member} 의 존재)를 쓴다.
     */
    EXPIRE_ON_CORRUPT_SCHEMA(
            500,
            "EXPIRATION-005",
            "오염 스키마에서는 만료 배치를 돌리지 않습니다."
    ),

    /**
     * 그 실행 번호가 없거나 만료 잡이 아니다.
     *
     * <p><b>둘을 같은 404 로 접는다.</b> 남의 잡이라는 것을 알려 주면 인증 없는 이 API 가
     * 배치 메타의 실행 번호 공간을 훑는 수단이 된다. {@code VerifyTriggerController} 가
     * 같은 근거로 같은 선택을 했다.
     */
    EXPIRE_EXECUTION_NOT_FOUND(
            404,
            "EXPIRATION-006",
            "해당 만료 실행을 찾을 수 없습니다."
    ),

    /**
     * <b>지금 걷어낼 수 있는 실행이 아니다.</b> 진도가 돌고 있거나, 이미 끝났다.
     *
     * <p><b>"이미 걷어냈다" 는 여기 안 온다.</b> 그것은 200 + {@code alreadyRecovered=true}
     * 다({@code ExpireRecoveryService} 가 실행 상태로 먼저 가른다) — 복구 API 의 재시도가
     * 에러를 내면 운영자가 첫 호출이 실패했다고 읽고 손 SQL 로 되돌아간다.
     *
     * <p>이 API 는 <i>복구</i>다 — 종료 표시를 못 남기고 죽은 실행을 닫는 것이지 도는
     * 배치를 멈추는 수단이 아니다. 만료는 <b>재고를 쓰는 유일한 잡</b>이라 중간에 끊으면
     * 다음 검증의 판정 근거가 흔들린다({@code VerifyTriggerController} 가 {@code /verify/}
     * 경로로 만료를 멈추지 못하게 막아 둔 것과 같은 이유다).
     *
     * <p>판정은 나이가 아니라 <b>진도</b>이고 {@code RunningJobProbe.stuckExecutions} 하나가
     * 그 임계를 진다. 손 SQL 이 임계를 따로 적어야 했던 자리를 이 코드가 대신한다.
     *
     * <p><b>문구가 갈래를 단정하지 않는 이유가 있다.</b> 클라이언트에 나가는 것은 이
     * 문장뿐이라({@code detail} 은 로그 전용) <i>"진도가 도는 실행이다"</i> 로 못 박으면
     * 이미 끝난 실행에 대해서도 그렇게 나가 <b>운영자가 시체를 살아 있다고 믿는다.</b>
     * 다음 동작은 {@code /runs/stuck} 을 다시 보는 것 하나이므로 그것을 문구에 담는다.
     */
    EXPIRE_EXECUTION_NOT_STUCK(
            409,
            "EXPIRATION-007",
            "지금 걷어낼 수 있는 실행이 아닙니다. /runs/stuck 을 다시 확인하십시오."
    );

    private final int status;
    private final String code;
    private final String message;

    ExpirationErrorCode(int status, String code, String message) {
        this.status = status;
        this.code = code;
        this.message = message;
    }

    /**
     * <b>잡이 낼 수 있는 코드인가.</b> 이 열거형의 클래스 주석이
     * <i>"잡이 낼 수 있는 코드를 순회하는 쪽(예: 지표 라벨)은 006·007 을 빼야 한다"</i>
     * 고 적어 뒀는데, 그 판정을 <b>읽는 쪽이 손으로 하게</b> 두면 매번 틀린다 —
     * 실제로 {@code ExpireFailureMetrics} 가 처음에 {@code values()} 를 통째로 돌았다.
     *
     * <p><b>500 이 기준이다.</b> 006·007 은 {@code ExpireAdminController} 의 거절 사유라
     * 404·409 이고, 잡 실패 다섯은 전부 500 이다. 새 코드를 더할 때 이 대응이 깨지면
     * {@code ExpireFailureMetricsTest} 가 개수로 잡는다.
     */
    public boolean isJobFailure() {
        return status == 500;
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
