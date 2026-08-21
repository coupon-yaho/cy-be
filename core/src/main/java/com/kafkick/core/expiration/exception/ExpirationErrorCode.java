// 만료 배치가 멈춰야 하는 상황들입니다.
package com.kafkick.core.expiration.exception;

import com.kafkick.core.support.exception.ErrorCode;

/**
 * <b>검증 코드를 빌려 쓰지 않는다.</b> {@code DATASET_MUTATED_DURING_RUN} 은
 * <i>재시도로 낫는다</i> 로 정의된 코드다 — 쓰기를 멈추고 다시 돌리면 통과한다.
 * 아래 셋은 다시 돌려도 같은 자리에서 죽는다. 원인이 경합이 아니라 데이터 구조이기 때문이다.
 *
 * <p>가르는 기준이 문구가 아니라 <b>재시도 가능성</b>이라는 것은
 * {@code VerificationErrorCode} 가 스스로 못 박아 둔 것이다. 그 규칙을 여기서 지킨다.
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
     * <p>{@code JOIN} 이 그 회차를 조용히 건너뛰므로 되돌릴 재고가 없다. 다시 돌려도
     * 재고 행은 여전히 없다 — 사람이 그 회차를 손봐야 끝난다.
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
     * <p>재시도 가능성으로 가르면 {@code STOCK_ROW_MISSING} 과 같은 쪽이다 — 다시 돌려도
     * 재고는 여전히 모자라다. 그래도 코드를 나누는 것은 <b>사람이 볼 곳이 다르기</b> 때문이다.
     * 하나는 없는 행을 만들어야 하고, 하나는 어긋난 수량을 맞춰야 한다.
     */
    STOCK_UNDERFLOW(
            500,
            "EXPIRATION-003",
            "만료분을 빼면 재고가 음수가 되는 회차가 있습니다."
    ),

    /**
     * {@code asOf} 가 현재보다 앞선다.
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
            "asOf 가 현재보다 앞섭니다."
    );

    private final int status;
    private final String code;
    private final String message;

    ExpirationErrorCode(int status, String code, String message) {
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
