// 검증 실행의 조회 범위입니다. 합격 판정은 FULL 에서만 합니다.
package com.kafkick.core.verification;

/**
 * 잡을 둘로 나누지 않습니다 — 규칙 구현이 두 벌이 되면 두 결과가 갈릴 때 진실을 판단할 근거가 사라집니다.
 * 같은 Job 의 파라미터 차이일 뿐입니다.
 *
 * INCREMENTAL 은 절대 구간 (from_ts, as_of] 만 받습니다. "최근 N분" 같은 상대 윈도우는
 * 현재 시각 기준이라 같은 파라미터로 재실행해도 재현되지 않습니다.
 */
public enum ScopeType {

    /** 전수. 합격 판정을 지는 유일한 범위 */
    FULL,

    /** 증분. 관측용이고 판정하지 않습니다 */
    INCREMENTAL
}
