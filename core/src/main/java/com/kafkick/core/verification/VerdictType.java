// 검증 판정 결과입니다. 규칙 기준이며 D10 게이트가 읽습니다.
package com.kafkick.core.verification;

/**
 * 통계 상태(StatsStatus)와 한 컬럼에 섞지 않습니다.
 * 섞으면 "검증은 됐는데 통계가 안 됐다"를 표현할 방법이 없어집니다 — CORRUPT run 이 정확히 그 상태입니다.
 */
public enum VerdictType {

    PASS,
    FAIL
}
