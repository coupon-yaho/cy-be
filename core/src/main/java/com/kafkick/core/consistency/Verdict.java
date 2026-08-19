package com.kafkick.core.consistency;

/** FINAL 단계의 정합성 합격 여부입니다. */
public enum Verdict {

    /** 적용 가능한 모든 gap과 초과 발급 수가 0입니다. */
    PASS,

    /** 적용 가능한 gap이 0이 아니거나 초과 발급이 발생했습니다. */
    FAIL
}
