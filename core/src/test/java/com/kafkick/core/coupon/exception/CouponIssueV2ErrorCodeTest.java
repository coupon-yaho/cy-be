package com.kafkick.core.coupon.exception;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CouponIssueV2ErrorCodeTest {

    /**
     * failover·재구성 구간의 5xx 는 인플라이트 전량이 같은 코드로 나간다. 요청마다 스택을
     * 찍으면 로그 I/O 가 응답 지연을 밀어 올려, 이 작업이 재려는 복구 시간 자체가 오염된다.
     */
    @Test
    void massProducedFiveHundredsDoNotCarryStackTraces() {
        assertThat(CouponIssueV2ErrorCode.REDIS_UNAVAILABLE.logStackTrace()).isFalse();
        assertThat(CouponIssueV2ErrorCode.GATE_NOT_READY.logStackTrace()).isFalse();
        assertThat(CouponIssueV2ErrorCode.COUNTER_UNREADABLE.logStackTrace()).isFalse();
    }

    /** 값 파손과 호출부 버그는 드물다 — 스택 없이는 어느 자리가 깨졌는지 못 찾는다. */
    @Test
    void rareServerFaultsKeepTheirStackTraces() {
        assertThat(CouponIssueV2ErrorCode.VALUE_CORRUPT.logStackTrace()).isTrue();
        assertThat(CouponIssueV2ErrorCode.BAD_ARGUMENT.logStackTrace()).isTrue();
    }
}
