package com.kafkick.core.admin.issuancehistory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

/** 관리자 발급 이력 조회 조건의 외부 입력 오류 계약을 검증합니다. */
class AdminIssuanceHistoryQueryTest {

    /** 잘못된 쿠폰 회차 식별자가 공통 400 오류로 변환되는지 검증합니다. */
    @Test
    @DisplayName("0 이하 couponId를 COMMON-001로 거부한다")
    void rejectsNonPositiveCouponIdAsInvalidInput() {
        assertInvalidInput(() -> new AdminIssuanceHistoryQuery(
                0L, null, null, null, null, AdminIssuanceHistoryQuery.DEFAULT_LIMIT));
    }

    /** 역전되거나 비어 있는 기간이 공통 400 오류로 변환되는지 검증합니다. */
    @Test
    @DisplayName("시작 시각이 종료 시각보다 빠르지 않으면 COMMON-001로 거부한다")
    void rejectsNonChronologicalRangeAsInvalidInput() {
        Instant boundary = Instant.parse("2026-08-23T00:00:00Z");

        assertInvalidInput(() -> new AdminIssuanceHistoryQuery(
                null, boundary, boundary, null, null, AdminIssuanceHistoryQuery.DEFAULT_LIMIT));
    }

    /** 허용 범위를 벗어난 페이지 크기가 공통 400 오류로 변환되는지 검증합니다. */
    @Test
    @DisplayName("허용 범위를 벗어난 limit을 COMMON-001로 거부한다")
    void rejectsOutOfRangeLimitAsInvalidInput() {
        assertInvalidInput(() -> new AdminIssuanceHistoryQuery(
                null, null, null, null, null, AdminIssuanceHistoryQuery.MAX_LIMIT + 1));
    }

    /** 예외 타입과 공통 오류 코드를 함께 검증합니다. */
    private static void assertInvalidInput(Runnable construction) {
        assertThatThrownBy(construction::run)
                .isInstanceOfSatisfying(BusinessException.class,
                        exception -> assertThat(exception.getErrorCode())
                                .isEqualTo(CommonErrorCode.INVALID_INPUT));
    }
}
