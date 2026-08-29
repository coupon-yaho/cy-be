package com.kafkick.storage.db.admin.inquiry;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** 대표 attempt 계산 전에 선택 쿠폰으로 모집단을 줄이는 SQL 계약을 검증합니다. */
class AdminIssuanceInquirySqlTest {

    /** 선택 쿠폰 조건이 대표 행 계산의 입력을 먼저 줄이고 바깥에서 다시 적용되지 않는지 확인합니다. */
    @Test
    void filtersCouponBeforeRankingAttempts() {
        String sql = AdminIssuanceInquirySql.attempts(null);
        int couponFilter = sql.indexOf("(:couponId IS NULL OR ia.coupon_id = :couponId)");
        int outerSelect = sql.indexOf("SELECT id, event_type, request_id");

        assertThat(couponFilter).isGreaterThanOrEqualTo(0).isLessThan(outerSelect);
        assertThat(sql).doesNotContain("(:couponId IS NULL OR coupon_id = :couponId)");
    }
}
