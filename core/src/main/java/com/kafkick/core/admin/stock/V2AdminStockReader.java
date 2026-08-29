package com.kafkick.core.admin.stock;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import com.kafkick.core.admin.couponmetrics.CouponMetricsSource;
import com.kafkick.core.coupon.domain.CouponRoundStatus;

/** V2 발급 재고 정본을 기술 중립 관리자 관측값으로 읽는 Core 포트입니다. */
public interface V2AdminStockReader {

    /** 요청한 회차별 Redis 재고를 같은 관리자 스냅샷 시각으로 반환합니다. */
    Map<Long, CouponMetricsSource.Observation<AdminStockSnapshot>> read(
            List<Request> requests,
            Instant observedAt
    );

    /** Redis 값 검증에 필요한 회차 상태와 DB 전체 수량입니다. */
    record Request(long couponId, CouponRoundStatus campaignStatus, long expectedTotalQuantity) {

        /** 잘못된 회차 식별자나 DB 전체 수량이 Redis 조회로 넘어가지 않게 막습니다. */
        public Request {
            if (couponId <= 0L) {
                throw new IllegalArgumentException("couponId는 양수여야 합니다.");
            }
            if (campaignStatus == null) {
                throw new NullPointerException("campaignStatus");
            }
            if (expectedTotalQuantity <= 0L) {
                throw new IllegalArgumentException("expectedTotalQuantity는 양수여야 합니다.");
            }
        }
    }
}
