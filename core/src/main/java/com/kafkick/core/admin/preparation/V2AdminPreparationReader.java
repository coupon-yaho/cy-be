package com.kafkick.core.admin.preparation;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.membership.domain.MembershipGrade;

/** 관리자 운영현황이 V2 예약 회차의 Redis 준비 상태를 일괄 조회하는 포트입니다. */
public interface V2AdminPreparationReader {

    /**
     * 동일한 관측 시각을 기준으로 요청한 V2 예약 회차의 준비 상태를 조회합니다.
     *
     * @param requests 중복되지 않은 V2 예약 회차 요청
     * @param observedAt 관리자 snapshot 관측 시각
     * @return couponId별 V2 준비 원천
     */
    Map<Long, V2PreparationSource> read(List<Request> requests, Instant observedAt);

    /** Redis 준비 계약을 DB 정본과 비교하는 데 필요한 예약 회차 값입니다. */
    record Request(
            long couponId,
            CouponRoundStatus campaignStatus,
            Instant opensAt,
            Instant closesAt,
            int expectedGradeMask,
            long expectedTotalQuantity
    ) {

        private static final Duration MAX_COUPON_ROUND_DURATION = Duration.ofHours(24L);

        /** 예약 상태와 발급 도메인이 복원할 수 있는 DB 비교값만 허용합니다. */
        public Request {
            Objects.requireNonNull(campaignStatus, "campaignStatus");
            Objects.requireNonNull(opensAt, "opensAt");
            Objects.requireNonNull(closesAt, "closesAt");
            if (couponId <= 0L
                    || campaignStatus != CouponRoundStatus.SCHEDULED
                    || !opensAt.isBefore(closesAt)
                    || Duration.between(opensAt, closesAt).compareTo(MAX_COUPON_ROUND_DURATION) > 0
                    || expectedTotalQuantity <= 0L) {
                throw new IllegalArgumentException("V2 준비 요청의 DB 비교값이 유효하지 않습니다.");
            }
            MembershipGrade.fromMask(expectedGradeMask);
        }
    }
}
