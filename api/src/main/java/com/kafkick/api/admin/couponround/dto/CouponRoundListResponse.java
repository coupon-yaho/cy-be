package com.kafkick.api.admin.couponround.dto;

import java.time.Instant;
import java.util.List;

import com.kafkick.core.coupon.domain.CouponRoundStatus;

/** 쿠폰 회차 목록의 과거 방향 cursor 응답입니다. */
public record CouponRoundListResponse(List<CouponRoundSummary> items, String nextBeforeCursor, boolean hasOlder) {
    /**
     * 쿠폰 회차 목록 한 행에 필요한 운영 상태와 재고 요약입니다.
     *
     * @param couponId 쿠폰 회차 식별자
     * @param brandId 소유 브랜드 식별자
     * @param couponName 관리자 화면 표시명
     * @param status 현재 쿠폰 회차 상태
     * @param totalQuantity 최초 발행 가능 수량
     * @param activeCount 현재 활성 발급 수
     * @param openAt 예정 또는 실제 오픈 시각
     * @param closeAt 예정 또는 실제 마감 시각
     */
    public record CouponRoundSummary(Long couponId, Long brandId, String couponName, CouponRoundStatus status,
                                  long totalQuantity, long activeCount, Instant openAt, Instant closeAt) { }
}
