// 사용자 보유 쿠폰 목록과 페이지 메타데이터를 반환합니다.
package com.kafkick.api.coupon.dto;

import java.util.List;

import com.kafkick.core.coupon.port.MemberCouponPage;

public record MemberCouponPageResponse(
        List<MemberCouponResponse> content,
        int page,
        int size,
        long totalElements,
        int totalPages
) {

    public MemberCouponPageResponse {
        content = List.copyOf(content);
    }

    public static MemberCouponPageResponse from(MemberCouponPage page) {
        return new MemberCouponPageResponse(
                page.content().stream()
                        .map(MemberCouponResponse::from)
                        .toList(),
                page.page(),
                page.size(),
                page.totalElements(),
                page.totalPages()
        );
    }
}
