package com.kafkick.api.coupon.dto.response;

import java.util.List;

import com.kafkick.core.coupon.query.MemberCouponPage;

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
