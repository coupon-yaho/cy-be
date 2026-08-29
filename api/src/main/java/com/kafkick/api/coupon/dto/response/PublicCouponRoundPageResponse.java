package com.kafkick.api.coupon.dto.response;

import java.util.List;

import com.kafkick.core.coupon.query.PublicCouponRoundPage;

public record PublicCouponRoundPageResponse(
        List<CouponRoundDetailResponse> content,
        int page,
        int size,
        long totalElements,
        int totalPages
) {

    public PublicCouponRoundPageResponse {
        content = List.copyOf(content);
    }

    public static PublicCouponRoundPageResponse from(
            PublicCouponRoundPage page
    ) {
        return new PublicCouponRoundPageResponse(
                page.content().stream()
                        .map(CouponRoundDetailResponse::from)
                        .toList(),
                page.page(),
                page.size(),
                page.totalElements(),
                page.totalPages()
        );
    }
}
