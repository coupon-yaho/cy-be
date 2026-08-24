package com.kafkick.api.coupon.dto.response;

import java.util.List;

import com.kafkick.core.coupon.query.IssuableCouponRoundPage;

public record IssuableCouponRoundPageResponse(
        List<IssuableCouponRoundResponse> content,
        int page,
        int size,
        long totalElements,
        int totalPages
) {

    public IssuableCouponRoundPageResponse {
        content = List.copyOf(content);
    }

    public static IssuableCouponRoundPageResponse from(
            IssuableCouponRoundPage page
    ) {
        return new IssuableCouponRoundPageResponse(
                page.content().stream()
                        .map(IssuableCouponRoundResponse::from)
                        .toList(),
                page.page(),
                page.size(),
                page.totalElements(),
                page.totalPages()
        );
    }
}
