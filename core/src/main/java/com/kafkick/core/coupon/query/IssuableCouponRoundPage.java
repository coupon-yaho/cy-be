package com.kafkick.core.coupon.query;

import java.util.List;

public record IssuableCouponRoundPage(
        List<IssuableCouponRoundSummary> content,
        int page,
        int size,
        long totalElements,
        int totalPages
) {

    public IssuableCouponRoundPage {
        content = List.copyOf(content);
    }
}
