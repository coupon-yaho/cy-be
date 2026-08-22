package com.kafkick.core.coupon.query;

import java.util.List;

public record MemberCouponPage(
        List<MemberCouponSummary> content,
        int page,
        int size,
        long totalElements,
        int totalPages
) {

    public MemberCouponPage {
        content = List.copyOf(content);
    }
}
