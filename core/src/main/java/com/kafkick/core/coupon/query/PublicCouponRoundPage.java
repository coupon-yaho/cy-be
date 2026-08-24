package com.kafkick.core.coupon.query;

import java.util.List;

public record PublicCouponRoundPage(
        List<CouponRoundDetail> content,
        int page,
        int size,
        long totalElements,
        int totalPages
) {

    public PublicCouponRoundPage {
        content = List.copyOf(content);
    }
}
