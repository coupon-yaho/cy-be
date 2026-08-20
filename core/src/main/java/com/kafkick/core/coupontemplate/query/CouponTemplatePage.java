package com.kafkick.core.coupontemplate.query;

import java.util.List;

import com.kafkick.core.coupontemplate.domain.CouponTemplate;

public record CouponTemplatePage(
        List<CouponTemplate> content,
        int page,
        int size,
        long totalElements,
        int totalPages
) {

    public CouponTemplatePage {
        content = List.copyOf(content);
    }
}
