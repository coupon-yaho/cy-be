package com.kafkick.api.coupontemplate.dto.response;

import java.util.List;

import com.kafkick.core.coupontemplate.query.CouponTemplatePage;

public record CouponTemplatePageResponse(
        List<CouponTemplateDetailResponse> content,
        int page,
        int size,
        long totalElements,
        int totalPages
) {

    public CouponTemplatePageResponse {
        content = List.copyOf(content);
    }

    public static CouponTemplatePageResponse from(
            CouponTemplatePage couponTemplatePage
    ) {
        return new CouponTemplatePageResponse(
                couponTemplatePage.content().stream()
                        .map(CouponTemplateDetailResponse::from)
                        .toList(),
                couponTemplatePage.page(),
                couponTemplatePage.size(),
                couponTemplatePage.totalElements(),
                couponTemplatePage.totalPages()
        );
    }
}
