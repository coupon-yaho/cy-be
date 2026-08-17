// 쿠폰 템플릿 목록과 페이지 메타데이터를 공통 응답 형식으로 제공합니다.
package com.kafkick.api.coupon.dto;

import java.util.List;

import com.kafkick.core.coupon.port.CouponTemplatePage;

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
