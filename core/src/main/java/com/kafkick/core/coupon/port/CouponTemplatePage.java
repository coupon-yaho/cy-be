// 저장 기술에 의존하지 않는 쿠폰 템플릿 페이지 조회 결과입니다.
package com.kafkick.core.coupon.port;

import java.util.List;

import com.kafkick.core.coupon.domain.CouponTemplate;

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
