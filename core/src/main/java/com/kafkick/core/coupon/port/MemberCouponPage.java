// 사용자 보유 쿠폰 목록과 페이지 메타데이터를 표현합니다.
package com.kafkick.core.coupon.port;

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
