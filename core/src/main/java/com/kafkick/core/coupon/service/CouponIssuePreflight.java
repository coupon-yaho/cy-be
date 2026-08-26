package com.kafkick.core.coupon.service;

import java.util.Optional;

/**
 * 발급 사전조회 한 번의 결과입니다.
 *
 * @param completedResponseBody 이미 완료된 요청이면 저장된 응답, 아니면 {@code null}
 */
public record CouponIssuePreflight(String completedResponseBody) {

    private static final CouponIssuePreflight PENDING =
            new CouponIssuePreflight(null);

    /** 아직 처리되지 않은 요청입니다. 권위 발급으로 진행합니다. */
    public static CouponIssuePreflight pending() {
        return PENDING;
    }

    /** 이미 완료된 요청입니다. 저장된 응답을 그대로 돌려줍니다. */
    public static CouponIssuePreflight completed(String responseBody) {
        return new CouponIssuePreflight(responseBody);
    }

    public Optional<String> completed() {
        return Optional.ofNullable(completedResponseBody);
    }
}
