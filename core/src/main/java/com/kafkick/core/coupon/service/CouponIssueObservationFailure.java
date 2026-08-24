package com.kafkick.core.coupon.service;

import com.kafkick.core.observation.Dependency;
import com.kafkick.core.observation.ReasonCode;

/**
 * 발급 실패 한 건의 최종 관측 분류입니다.
 *
 * @param httpStatus 실제 실패 응답 상태
 * @param reasonCode 관측용 실패 사유
 * @param dependency 실패와 직접 관련된 외부 의존성
 */
public record CouponIssueObservationFailure(
        int httpStatus,
        ReasonCode reasonCode,
        Dependency dependency
) {
}
