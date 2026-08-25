package com.kafkick.api.observation.issuance;

import java.util.Objects;

import com.kafkick.core.observation.Dependency;
import com.kafkick.core.observation.ReasonCode;

/**
 * 발급 중 발생한 예외를 관측 이벤트에 필요한 최종 값으로 변환한 결과입니다.
 *
 * @param httpStatus 실제 HTTP 응답과 같은 상태 코드
 * @param reasonCode 운영 집계에 사용할 실패 사유
 * @param dependency 실패한 외부 의존성
 */
public record CouponIssueObservationFailure(
        int httpStatus,
        ReasonCode reasonCode,
        Dependency dependency
) {

    public CouponIssueObservationFailure {
        if (httpStatus < 100 || httpStatus > 599) {
            throw new IllegalArgumentException(
                    "httpStatus는 100 이상 599 이하여야 합니다."
            );
        }
        Objects.requireNonNull(reasonCode, "reasonCode");
        Objects.requireNonNull(dependency, "dependency");
    }
}
