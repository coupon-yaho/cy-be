package com.kafkick.api.admin.issuance.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import com.kafkick.core.observation.ReasonCode;

/**
 * 회원 발급 문의 조회의 필터와 과거 방향 cursor를 바인딩하는 변경 가능한 선구축 초안입니다.
 *
 * <p>{@code memberId}만 필수이고, 쿠폰·HTTP 상태·사유 코드는 선택 조건입니다. HTTP 상태는 표준 범위를
 * 포함하는 100~599만 허용합니다. {@code beforeCursor}는 현재 페이지보다 오래된 결과를 가리키고,
 * {@code limit}은 누락 시 설정된 기본값(초깃값 50), 최대 200으로 제한됩니다.</p>
 *
 * @param memberId 필수 회원 식별자
 * @param couponId 선택 쿠폰 캠페인 회차 필터
 * @param httpStatus 선택 HTTP 상태 필터
 * @param reasonCode 선택 실패·정책 결과 사유 필터
 * @param beforeCursor 현재 페이지보다 오래된 결과를 요청하는 불투명 cursor
 * @param limit 페이지 크기; 누락 시 설정된 기본값(초깃값 50), 허용 범위 1~200
 */
public record IssuanceInquiryQuery(
        @NotNull(message = "memberId는 필수입니다.")
        @Positive(message = "memberId는 양수여야 합니다.") Long memberId,
        @Positive(message = "couponId는 양수여야 합니다.") Long couponId,
        @Min(value = 100, message = "httpStatus는 100 이상이어야 합니다.")
        @Max(value = 599, message = "httpStatus는 599 이하여야 합니다.") Integer httpStatus,
        ReasonCode reasonCode,
        String beforeCursor,
        @Min(value = 1, message = "limit은 1 이상이어야 합니다.")
        @Max(value = 200, message = "limit은 200 이하여야 합니다.") Integer limit
) {

    /** HTTP 어댑터가 설정으로 정규화한 limit을 반영한 복사본을 만듭니다. */
    public IssuanceInquiryQuery withLimit(int normalizedLimit) {
        return new IssuanceInquiryQuery(
                memberId, couponId, httpStatus, reasonCode, beforeCursor, normalizedLimit);
    }
}
