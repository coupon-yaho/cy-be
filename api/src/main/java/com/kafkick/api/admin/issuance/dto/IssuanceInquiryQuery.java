package com.kafkick.api.admin.issuance.dto;

import java.util.Objects;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import com.kafkick.api.admin.issuance.IssuanceInquiryCursorCodec;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryQuery;
import com.kafkick.core.observation.ReasonCode;

/**
 * 회원 발급 문의 조회의 필터와 과거 방향 cursor를 바인딩하는 변경 가능한 선구축 초안입니다.
 *
 * <p>{@code memberId}만 필수이고, 쿠폰·HTTP 상태·사유 코드는 선택 조건입니다. HTTP 상태는 표준 범위를
 * 포함하는 100~599만 허용합니다. {@code beforeCursor}는 현재 페이지보다 오래된 결과를 가리키고,
 * {@code limit}은 지정하는 경우 1~200으로 제한됩니다.</p>
 *
 * @param memberId 필수 회원 식별자
 * @param couponId 선택 쿠폰 캠페인 회차 필터
 * @param httpStatus 선택 HTTP 상태 필터
 * @param reasonCode 선택 실패·정책 결과 사유 필터
 * @param beforeCursor 현재 페이지보다 오래된 결과를 요청하는 불투명 cursor
 * @param limit 페이지 크기; 선택값, 허용 범위 1~200
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

    /** 검증된 HTTP 필터와 cursor를 Core 조회 조건으로 변환합니다. */
    public AdminIssuanceInquiryQuery toCoreQuery(IssuanceInquiryCursorCodec cursorCodec) {
        Objects.requireNonNull(cursorCodec, "cursorCodec");
        int resolvedLimit = limit == null
                ? AdminIssuanceInquiryQuery.DEFAULT_LIMIT
                : limit;
        return new AdminIssuanceInquiryQuery(
                Objects.requireNonNull(memberId, "memberId"),
                couponId,
                httpStatus,
                reasonCode,
                // null만 파라미터 생략이다. 빈 값·공백은 잘못된 Cursor로 Codec에서 거부한다.
                beforeCursor == null ? null : cursorCodec.decode(beforeCursor),
                resolvedLimit);
    }
}
