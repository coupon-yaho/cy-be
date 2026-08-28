package com.kafkick.api.admin.dashboard.dto;

import java.time.LocalDate;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import com.kafkick.api.admin.support.validation.ValidAnalyticsRange;

/**
 * 관리자 분석 조회의 필수 기간과 선택 필터를 바인딩하는 HTTP 요청입니다.
 *
 * <p>{@code from}/{@code to}는 모두 필수이고 최대 1년 범위까지 허용됩니다. 브랜드와 쿠폰 식별자는
 * 독립 선택 필터라 함께 지정할 수 있습니다. 두 식별자의 실제 존재와 소속 관계는 Core 분석 Service가
 * Source 메타데이터를 기준으로 검증합니다.</p>
 *
 * @param from 필수 조회 시작일
 * @param to 필수 조회 종료일
 * @param brandId 선택 브랜드 필터
 * @param couponId 선택 쿠폰 캠페인 회차 필터
 */
@ValidAnalyticsRange
public record AnalyticsQuery(
        @NotNull(message = "from은 필수입니다.") LocalDate from,
        @NotNull(message = "to는 필수입니다.") LocalDate to,
        @Positive(message = "brandId는 양수여야 합니다.") Long brandId,
        @Positive(message = "couponId는 양수여야 합니다.") Long couponId
) { }
