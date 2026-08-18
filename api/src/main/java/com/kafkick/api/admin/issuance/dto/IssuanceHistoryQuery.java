package com.kafkick.api.admin.issuance.dto;

import java.time.LocalDate;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Positive;

import com.kafkick.core.coupon.IssuanceEventType;

/**
 * 발급 상태 이력의 선택 필터와 과거 방향 cursor를 바인딩하는 변경 가능한 선구축 초안입니다.
 *
 * <p>쿠폰·기간·이벤트 유형은 모두 선택 조건입니다. {@code beforeCursor}는 현재 페이지 마지막 전이보다
 * 오래된 이력을 가리키며, {@code limit}은 누락 시 설정된 기본값(초깃값 50), 최대 200입니다. 기간의 양 끝 중 하나만 지정하는
 * 요청은 허용하고, 둘 다 있을 때만 순서를 검증합니다.</p>
 *
 * @param couponId 선택 쿠폰 캠페인 회차 필터
 * @param beforeCursor 현재 페이지보다 오래된 이력을 요청하는 불투명 cursor
 * @param limit 페이지 크기; 누락 시 설정된 기본값(초깃값 50), 허용 범위 1~200
 * @param from 선택 조회 시작일
 * @param to 선택 조회 종료일
 * @param eventType 선택 상태 전이 이벤트 유형
 */
public record IssuanceHistoryQuery(
        @Positive(message = "couponId는 양수여야 합니다.") Long couponId,
        String beforeCursor,
        @Min(value = 1, message = "limit은 1 이상이어야 합니다.")
        @Max(value = 200, message = "limit은 200 이하여야 합니다.") Integer limit,
        LocalDate from,
        LocalDate to,
        IssuanceEventType eventType
) {

    /** HTTP 어댑터가 설정으로 정규화한 limit을 반영한 복사본을 만듭니다. */
    public IssuanceHistoryQuery withLimit(int normalizedLimit) {
        return new IssuanceHistoryQuery(
                couponId, beforeCursor, normalizedLimit, from, to, eventType);
    }

    /**
     * from과 to가 모두 존재할 때 시작일이 종료일보다 늦지 않은지 확인합니다.
     *
     * @return 날짜 한쪽이 없거나 from이 to보다 늦지 않으면 true
     */
    @AssertTrue(message = "from은 to보다 늦을 수 없습니다.")
    public boolean hasChronologicalRange() {
        return from == null || to == null || !from.isAfter(to);
    }
}
