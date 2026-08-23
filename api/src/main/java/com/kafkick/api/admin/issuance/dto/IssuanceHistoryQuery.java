package com.kafkick.api.admin.issuance.dto;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Positive;

import com.kafkick.api.admin.issuance.IssuanceHistoryCursorCodec;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryQuery;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryQuery.HistoryPosition;
import com.kafkick.core.coupon.IssuanceEventType;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

/**
 * 발급 상태 이력의 선택 필터와 과거 방향 cursor를 바인딩하는 변경 가능한 선구축 초안입니다.
 *
 * <p>쿠폰·기간·이벤트 유형은 모두 선택 조건입니다. {@code beforeCursor}는 현재 페이지 마지막 전이보다
 * 오래된 이력을 가리키며, {@code limit}은 지정하는 경우 1~200입니다. 기간의 양 끝 중 하나만 지정하는
 * 요청은 허용하고, 둘 다 있을 때만 순서를 검증합니다.</p>
 *
 * @param couponId 선택 쿠폰 캠페인 회차 필터
 * @param beforeCursor 현재 페이지보다 오래된 이력을 요청하는 불투명 cursor
 * @param limit 페이지 크기; 선택값, 허용 범위 1~200
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

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    /**
     * from과 to가 모두 존재할 때 시작일이 종료일보다 늦지 않은지 확인합니다.
     *
     * @return 날짜 한쪽이 없거나 from이 to보다 늦지 않으면 true
     */
    @AssertTrue(message = "from은 to보다 늦을 수 없습니다.")
    public boolean hasChronologicalRange() {
        return from == null || to == null || !from.isAfter(to);
    }

    /**
     * HTTP 날짜와 cursor를 Core의 절대 시각 범위와 Keyset 위치로 변환합니다.
     *
     * @param cursorCodec 불투명 HTTP cursor 변환기
     * @return Core 발급 이력 조회 조건
     * @throws BusinessException 날짜를 유효한 Instant 조회 범위로 변환할 수 없는 경우
     */
    public AdminIssuanceHistoryQuery toCoreQuery(IssuanceHistoryCursorCodec cursorCodec) {
        Instant fromInclusive;
        Instant toExclusive;
        try {
            // 조회 날짜는 KST 00:00 포함 경계와 다음 날 00:00 제외 경계로 고정합니다.
            fromInclusive = from == null ? null : from.atStartOfDay(KST).toInstant();
            toExclusive = to == null ? null : to.plusDays(1L).atStartOfDay(KST).toInstant();
        } catch (DateTimeException exception) {
            // 클라이언트 날짜의 산술·변환 범위 오류를 내부 실패가 아닌 공통 400으로 통일합니다.
            throw new BusinessException(
                    CommonErrorCode.INVALID_INPUT,
                    "유효하지 않은 발급 이력 조회 기간입니다.",
                    exception);
        }
        HistoryPosition before = beforeCursor == null || beforeCursor.isBlank()
                ? null
                : cursorCodec.decode(beforeCursor);
        int resolvedLimit = limit == null ? AdminIssuanceHistoryQuery.DEFAULT_LIMIT : limit;
        return new AdminIssuanceHistoryQuery(
                couponId, fromInclusive, toExclusive, eventType, before, resolvedLimit);
    }
}
