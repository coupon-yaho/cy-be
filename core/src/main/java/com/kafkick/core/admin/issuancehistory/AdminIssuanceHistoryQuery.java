package com.kafkick.core.admin.issuancehistory;

import java.time.Instant;
import java.util.Objects;

import com.kafkick.core.coupon.domain.IssuanceEventType;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

/** 관리자 발급 상태 변경 이력의 필터와 Keyset 페이지 조건입니다. */
public record AdminIssuanceHistoryQuery(
        Long couponId,
        Instant fromInclusive,
        Instant toExclusive,
        IssuanceEventType eventType,
        HistoryPosition before,
        int limit
) {

    /** 기본 페이지 크기입니다. */
    public static final int DEFAULT_LIMIT = 50;

    /** 한 페이지에 반환할 수 있는 최대 이력 수입니다. */
    public static final int MAX_LIMIT = 200;

    /**
     * 선택 필터와 페이지 크기의 유효 범위를 검증합니다.
     *
     * @throws BusinessException 외부 조회 조건이 허용 범위를 벗어난 경우 COMMON-001
     */
    public AdminIssuanceHistoryQuery {
        if (couponId != null && couponId <= 0L) {
            throw invalidInput("couponId는 양수여야 합니다.");
        }
        if (fromInclusive != null && toExclusive != null && !fromInclusive.isBefore(toExclusive)) {
            throw invalidInput("fromInclusive는 toExclusive보다 빨라야 합니다.");
        }
        if (limit < 1 || limit > MAX_LIMIT) {
            throw invalidInput("limit은 1에서 200 사이여야 합니다.");
        }
    }

    /** 외부 조회 조건의 세부 검증 실패를 공통 HTTP 400 계약으로 변환합니다. */
    private static BusinessException invalidInput(String detail) {
        return new BusinessException(CommonErrorCode.INVALID_INPUT, detail);
    }

    /** 다음 페이지가 이 위치보다 오래된 이력만 조회하도록 하는 Keyset 위치입니다. */
    public record HistoryPosition(Instant occurredAt, long historyId) {

        /** Cursor를 구성하는 시각과 이력 ID를 검증합니다. */
        public HistoryPosition {
            Objects.requireNonNull(occurredAt, "occurredAt");
            if (historyId <= 0L) {
                throw new IllegalArgumentException("historyId는 양수여야 합니다.");
            }
        }
    }
}
