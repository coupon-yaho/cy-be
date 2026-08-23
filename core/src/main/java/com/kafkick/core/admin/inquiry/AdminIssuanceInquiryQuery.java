package com.kafkick.core.admin.inquiry;

import java.time.Instant;
import java.util.Objects;

import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

/** 회원 발급 문의의 필터와 Keyset 페이지 조건입니다. */
public record AdminIssuanceInquiryQuery(
        long memberId,
        Long couponId,
        Integer httpStatus,
        ReasonCode reasonCode,
        InquiryPosition before,
        int limit
) {

    public static final int DEFAULT_LIMIT = 50;
    public static final int MAX_LIMIT = 200;

    /** 외부 조회 조건을 공통 400 입력 오류 계약으로 검증합니다. */
    public AdminIssuanceInquiryQuery {
        if (memberId <= 0L) {
            throw invalidInput("memberId는 양수여야 합니다.");
        }
        if (couponId != null && couponId <= 0L) {
            throw invalidInput("couponId는 양수여야 합니다.");
        }
        if (httpStatus != null && (httpStatus < 100 || httpStatus > 599)) {
            throw invalidInput("httpStatus는 100~599여야 합니다.");
        }
        if (limit < 1 || limit > MAX_LIMIT) {
            throw invalidInput("limit은 1~200이어야 합니다.");
        }
    }

    private static BusinessException invalidInput(String detail) {
        return new BusinessException(CommonErrorCode.INVALID_INPUT, detail);
    }

    /** 서로 다른 ID 공간까지 포함하는 문의 결과의 정렬 위치입니다. */
    public record InquiryPosition(
            Instant occurredAt,
            SourceKind sourceKind,
            long sourceId
    ) {

        /** Cursor 구성 요소의 필수성과 양수 ID를 검증합니다. */
        public InquiryPosition {
            Objects.requireNonNull(occurredAt, "occurredAt");
            Objects.requireNonNull(sourceKind, "sourceKind");
            if (sourceId <= 0L) {
                throw new IllegalArgumentException("sourceId는 양수여야 합니다.");
            }
        }
    }

    /** 정렬 위치가 사용하는 원천 ID 공간입니다. */
    public enum SourceKind {
        ATTEMPT,
        ISSUANCE
    }
}
