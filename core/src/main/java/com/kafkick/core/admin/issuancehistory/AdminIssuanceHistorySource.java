package com.kafkick.core.admin.issuancehistory;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.kafkick.core.coupon.CouponStateMachine;
import com.kafkick.core.coupon.IssuanceEventType;
import com.kafkick.core.coupon.IssuanceStatus;

/** 저장 기술에 독립적인 관리자 발급 상태 변경 이력 원천값입니다. */
public record AdminIssuanceHistorySource(List<RawHistory> histories) {

    /** 원천 행의 필수성, 중복 ID, 불변 복사를 검증합니다. */
    public AdminIssuanceHistorySource {
        Objects.requireNonNull(histories, "histories");
        histories = List.copyOf(histories);
        Set<Long> historyIds = new HashSet<>();
        for (RawHistory history : histories) {
            if (!historyIds.add(history.historyId())) {
                throw new IllegalArgumentException("historyId는 중복될 수 없습니다.");
            }
        }
    }

    /** 하나의 발급건에서 일어난 상태 변경을 표현하는 원천 행입니다. */
    public record RawHistory(
            long historyId,
            long issuanceId,
            String issuanceCode,
            long couponId,
            IssuanceStatus fromStatus,
            IssuanceStatus toStatus,
            IssuanceEventType eventType,
            String reason,
            String requestId,
            Instant occurredAt
    ) {

        /** 이력 ID, 표시 코드와 상태 전이가 정상적인 원천 행인지 검증합니다. */
        public RawHistory {
            requirePositive(historyId, "historyId");
            requirePositive(issuanceId, "issuanceId");
            requirePositive(couponId, "couponId");
            Objects.requireNonNull(issuanceCode, "issuanceCode");
            Objects.requireNonNull(toStatus, "toStatus");
            Objects.requireNonNull(eventType, "eventType");
            Objects.requireNonNull(occurredAt, "occurredAt");
            if (issuanceCode.length() != 16) {
                throw new IllegalArgumentException("issuanceCode는 정확히 16자여야 합니다.");
            }
            if (reason != null && reason.isEmpty()) {
                throw new IllegalArgumentException("reason은 빈 문자열일 수 없습니다.");
            }
            if (requestId != null && requestId.isEmpty()) {
                throw new IllegalArgumentException("requestId는 빈 문자열일 수 없습니다.");
            }
            if (!CouponStateMachine.isLegal(fromStatus, eventType, toStatus)) {
                throw new IllegalArgumentException("상태 전이가 합법적이지 않습니다.");
            }
        }

        /** 양수 식별자 공통 규칙을 적용합니다. */
        private static void requirePositive(long value, String name) {
            if (value <= 0L) {
                throw new IllegalArgumentException(name + "는 양수여야 합니다.");
            }
        }
    }
}
