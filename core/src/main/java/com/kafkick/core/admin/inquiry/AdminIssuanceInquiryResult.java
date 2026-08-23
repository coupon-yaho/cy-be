package com.kafkick.core.admin.inquiry;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryQuery.InquiryPosition;
import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.core.observation.ReasonCode;

/** 연결·필터·정렬을 마친 회원 발급 문의의 한 페이지입니다. */
public record AdminIssuanceInquiryResult(
        List<InquiryItem> items,
        InquiryPosition nextBefore,
        boolean hasOlder
) {

    /** 반환 목록의 불변성과 다음 Cursor 일관성을 검증합니다. */
    public AdminIssuanceInquiryResult {
        items = List.copyOf(Objects.requireNonNull(items, "items"));
        if (hasOlder && (items.isEmpty() || nextBefore == null)) {
            throw new IllegalArgumentException("이전 결과가 있으면 마지막 항목 위치가 필요합니다.");
        }
        if (hasOlder && !nextBefore.equals(items.getLast().position())) {
            throw new IllegalArgumentException("다음 위치는 마지막 반환 항목 위치여야 합니다.");
        }
        if (!hasOlder && nextBefore != null) {
            throw new IllegalArgumentException("이전 결과가 없으면 다음 위치도 없어야 합니다.");
        }
    }

    /** 로그 결과와 확인된 실제 발급 상태를 함께 표현합니다. */
    public record InquiryItem(
            Long memberId,
            Long couponId,
            Long issuanceId,
            Integer httpStatus,
            ReasonCode reasonCode,
            IssuanceStatus currentStatus,
            Instant occurredAt,
            InquiryPosition position
    ) {

        /** 공개 항목의 식별자, 로그 결과와 실제 발급 상태 관계를 검증합니다. */
        public InquiryItem {
            requirePositive(memberId, "memberId");
            requirePositive(couponId, "couponId");
            if (issuanceId != null) {
                requirePositive(issuanceId, "issuanceId");
            }
            if (httpStatus != null && (httpStatus < 100 || httpStatus > 599)) {
                throw new IllegalArgumentException("httpStatus는 100~599여야 합니다.");
            }
            if (httpStatus == null && reasonCode != null) {
                throw new IllegalArgumentException("HTTP 결과가 없으면 reasonCode도 없어야 합니다.");
            }
            if (httpStatus != null && httpStatus >= 400 && reasonCode == null) {
                throw new IllegalArgumentException("실패 HTTP 결과에는 reasonCode가 필요합니다.");
            }
            if (httpStatus != null && httpStatus < 400 && reasonCode != null) {
                throw new IllegalArgumentException("성공 HTTP 결과에는 reasonCode를 넣을 수 없습니다.");
            }
            if ((issuanceId == null) != (currentStatus == null)) {
                throw new IllegalArgumentException("실제 발급 ID와 현재 상태는 함께 존재해야 합니다.");
            }
            Objects.requireNonNull(occurredAt, "occurredAt");
            Objects.requireNonNull(position, "position");
        }

        private static void requirePositive(Long value, String name) {
            if (value == null || value <= 0L) {
                throw new IllegalArgumentException(name + "는 양수여야 합니다.");
            }
        }
    }
}
