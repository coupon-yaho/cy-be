package com.kafkick.core.admin.inquiry;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.kafkick.core.coupon.IssuanceEventType;
import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.core.observation.EventType;
import com.kafkick.core.observation.ReasonCode;

/** 저장 기술에 독립적인 회원 발급 문의의 세 DB 조회 결과입니다. */
public record AdminIssuanceInquirySource(
        List<RawAttempt> attempts,
        List<RawIssuance> issuances,
        List<RawHistoryLink> histories
) {

    /** 원천 목록의 null, 중복 ID와 외부 변경을 차단합니다. */
    public AdminIssuanceInquirySource {
        attempts = immutableWithUniqueIds(attempts, RawAttempt::attemptId, "attemptId");
        issuances = immutableWithUniqueIds(issuances, RawIssuance::issuanceId, "issuanceId");
        histories = immutableWithUniqueIds(histories, RawHistoryLink::historyId, "historyId");
    }

    /** 발급 진입·시도·결과 로그의 조회 행입니다. */
    public record RawAttempt(
            long attemptId,
            EventType eventType,
            String requestId,
            long memberId,
            long couponId,
            Long issuanceId,
            Integer httpStatus,
            ReasonCode reasonCode,
            Instant occurredAt
    ) {

        /** 문의 모집단과 이벤트별 필드 관계를 검증합니다. */
        public RawAttempt {
            requirePositive(attemptId, "attemptId");
            Objects.requireNonNull(eventType, "eventType");
            requireText(requestId, "requestId");
            requirePositive(memberId, "memberId");
            requirePositive(couponId, "couponId");
            Objects.requireNonNull(occurredAt, "occurredAt");
            if (issuanceId != null) {
                requirePositive(issuanceId, "issuanceId");
            }
            switch (eventType) {
                case ISSUE_ATTEMPT -> validateAttemptFields(issuanceId, httpStatus, reasonCode);
                case ENTRY_RESULT -> validateEntryResultFields(issuanceId, httpStatus, reasonCode);
                case ISSUE_RESULT -> validateIssueResultFields(issuanceId, httpStatus, reasonCode);
                case QUEUE_ADMITTED -> throw new IllegalArgumentException(
                        "QUEUE_ADMITTED는 발급 문의 모집단이 아닙니다.");
            }
        }

        private static void validateAttemptFields(
                Long issuanceId,
                Integer httpStatus,
                ReasonCode reasonCode
        ) {
            if (issuanceId != null || httpStatus != null || reasonCode != null) {
                throw new IllegalArgumentException("ISSUE_ATTEMPT 결과 필드는 null이어야 합니다.");
            }
        }

        private static void validateEntryResultFields(
                Long issuanceId,
                Integer httpStatus,
                ReasonCode reasonCode
        ) {
            if (issuanceId != null) {
                throw new IllegalArgumentException("ENTRY_RESULT에는 issuanceId를 넣을 수 없습니다.");
            }
            validateHttpResult(httpStatus, reasonCode);
            if (httpStatus < 400 && httpStatus != 200 && httpStatus != 202) {
                throw new IllegalArgumentException("성공 ENTRY_RESULT는 HTTP 200 또는 202여야 합니다.");
            }
        }

        private static void validateIssueResultFields(
                Long issuanceId,
                Integer httpStatus,
                ReasonCode reasonCode
        ) {
            validateHttpResult(httpStatus, reasonCode);
            if (httpStatus < 400 && httpStatus != 201) {
                throw new IllegalArgumentException("성공 ISSUE_RESULT는 HTTP 201이어야 합니다.");
            }
            if (httpStatus == 201 && issuanceId == null) {
                throw new IllegalArgumentException("HTTP 201 ISSUE_RESULT에는 issuanceId가 필요합니다.");
            }
            if (httpStatus != 201 && issuanceId != null) {
                throw new IllegalArgumentException("HTTP 201이 아닌 결과에는 issuanceId를 넣을 수 없습니다.");
            }
        }

        private static void validateHttpResult(Integer httpStatus, ReasonCode reasonCode) {
            if (httpStatus == null || httpStatus < 100 || httpStatus > 599) {
                throw new IllegalArgumentException("HTTP 상태는 100~599여야 합니다.");
            }
            if (httpStatus >= 400 && reasonCode == null) {
                throw new IllegalArgumentException("실패 결과에는 reasonCode가 필요합니다.");
            }
            if (httpStatus < 400 && reasonCode != null) {
                throw new IllegalArgumentException("성공 결과에는 reasonCode를 넣을 수 없습니다.");
            }
        }
    }

    /** 실제 발급과 현재 상태의 권위 DB 조회 행입니다. */
    public record RawIssuance(
            long issuanceId,
            long memberId,
            long couponId,
            IssuanceStatus currentStatus,
            Instant issuedAt
    ) {

        /** 발급 식별자, 현재 상태와 발급 시각을 검증합니다. */
        public RawIssuance {
            requirePositive(issuanceId, "issuanceId");
            requirePositive(memberId, "memberId");
            requirePositive(couponId, "couponId");
            Objects.requireNonNull(currentStatus, "currentStatus");
            Objects.requireNonNull(issuedAt, "issuedAt");
        }
    }

    /** ISSUE 이력의 requestId로 실제 발급을 연결하는 조회 행입니다. */
    public record RawHistoryLink(
            long historyId,
            long issuanceId,
            IssuanceEventType eventType,
            String requestId,
            Instant occurredAt
    ) {

        /** ISSUE 연결에 필요한 식별자와 시각을 검증합니다. */
        public RawHistoryLink {
            requirePositive(historyId, "historyId");
            requirePositive(issuanceId, "issuanceId");
            Objects.requireNonNull(eventType, "eventType");
            if (eventType != IssuanceEventType.ISSUE) {
                throw new IllegalArgumentException("RawHistoryLink는 ISSUE 이벤트만 허용합니다.");
            }
            requireText(requestId, "requestId");
            Objects.requireNonNull(occurredAt, "occurredAt");
        }
    }

    private static <T> List<T> immutableWithUniqueIds(
            List<T> rows,
            java.util.function.ToLongFunction<T> idExtractor,
            String idName
    ) {
        Objects.requireNonNull(rows, "rows");
        List<T> copy = List.copyOf(rows);
        Set<Long> ids = new HashSet<>();
        for (T row : copy) {
            if (!ids.add(idExtractor.applyAsLong(row))) {
                throw new IllegalArgumentException(idName + "는 중복될 수 없습니다.");
            }
        }
        return copy;
    }

    private static void requirePositive(long value, String name) {
        if (value <= 0L) {
            throw new IllegalArgumentException(name + "는 양수여야 합니다.");
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank() || value.length() > 36) {
            throw new IllegalArgumentException(name + " 형식이 올바르지 않습니다.");
        }
    }
}
