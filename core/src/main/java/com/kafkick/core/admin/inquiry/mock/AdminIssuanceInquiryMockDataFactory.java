package com.kafkick.core.admin.inquiry.mock;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import org.springframework.stereotype.Component;

import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySourceReader;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource.RawAttempt;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource.RawHistoryLink;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource.RawIssuance;
import com.kafkick.core.coupon.domain.IssuanceEventType;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.observation.EventType;
import com.kafkick.core.observation.ReasonCode;

/** 실제 Repository를 대신해 세 DB 테이블 형태의 고정 문의 원천 행을 제공합니다. */
@Component
public class AdminIssuanceInquiryMockDataFactory implements AdminIssuanceInquirySourceReader {

    private static final Instant FIXTURE_V1_ANCHOR = Instant.parse("2026-08-23T00:00:00Z");
    // 요청 시각이 바뀌어도 같은 Cursor가 같은 행을 가리키도록 행 시각은 기준점에 고정한다.
    private static final Instant NEWEST_FIXED_ROW = FIXTURE_V1_ANCHOR.minus(Duration.ofMinutes(2));

    /**
     * 모든 Factory와 요청에서 동일한 fixture-v1 원천 행을 반환합니다.
     *
     * @param snapshotAt 한 요청에서 확정한 관측 기준 시각
     * @return issue_attempts, issuances, issuance_histories 형태의 원천 행
     */
    @Override
    public AdminIssuanceInquirySource create(Instant snapshotAt) {
        Objects.requireNonNull(snapshotAt, "snapshotAt");
        if (snapshotAt.isBefore(NEWEST_FIXED_ROW)) {
            throw new IllegalArgumentException("snapshotAt은 fixture-v1 최신 행보다 빠를 수 없습니다.");
        }
        return new AdminIssuanceInquirySource(attempts(), issuances(), histories());
    }

    /** 결과·실패·재시도·동률과 같은 issue_attempts 조회 시나리오를 만듭니다. */
    private static List<RawAttempt> attempts() {
        return List.of(
                // 같은 requestId의 ATTEMPT와 RESULT 중 결과 행 하나만 대표 문의로 남는다.
                attempt(101L, EventType.ISSUE_ATTEMPT, "inquiry-direct", 1_001L, 2_001L,
                        null, null, null, minutesBefore(11)),
                attempt(102L, EventType.ISSUE_RESULT, "inquiry-direct", 1_001L, 2_001L,
                        5_001L, 201, null, minutesBefore(10)),
                // issuanceId 없이 ISSUE 이력의 requestId를 통해 실제 발급과 연결되는 사례다.
                attempt(103L, EventType.ENTRY_RESULT, "inquiry-history", 1_001L, 2_002L,
                        null, 200, null, minutesBefore(9)),
                attempt(104L, EventType.ISSUE_RESULT, "inquiry-policy", 1_001L, 2_003L,
                        null, 409, ReasonCode.ALREADY_ISSUED, minutesBefore(8)),
                attempt(105L, EventType.ISSUE_RESULT, "inquiry-system", 1_001L, 2_003L,
                        null, 500, ReasonCode.INTERNAL_ERROR, minutesBefore(7)),
                // 로그에는 ID가 있지만 DB 행이 없으므로 실제 발급으로 확정하지 않는 사례다.
                attempt(106L, EventType.ISSUE_RESULT, "inquiry-unconfirmed", 1_001L, 2_003L,
                        5_999L, 201, null, minutesBefore(6)),
                // 회원·캠페인이 같아도 requestId가 다른 두 재시도는 별도 결과로 남는다.
                attempt(107L, EventType.ISSUE_RESULT, "inquiry-retry-a", 1_001L, 2_004L,
                        null, 409, ReasonCode.ALREADY_ISSUED, minutesBefore(5)),
                attempt(108L, EventType.ISSUE_RESULT, "inquiry-retry-b", 1_001L, 2_004L,
                        null, 503, ReasonCode.TEMPORARILY_UNAVAILABLE, minutesBefore(4)),
                // 같은 시각의 행도 sourceId로 순서와 Cursor 경계를 결정해야 한다.
                attempt(109L, EventType.ISSUE_RESULT, "inquiry-tied-a", 1_001L, 2_006L,
                        null, 500, ReasonCode.INTERNAL_ERROR, minutesBefore(2)),
                attempt(110L, EventType.ISSUE_RESULT, "inquiry-tied-b", 1_001L, 2_006L,
                        null, 503, ReasonCode.TEMPORARILY_UNAVAILABLE, minutesBefore(2)));
    }

    /** 실제 발급 연결 두 건과 로그가 유실된 DB 단독 발급 한 건을 만듭니다. */
    private static List<RawIssuance> issuances() {
        return List.of(
                new RawIssuance(
                        5_001L, 1_001L, 2_001L, IssuanceStatus.USED,
                        FIXTURE_V1_ANCHOR.minus(Duration.ofMinutes(10)).minusSeconds(30)),
                new RawIssuance(
                        5_002L, 1_001L, 2_002L, IssuanceStatus.CANCELLED,
                        FIXTURE_V1_ANCHOR.minus(Duration.ofMinutes(9)).minusSeconds(30)),
                new RawIssuance(
                        5_003L, 1_001L, 2_005L, IssuanceStatus.EXPIRED,
                        FIXTURE_V1_ANCHOR.minus(Duration.ofMinutes(4)).minusSeconds(30)));
    }

    /** requestId로 두 번째 실제 발급을 연결하는 ISSUE 이력 한 행을 만듭니다. */
    private static List<RawHistoryLink> histories() {
        return List.of(new RawHistoryLink(
                9_001L,
                5_002L,
                IssuanceEventType.ISSUE,
                "inquiry-history",
                FIXTURE_V1_ANCHOR.minus(Duration.ofMinutes(9)).minusSeconds(30)));
    }

    private static RawAttempt attempt(
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
        return new RawAttempt(
                attemptId, eventType, requestId, memberId, couponId,
                issuanceId, httpStatus, reasonCode, occurredAt);
    }

    private static Instant minutesBefore(long minutes) {
        return FIXTURE_V1_ANCHOR.minus(Duration.ofMinutes(minutes));
    }
}
