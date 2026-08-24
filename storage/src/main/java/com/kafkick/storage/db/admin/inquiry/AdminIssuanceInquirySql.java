package com.kafkick.storage.db.admin.inquiry;

import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryQuery.InquiryPosition;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryQuery.SourceKind;

/** 회원 발급 문의가 관측 DB에서 실행하는 고정 SQL과 원천별 keyset 조건입니다. */
public final class AdminIssuanceInquirySql {

    public static final String EXISTENCE = """
            SELECT
                EXISTS(SELECT 1 FROM members m WHERE m.id = :memberId) AS member_exists,
                CASE WHEN :couponFilterApplied = 0 THEN 1
                     ELSE EXISTS(SELECT 1 FROM coupons c WHERE c.id = :couponId)
                END AS coupon_exists
            """;

    private static final String ATTEMPT_BASE = """
            WITH ranked_attempts AS (
                SELECT ia.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY ia.member_id, ia.coupon_id, ia.request_id
                           ORDER BY
                               CASE WHEN ia.event_type IN ('ENTRY_RESULT', 'ISSUE_RESULT')
                                    THEN 1 ELSE 0 END DESC,
                               ia.occurred_at DESC,
                               ia.id DESC
                       ) AS representative_rank
                  FROM issue_attempts ia
                 WHERE ia.member_id = :memberId
                   AND ia.event_type IN ('ENTRY_RESULT', 'ISSUE_ATTEMPT', 'ISSUE_RESULT')
                   AND ia.occurred_at <= :snapshotAt
            )
            SELECT id, event_type, request_id, member_id, coupon_id,
                   issuance_id, http_status, reason_code, occurred_at
              FROM ranked_attempts
             WHERE representative_rank = 1
               AND (:couponId IS NULL OR coupon_id = :couponId)
               AND (:httpStatus IS NULL OR http_status = :httpStatus)
               AND (:reasonCode IS NULL OR reason_code = :reasonCode)
            %s
             ORDER BY occurred_at DESC, id DESC
             LIMIT :fetchLimit
            """;

    private static final String ISSUANCE_BASE = """
            SELECT i.id, i.member_id, i.coupon_id, i.status, i.issued_at
              FROM issuances i
             WHERE i.member_id = :memberId
               AND (:couponId IS NULL OR i.coupon_id = :couponId)
               AND i.issued_at <= :snapshotAt
               AND NOT EXISTS (
                   SELECT 1
                     FROM issue_attempts direct_attempt
                    WHERE direct_attempt.member_id = i.member_id
                      AND direct_attempt.coupon_id = i.coupon_id
                      AND direct_attempt.issuance_id = i.id
                      AND direct_attempt.event_type IN ('ENTRY_RESULT', 'ISSUE_ATTEMPT', 'ISSUE_RESULT')
                      AND direct_attempt.occurred_at <= :snapshotAt
               )
               AND NOT EXISTS (
                   SELECT 1
                     FROM issuance_histories issue_history
                     JOIN issue_attempts history_attempt
                       ON history_attempt.member_id = i.member_id
                      AND history_attempt.coupon_id = i.coupon_id
                      AND history_attempt.request_id = issue_history.request_id
                      AND history_attempt.event_type IN ('ENTRY_RESULT', 'ISSUE_ATTEMPT', 'ISSUE_RESULT')
                      AND history_attempt.occurred_at <= :snapshotAt
                    WHERE issue_history.issuance_id = i.id
                      AND issue_history.event_type = 'ISSUE'
                      AND issue_history.created_at <= :snapshotAt
               )
            %s
             ORDER BY i.issued_at DESC, i.id DESC
             LIMIT :fetchLimit
            """;

    public static final String DIRECT_ISSUANCES = """
            SELECT i.id, i.member_id, i.coupon_id, i.status, i.issued_at
              FROM issuances i
             WHERE i.id IN (:issuanceIds)
               AND i.member_id = :memberId
               AND (:couponId IS NULL OR i.coupon_id = :couponId)
               AND i.issued_at <= :snapshotAt
            """;

    public static final String ISSUE_HISTORIES = """
            SELECT h.id, h.issuance_id, h.event_type, h.request_id, h.created_at,
                   i.member_id, i.coupon_id, i.status, i.issued_at
              FROM issuances i
              JOIN issuance_histories h ON h.issuance_id = i.id
             WHERE i.member_id = :memberId
               AND (:couponId IS NULL OR i.coupon_id = :couponId)
               AND i.issued_at <= :snapshotAt
               AND h.event_type = 'ISSUE'
               AND h.request_id IN (:requestIds)
               AND h.created_at <= :snapshotAt
            """;

    private AdminIssuanceInquirySql() {
    }

    /** 대표 행을 고른 뒤 원천 ATTEMPT의 동률 순서까지 적용한 고정 SQL을 반환합니다. */
    public static String attempts(InquiryPosition before) {
        // ISSUANCE cursor와 같은 시각의 ATTEMPT는 전역 순서상 더 오래되어 포함한다.
        String cursor = before == null ? ""
                : before.sourceKind() == SourceKind.ATTEMPT
                        ? "AND (occurred_at < :beforeOccurredAt OR "
                                + "(occurred_at = :beforeOccurredAt AND id < :beforeSourceId))"
                        : "AND occurred_at <= :beforeOccurredAt";
        return ATTEMPT_BASE.formatted(cursor);
    }

    /** 유실된 attempt의 발급을 보존하며 원천 ISSUANCE의 동률 순서까지 적용한 SQL을 반환합니다. */
    public static String issuances(InquiryPosition before) {
        // ATTEMPT cursor와 같은 시각의 ISSUANCE는 전역 순서상 더 최신이므로 제외한다.
        String cursor = before == null ? ""
                : before.sourceKind() == SourceKind.ISSUANCE
                        ? "AND (i.issued_at < :beforeOccurredAt OR "
                                + "(i.issued_at = :beforeOccurredAt AND i.id < :beforeSourceId))"
                        : "AND i.issued_at < :beforeOccurredAt";
        return ISSUANCE_BASE.formatted(cursor);
    }
}
