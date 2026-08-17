package com.kafkick.api.admin.issuance.dto;

import java.time.Instant;
import java.util.List;

import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.core.admin.ReasonCode;

/**
 * 회원의 발급 시도 결과와 권위 DB의 현재 발급 상태를 과거 방향으로 반환하는 목록 응답 초안입니다.
 *
 * <p>시도 로그가 유실되고 권위 DB 발급 건만 존재할 수 있어 {@code httpStatus}와 {@code reasonCode}는
 * null일 수 있습니다. 실패한 시도는 발급 엔터티가 생성되지 않을 수 있으므로 {@code couponId},
 * {@code issuanceId}, {@code currentStatus}도 데이터 원천에 따라 nullable입니다.</p>
 *
 * @param items 최신 문의 결과부터 과거 결과 순서로 정렬된 목록
 * @param nextBeforeCursor 다음 과거 페이지 조회에 사용할 cursor; 다음 페이지가 없으면 null
 * @param hasOlder 더 과거의 문의 결과 존재 여부
 */
public record IssuanceInquiryPageResponse(List<IssuanceInquiryItem> items, String nextBeforeCursor, boolean hasOlder) {
    /**
     * 선구축 단계의 JSON 필드 계약을 검증하기 위한 빈 목록 예시를 만듭니다.
     *
     * @return 다음 페이지가 없는 빈 발급 문의 목록
     */
    public static IssuanceInquiryPageResponse draft() { return new IssuanceInquiryPageResponse(List.of(), null, false); }

    /**
     * 한 회원의 발급 시도 결과와, 존재할 경우 연결된 발급권의 현재 상태를 나타냅니다.
     *
     * @param memberId 문의 대상 회원 식별자
     * @param couponId 관련 쿠폰 캠페인 회차 식별자; 확인할 수 없으면 null
     * @param issuanceId 생성된 발급권 식별자; 발급 실패 또는 미확인이면 null
     * @param httpStatus 발급 시도 HTTP 상태; 시도 로그가 없으면 null
     * @param reasonCode 실패·정책 결과 사유
     * @param currentStatus 권위 DB의 현재 발급권 상태; 발급권이 없으면 null
     * @param occurredAt 발급 시도 또는 권위 상태가 기록된 시각
     */
    public record IssuanceInquiryItem(
            Long memberId,
            Long couponId,
            Long issuanceId,
            Integer httpStatus,
            ReasonCode reasonCode,
            IssuanceStatus currentStatus,
            Instant occurredAt
    ) { }
}
