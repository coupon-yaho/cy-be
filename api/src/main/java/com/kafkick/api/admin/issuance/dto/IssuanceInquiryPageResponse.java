package com.kafkick.api.admin.issuance.dto;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import com.kafkick.api.admin.issuance.IssuanceInquiryCursorCodec;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryResult;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.observation.ReasonCode;

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

    /** 공개 목록을 방어적으로 복사하고 페이지 cursor 일관성을 검증합니다. */
    public IssuanceInquiryPageResponse {
        items = List.copyOf(Objects.requireNonNull(items, "items"));
        if (hasOlder && nextBeforeCursor == null) {
            throw new IllegalArgumentException("이전 결과가 있으면 다음 cursor가 필요합니다.");
        }
        if (!hasOlder && nextBeforeCursor != null) {
            throw new IllegalArgumentException("이전 결과가 없으면 다음 cursor도 없어야 합니다.");
        }
    }

    /**
     * 선구축 단계의 JSON 필드 계약을 검증하기 위한 빈 목록 예시를 만듭니다.
     *
     * @return 다음 페이지가 없는 빈 발급 문의 목록
     */
    public static IssuanceInquiryPageResponse draft() { return new IssuanceInquiryPageResponse(List.of(), null, false); }

    /** Core 페이지를 내부 정렬 위치가 노출되지 않는 HTTP 응답으로 변환합니다. */
    public static IssuanceInquiryPageResponse from(
            AdminIssuanceInquiryResult result,
            IssuanceInquiryCursorCodec cursorCodec
    ) {
        Objects.requireNonNull(result, "result");
        Objects.requireNonNull(cursorCodec, "cursorCodec");
        // position은 서버 Keyset 계산 전용이므로 공개 항목으로 복사하지 않는다.
        List<IssuanceInquiryItem> items = result.items().stream()
                .map(IssuanceInquiryItem::from)
                .toList();
        String nextBeforeCursor = result.hasOlder()
                ? cursorCodec.encode(result.nextBefore())
                : null;
        return new IssuanceInquiryPageResponse(items, nextBeforeCursor, result.hasOlder());
    }

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
    ) {

        private static IssuanceInquiryItem from(AdminIssuanceInquiryResult.InquiryItem item) {
            return new IssuanceInquiryItem(
                    item.memberId(),
                    item.couponId(),
                    item.issuanceId(),
                    item.httpStatus(),
                    item.reasonCode(),
                    item.currentStatus(),
                    item.occurredAt());
        }
    }
}
