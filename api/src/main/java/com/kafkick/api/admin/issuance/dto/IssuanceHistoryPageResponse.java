package com.kafkick.api.admin.issuance.dto;

import java.time.Instant;
import java.util.List;

import com.kafkick.core.coupon.IssuanceEventType;
import com.kafkick.core.coupon.IssuanceStatus;

/**
 * 쿠폰 발급 상태 전이를 최신 발생 시각부터 과거 방향으로 반환하는 목록 응답 초안입니다.
 *
 * <p>{@code nextBeforeCursor}는 현재 페이지 마지막 항목보다 더 오래된 전이를 요청할 때 사용하며,
 * {@code hasOlder}가 false이면 더 과거의 이력이 없습니다. 발급 코드는 운영 화면에서도 반드시 마스킹된
 * {@code issuanceCodeMasked}만 전달합니다.</p>
 *
 * @param items 최신 전이부터 과거 전이 순서로 정렬된 이력
 * @param nextBeforeCursor 다음 과거 페이지 조회에 사용할 cursor; 다음 페이지가 없으면 null
 * @param hasOlder 더 과거의 상태 전이 존재 여부
 */
public record IssuanceHistoryPageResponse(List<IssuanceHistoryItem> items, String nextBeforeCursor, boolean hasOlder) {
    /**
     * 선구축 단계의 JSON 필드 계약을 검증하기 위한 빈 목록 예시를 만듭니다.
     *
     * @return 다음 페이지가 없는 빈 발급 이력 목록
     */
    public static IssuanceHistoryPageResponse draft() { return new IssuanceHistoryPageResponse(List.of(), null, false); }

    /**
     * 한 번의 발급 상태 전이를 나타냅니다. 최초 발급은 이전 상태가 존재하지 않으므로
     * {@code fromStatus}가 null일 수 있고, {@code toStatus}와 {@code occurredAt}은 실제 전이를 설명합니다.
     *
     * @param issuanceId 발급권 식별자
     * @param issuanceCodeMasked 마스킹된 발급 코드
     * @param couponId 쿠폰 캠페인 회차 식별자
     * @param fromStatus 전이 이전 상태; 최초 발급이면 null
     * @param toStatus 전이 이후 상태
     * @param eventType 상태 전이를 일으킨 이벤트 유형
     * @param occurredAt 상태 전이 발생 시각
     */
    public record IssuanceHistoryItem(
            Long issuanceId,
            String issuanceCodeMasked,
            Long couponId,
            IssuanceStatus fromStatus,
            IssuanceStatus toStatus,
            IssuanceEventType eventType,
            Instant occurredAt
    ) { }
}
