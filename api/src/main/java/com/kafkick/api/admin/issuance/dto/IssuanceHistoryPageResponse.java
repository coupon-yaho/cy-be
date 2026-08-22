package com.kafkick.api.admin.issuance.dto;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import com.kafkick.api.admin.issuance.IssuanceHistoryCursorCodec;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryResult;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryResult.HistorySummary;
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
 * @param summary cursor와 limit 전 업무 필터 모집단의 이벤트별 건수
 */
public record IssuanceHistoryPageResponse(
        List<IssuanceHistoryItem> items,
        String nextBeforeCursor,
        boolean hasOlder,
        IssuanceHistorySummary summary
) {

    /** 목록을 불변으로 보호하고 필수 응답 부분의 null을 거부합니다. */
    public IssuanceHistoryPageResponse {
        Objects.requireNonNull(items, "items");
        items = List.copyOf(items);
        Objects.requireNonNull(summary, "summary");
    }

    /** 기존 선구축 호출부에 비어 있지 않은 기본 요약을 제공하는 호환 생성자입니다. */
    public IssuanceHistoryPageResponse(
            List<IssuanceHistoryItem> items,
            String nextBeforeCursor,
            boolean hasOlder
    ) {
        this(items, nextBeforeCursor, hasOlder,
                new IssuanceHistorySummary(0L, 0L, 0L, 0L, 0L, 0L));
    }

    /**
     * 선구축 단계의 JSON 필드 계약을 검증하기 위한 빈 목록 예시를 만듭니다.
     *
     * @return 다음 페이지가 없는 빈 발급 이력 목록
     */
    public static IssuanceHistoryPageResponse draft() {
        return new IssuanceHistoryPageResponse(
                List.of(), null, false, new IssuanceHistorySummary(0L, 0L, 0L, 0L, 0L, 0L));
    }

    /**
     * Core 발급 이력 결과를 HTTP 항목, cursor와 요약 응답으로 변환합니다.
     *
     * @param result 마스킹과 페이지 계산을 마친 Core 결과
     * @param cursorCodec 다음 Keyset 위치의 HTTP cursor 변환기
     * @return 외부 발급 이력 페이지 응답
     */
    public static IssuanceHistoryPageResponse from(
            AdminIssuanceHistoryResult result,
            IssuanceHistoryCursorCodec cursorCodec
    ) {
        Objects.requireNonNull(result, "result");
        Objects.requireNonNull(cursorCodec, "cursorCodec");
        List<IssuanceHistoryItem> items = result.items().stream()
                .map(IssuanceHistoryPageResponse::fromItem)
                .toList();
        // 다음 페이지가 실제로 있을 때만 Core 위치를 외부 cursor로 노출합니다.
        String nextBeforeCursor = result.hasOlder()
                ? cursorCodec.encode(result.nextBefore())
                : null;
        HistorySummary summary = result.summary();
        return new IssuanceHistoryPageResponse(
                items,
                nextBeforeCursor,
                result.hasOlder(),
                new IssuanceHistorySummary(
                        summary.totalCount(),
                        summary.issueCount(),
                        summary.useCount(),
                        summary.cancelUseCount(),
                        summary.cancelCount(),
                        summary.expireCount()));
    }

    /** Core가 이미 마스킹한 한 항목을 HTTP 응답 필드로 복사합니다. */
    private static IssuanceHistoryItem fromItem(AdminIssuanceHistoryResult.HistoryItem item) {
        return new IssuanceHistoryItem(
                item.issuanceId(),
                item.issuanceCodeMasked(),
                item.couponId(),
                item.fromStatus(),
                item.toStatus(),
                item.eventType(),
                item.occurredAt());
    }

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

    /**
     * 업무 필터 전체 모집단의 발급 상태 변경 이벤트별 건수입니다.
     *
     * @param totalCount 전체 상태 변경 건수
     * @param issueCount 발급 건수
     * @param useCount 사용 건수
     * @param cancelUseCount 사용 취소 건수
     * @param cancelCount 발급 취소 건수
     * @param expireCount 만료 건수
     */
    public record IssuanceHistorySummary(
            long totalCount,
            long issueCount,
            long useCount,
            long cancelUseCount,
            long cancelCount,
            long expireCount
    ) { }
}
