package com.kafkick.core.admin.issuancehistory;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.springframework.stereotype.Component;

import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryQuery.HistoryPosition;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryResult.HistoryItem;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryResult.HistorySummary;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistorySource.RawHistory;
import com.kafkick.core.coupon.domain.IssuanceEventType;

/** 발급 상태 변경 이력을 기술 중립 조회 페이지와 이벤트별 요약으로 계산합니다. */
@Component
public class IssuanceHistoryCalculator {

    private static final Comparator<RawHistory> NEWEST_FIRST = Comparator
            .comparing(RawHistory::occurredAt)
            .reversed()
            .thenComparing(Comparator.comparingLong(RawHistory::historyId).reversed());

    private final IssuanceCodeMasker issuanceCodeMasker;

    /** 발급 코드 마스킹 정책을 주입받습니다. */
    public IssuanceHistoryCalculator(IssuanceCodeMasker issuanceCodeMasker) {
        this.issuanceCodeMasker = Objects.requireNonNull(issuanceCodeMasker, "issuanceCodeMasker");
    }

    /**
     * 업무 필터 전체의 요약과 Cursor 이후 한 페이지의 이력 항목을 계산합니다.
     *
     * @param source 유효성 검증을 마친 원시 이력
     * @param query 업무 필터와 페이지 위치
     * @return 마스킹된 이력 페이지와 필터 모집단 요약
     */
    public AdminIssuanceHistoryResult calculate(
            AdminIssuanceHistorySource source,
            AdminIssuanceHistoryQuery query
    ) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(query, "query");

        // 업무 필터를 먼저 적용해 요약과 페이지가 같은 모집단을 보도록 합니다.
        List<RawHistory> population = source.histories().stream()
                .filter(history -> matchesBusinessFilter(history, query))
                .sorted(NEWEST_FIRST)
                .toList();

        // Summary는 Cursor와 limit을 적용하기 전의 전체 업무 모집단에서 계산합니다.
        HistorySummary summary = summarize(population);
        List<RawHistory> candidates = population.stream()
                .filter(history -> isOlderThanBefore(history, query.before()))
                .limit((long) query.limit() + 1L)
                .toList();

        // limit + 1번째 후보의 존재로 다음 페이지 여부를 판단합니다.
        boolean hasOlder = candidates.size() > query.limit();
        List<RawHistory> pageRows = hasOlder
                ? candidates.subList(0, query.limit())
                : candidates;
        List<HistoryItem> items = pageRows.stream().map(this::toHistoryItem).toList();
        HistoryPosition nextBefore = hasOlder ? positionOf(pageRows.getLast()) : null;

        return new AdminIssuanceHistoryResult(items, nextBefore, hasOlder, summary);
    }

    /** 업무 필터의 쿠폰, 기간, 이벤트 유형 조건을 모두 만족하는지 확인합니다. */
    private static boolean matchesBusinessFilter(
            RawHistory history,
            AdminIssuanceHistoryQuery query
    ) {
        if (query.couponId() != null && history.couponId() != query.couponId()) {
            return false;
        }
        if (query.fromInclusive() != null && history.occurredAt().isBefore(query.fromInclusive())) {
            return false;
        }
        if (query.toExclusive() != null && !history.occurredAt().isBefore(query.toExclusive())) {
            return false;
        }
        return query.eventType() == null || history.eventType() == query.eventType();
    }

    /** Cursor가 없거나, 동일 시각이면 더 작은 ID를 가진 과거 이력인지 확인합니다. */
    private static boolean isOlderThanBefore(RawHistory history, HistoryPosition before) {
        if (before == null) {
            return true;
        }
        // 같은 시각은 ID가 더 작은 행만 다음 페이지에 포함합니다.
        return history.occurredAt().isBefore(before.occurredAt())
                || (history.occurredAt().equals(before.occurredAt())
                && history.historyId() < before.historyId());
    }

    /** 업무 모집단의 이벤트 유형별 건수를 overflow 없이 합산합니다. */
    private static HistorySummary summarize(List<RawHistory> population) {
        long issueCount = 0L;
        long useCount = 0L;
        long cancelUseCount = 0L;
        long cancelCount = 0L;
        long expireCount = 0L;
        for (RawHistory history : population) {
            IssuanceEventType eventType = history.eventType();
            switch (eventType) {
                case ISSUE -> issueCount = Math.incrementExact(issueCount);
                case USE -> useCount = Math.incrementExact(useCount);
                case CANCEL_USE -> cancelUseCount = Math.incrementExact(cancelUseCount);
                case CANCEL -> cancelCount = Math.incrementExact(cancelCount);
                case EXPIRE -> expireCount = Math.incrementExact(expireCount);
            }
        }
        long totalCount = Math.addExact(issueCount, useCount);
        totalCount = Math.addExact(totalCount, cancelUseCount);
        totalCount = Math.addExact(totalCount, cancelCount);
        totalCount = Math.addExact(totalCount, expireCount);
        return new HistorySummary(
                totalCount, issueCount, useCount, cancelUseCount, cancelCount, expireCount);
    }

    /** 원시 이력을 HTTP 항목에 노출 가능한 마스킹 이력으로 바꿉니다. */
    private HistoryItem toHistoryItem(RawHistory history) {
        return new HistoryItem(
                history.issuanceId(),
                issuanceCodeMasker.mask(history.issuanceCode()),
                history.couponId(),
                history.fromStatus(),
                history.toStatus(),
                history.eventType(),
                history.occurredAt());
    }

    /** 다음 Keyset Cursor에 사용할 마지막 반환 항목의 위치를 만듭니다. */
    private static HistoryPosition positionOf(RawHistory history) {
        return new HistoryPosition(history.occurredAt(), history.historyId());
    }
}
