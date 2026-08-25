package com.kafkick.core.admin.issuancehistory;

import java.util.List;
import java.util.Objects;

import org.springframework.stereotype.Component;

import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryQuery.HistoryPosition;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryResult.HistoryItem;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryResult.HistorySummary;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistorySource.RawHistory;

/** 발급 상태 변경 이력을 기술 중립 조회 페이지와 이벤트별 요약으로 계산합니다. */
@Component
public class IssuanceHistoryCalculator {

    private final IssuanceCodeMasker issuanceCodeMasker;

    /** 발급 코드 마스킹 정책을 주입받습니다. */
    public IssuanceHistoryCalculator(IssuanceCodeMasker issuanceCodeMasker) {
        this.issuanceCodeMasker = Objects.requireNonNull(issuanceCodeMasker, "issuanceCodeMasker");
    }

    /**
     * Reader가 제한한 후보를 마스킹하여 한 페이지로 조립합니다.
     *
     * @param readResult DB가 필터·정렬·Cursor를 적용한 결과
     * @param limit API가 요청한 페이지 크기
     * @return 마스킹된 이력 페이지와 필터 모집단 요약
     */
    public AdminIssuanceHistoryResult calculate(
            AdminIssuanceHistoryReadResult readResult,
            int limit
    ) {
        Objects.requireNonNull(readResult, "readResult");
        List<RawHistory> candidates = readResult.candidates();

        // limit + 1번째 후보의 존재로 다음 페이지 여부를 판단합니다.
        // limit + 1번째 후보의 존재로 다음 페이지 여부를 판단합니다.
        boolean hasOlder = candidates.size() > limit;
        List<RawHistory> pageRows = hasOlder
                ? candidates.subList(0, limit)
                : candidates;
        List<HistoryItem> items = pageRows.stream().map(this::toHistoryItem).toList();
        HistoryPosition nextBefore = hasOlder ? positionOf(pageRows.getLast()) : null;

        return new AdminIssuanceHistoryResult(items, nextBefore, hasOlder, readResult.summary());
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
