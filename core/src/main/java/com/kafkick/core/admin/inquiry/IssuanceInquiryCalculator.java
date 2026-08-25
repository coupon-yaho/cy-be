package com.kafkick.core.admin.inquiry;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryQuery.InquiryPosition;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryQuery.SourceKind;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryResult.InquiryItem;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource.RawAttempt;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource.RawHistoryLink;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource.RawIssuance;

/** 세 DB 조회 결과를 회원 발급 문의 페이지로 연결하고 조립합니다. */
@Component
public class IssuanceInquiryCalculator {

    private static final Comparator<InquiryPosition> NEWEST_POSITION_FIRST = Comparator
            .comparing(InquiryPosition::occurredAt)
            .reversed()
            // ATTEMPT와 ISSUANCE는 ID 채번 공간이 다르므로 동률 시 원천 종류까지 정렬 키에 포함한다.
            .thenComparing(Comparator.comparing(InquiryPosition::sourceKind).reversed())
            .thenComparing(Comparator.comparingLong(InquiryPosition::sourceId).reversed());

    private static final Comparator<InquiryItem> NEWEST_ITEM_FIRST = Comparator.comparing(
            InquiryItem::position, NEWEST_POSITION_FIRST);

    /**
     * 정확한 연결 키로 실제 발급 상태를 보강한 뒤 결과 필터·전역 정렬·Cursor·페이지를 적용합니다.
     *
     * <p>원천은 query의 회원과 선택 쿠폰으로 이미 제한된 후보여야 합니다. 대표 attempt 선택이
     * 먼저이므로 HTTP 결과와 사유 필터는 여기서 적용합니다.
     */
    public AdminIssuanceInquiryResult calculate(
            AdminIssuanceInquirySource source,
            AdminIssuanceInquiryQuery query
    ) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(query, "query");

        Map<Long, RawIssuance> issuanceById = new HashMap<>();
        for (RawIssuance issuance : source.issuances()) {
            issuanceById.put(issuance.issuanceId(), issuance);
        }
        Map<RequestScope, RawHistoryLink> issueHistoryByRequest = newestIssueHistoryByRequest(
                source.histories(), issuanceById);
        Set<Long> linkedIssuanceIds = new HashSet<>();
        List<InquiryItem> joined = new ArrayList<>();

        for (RawAttempt attempt : representativeAttempts(source.attempts())) {
            // 회원·캠페인·발생 시각은 재시도를 오연결할 수 있어 정확한 ID와 requestId만 사용한다.
            RawIssuance issuance = findLinkedIssuance(
                    attempt, issuanceById, issueHistoryByRequest);
            if (issuance != null) {
                linkedIssuanceIds.add(issuance.issuanceId());
            }
            joined.add(toAttemptItem(attempt, issuance));
        }
        for (RawIssuance issuance : source.issuances()) {
            // 시도 로그가 유실돼도 권위 DB에 남은 실제 발급은 문의 결과에서 사라지면 안 된다.
            if (!linkedIssuanceIds.contains(issuance.issuanceId())) {
                joined.add(toIssuanceItem(issuance));
            }
        }

        // 로그 필터는 DB 상태를 보강한 뒤 적용하며, null 로그 필드를 임의 상태로 치환하지 않는다.
        List<InquiryItem> candidates = joined.stream()
                .filter(item -> matchesFilters(item, query))
                .sorted(NEWEST_ITEM_FIRST)
                .filter(item -> isOlderThanBefore(item.position(), query.before()))
                // 한 건을 더 확인해야 반환 목록과 별개로 다음 페이지 존재 여부를 판정할 수 있다.
                .limit((long) query.limit() + 1L)
                .toList();
        boolean hasOlder = candidates.size() > query.limit();
        List<InquiryItem> pageItems = hasOlder
                ? candidates.subList(0, query.limit())
                : candidates;
        InquiryPosition nextBefore = hasOlder
                ? pageItems.getLast().position()
                : null;
        return new AdminIssuanceInquiryResult(pageItems, nextBefore, hasOlder);
    }

    /** 결과 이벤트가 있으면 시도 이벤트보다 우선하고, 같은 종류에서는 최신 행을 고릅니다. */
    static List<RawAttempt> representativeAttempts(List<RawAttempt> attempts) {
        Map<RequestScope, RawAttempt> representativeByRequest = new HashMap<>();
        for (RawAttempt attempt : attempts) {
            // 같은 requestId의 시도·결과는 결과 우선 규칙으로 먼저 대표 한 행을 고른다.
            // 회원·캠페인·requestId 범위가 다른 재시도는 서로 다른 문의 행으로 유지된다.
            representativeByRequest.merge(
                    RequestScope.of(attempt),
                    attempt,
                    IssuanceInquiryCalculator::newerRepresentative);
        }
        return List.copyOf(representativeByRequest.values());
    }

    private static RawAttempt newerRepresentative(RawAttempt left, RawAttempt right) {
        boolean leftResult = isResult(left);
        boolean rightResult = isResult(right);
        if (leftResult != rightResult) {
            return leftResult ? left : right;
        }
        int occurredComparison = left.occurredAt().compareTo(right.occurredAt());
        if (occurredComparison != 0) {
            return occurredComparison > 0 ? left : right;
        }
        return left.attemptId() >= right.attemptId() ? left : right;
    }

    private static boolean isResult(RawAttempt attempt) {
        return attempt.eventType() != com.kafkick.core.observation.EventType.ISSUE_ATTEMPT;
    }

    /** 같은 requestId에 비정상적으로 여러 ISSUE 이력이 있으면 최신 연결 행을 사용합니다. */
    private static Map<RequestScope, RawHistoryLink> newestIssueHistoryByRequest(
            List<RawHistoryLink> histories,
            Map<Long, RawIssuance> issuanceById
    ) {
        Map<RequestScope, RawHistoryLink> byRequest = new HashMap<>();
        for (RawHistoryLink history : histories) {
            RawIssuance issuance = issuanceById.get(history.issuanceId());
            if (issuance == null) {
                continue;
            }
            RequestScope scope = new RequestScope(
                    issuance.memberId(), issuance.couponId(), history.requestId());
            byRequest.merge(scope, history, (left, right) -> {
                int occurredComparison = left.occurredAt().compareTo(right.occurredAt());
                if (occurredComparison != 0) {
                    return occurredComparison > 0 ? left : right;
                }
                return left.historyId() >= right.historyId() ? left : right;
            });
        }
        return byRequest;
    }

    /** 직접 issuanceId를 먼저 보고, 없거나 유실된 경우 ISSUE requestId 연결을 사용합니다. */
    private static RawIssuance findLinkedIssuance(
            RawAttempt attempt,
            Map<Long, RawIssuance> issuanceById,
            Map<RequestScope, RawHistoryLink> issueHistoryByRequest
    ) {
        RawIssuance direct = issuanceById.get(attempt.issuanceId());
        if (direct != null
                && direct.memberId() == attempt.memberId()
                && direct.couponId() == attempt.couponId()) {
            return direct;
        }
        // 직접 ID가 없거나 문의 범위와 다르면 ISSUE 이력의 동일 requestId로 보조 연결한다.
        RawHistoryLink history = issueHistoryByRequest.get(RequestScope.of(attempt));
        return history == null ? null : issuanceById.get(history.issuanceId());
    }

    private static InquiryItem toAttemptItem(RawAttempt attempt, RawIssuance issuance) {
        InquiryPosition position = new InquiryPosition(
                attempt.occurredAt(), SourceKind.ATTEMPT, attempt.attemptId());
        // 로그의 issuanceId가 있어도 실제 DB 행이 없으면 발급 ID와 상태를 공개하지 않는다.
        return new InquiryItem(
                attempt.memberId(),
                attempt.couponId(),
                issuance == null ? null : issuance.issuanceId(),
                attempt.httpStatus(),
                attempt.reasonCode(),
                issuance == null ? null : issuance.currentStatus(),
                attempt.occurredAt(),
                position);
    }

    private static InquiryItem toIssuanceItem(RawIssuance issuance) {
        InquiryPosition position = new InquiryPosition(
                issuance.issuedAt(), SourceKind.ISSUANCE, issuance.issuanceId());
        // DB 단독 발급은 로그를 추정하지 않고 HTTP 결과와 사유를 null로 유지한다.
        return new InquiryItem(
                issuance.memberId(),
                issuance.couponId(),
                issuance.issuanceId(),
                null,
                null,
                issuance.currentStatus(),
                issuance.issuedAt(),
                position);
    }

    private static boolean matchesFilters(
            InquiryItem item,
            AdminIssuanceInquiryQuery query
    ) {
        if (item.memberId() != query.memberId()) {
            return false;
        }
        if (query.couponId() != null && !query.couponId().equals(item.couponId())) {
            return false;
        }
        if (query.httpStatus() != null && !query.httpStatus().equals(item.httpStatus())) {
            return false;
        }
        return query.reasonCode() == null || query.reasonCode() == item.reasonCode();
    }

    /** 정렬 Comparator에서 Cursor 뒤에 오는 항목만 더 오래된 결과입니다. */
    private static boolean isOlderThanBefore(
            InquiryPosition position,
            InquiryPosition before
    ) {
        return before == null || NEWEST_POSITION_FIRST.compare(position, before) > 0;
    }

    /** requestId 재사용이 다른 회원·캠페인의 문의 행을 합치지 못하게 하는 연결 범위입니다. */
    private record RequestScope(long memberId, long couponId, String requestId) {

        private static RequestScope of(RawAttempt attempt) {
            return new RequestScope(
                    attempt.memberId(), attempt.couponId(), attempt.requestId());
        }
    }
}
