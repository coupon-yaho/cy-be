package com.kafkick.core.admin.issuancehistory.mock;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Objects;
import java.util.Comparator;

import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryQuery;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryReadResult;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryReader;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryResult.HistorySummary;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistorySource;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistorySource.RawHistory;
import com.kafkick.core.coupon.domain.IssuanceEventType;
import com.kafkick.core.coupon.domain.IssuanceStatus;

/**
 * 실제 Repository 조회를 대신할 fixture-v1 관리자 발급 상태 변경 이력 원천을 만듭니다.
 *
 * <p>모든 프로세스와 Factory 인스턴스가 같은 고정 기준을 사용하며, 요약, 마스킹, 페이지와 같은
 * 파생 결과 없이 저장소가 반환할 원시 행만 제공합니다.</p>
 *
 * <p><b>[OBS-36] 이 클래스는 더 이상 스프링 컴포넌트가 아니다.</b> 등록 여부는 API 가 소유한다 —
 * {@code AdminFixtureConfig} 가 {@code admin.mock.enabled=true} 일 때만 빈으로 만든다(기본 꺼짐).
 *
 * <p>예전에는 조건 없는 {@code @Component} 였다. 그래서 <b>운영에서도 이 fixture 가 200 으로
 * 나갔다</b> — 화면은 정상으로 보이고 수치만 가짜였다. 왜 조건을 여기가 아니라 API 가 갖는지,
 * 끈 상태가 왜 PENDING 이 아니라 기동 실패인지는 {@code AdminFixtureConfig} 에 적었다.
 */
public class AdminIssuanceHistoryMockDataFactory implements AdminIssuanceHistoryReader {

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    private static final Instant FIXTURE_V1_ANCHOR = Instant.parse("2026-08-23T00:00:00Z");
    private static final Instant NEWEST_FIXTURE_OCCURRED_AT = FIXTURE_V1_ANCHOR.minus(Duration.ofHours(1));

    /**
     * 요청 시각보다 미래에 있지 않은 고정 위치의 원시 발급 이력 Dataset을 만듭니다.
     *
     * @param snapshotAt 한 요청에서 공유할 기준 시각
     * @return 상태 변경 종류와 필터 경계를 포함한 불변 원시 이력 원천
     * @throws IllegalArgumentException 요청 시각이 Dataset 생성 기준보다 빠른 경우
     */
    public AdminIssuanceHistorySource create(Instant snapshotAt) {
        Objects.requireNonNull(snapshotAt, "snapshotAt");
        if (snapshotAt.isBefore(NEWEST_FIXTURE_OCCURRED_AT)) {
            throw new IllegalArgumentException("snapshotAt은 fixture-v1 최신 이력보다 빠를 수 없습니다.");
        }
        LocalDate anchorKstDate = FIXTURE_V1_ANCHOR.atZone(KST).toLocalDate();
        Instant previousKstDayStart = kstStartOfDay(anchorKstDate.minusDays(1));
        Instant currentKstDayStart = kstStartOfDay(anchorKstDate);
        // 프로세스 재시작과 replica 이동에도 행 위치가 같도록 versioned fixture 기준에 고정합니다.
        return new AdminIssuanceHistorySource(List.of(
                raw(1_001L, 5_001L, "A101000000000001", 101L, null,
                        IssuanceStatus.ISSUED, IssuanceEventType.ISSUE,
                        previousKstDayStart),
                raw(1_002L, 5_001L, "A101000000000001", 101L, IssuanceStatus.ISSUED,
                        IssuanceStatus.USED, IssuanceEventType.USE,
                        FIXTURE_V1_ANCHOR.minus(Duration.ofHours(30))),
                raw(1_003L, 5_002L, "A101000000000002", 101L, IssuanceStatus.USED,
                        IssuanceStatus.ISSUED, IssuanceEventType.CANCEL_USE,
                        FIXTURE_V1_ANCHOR.minus(Duration.ofHours(28))),
                raw(1_004L, 5_003L, "A101000000000003", 101L, IssuanceStatus.ISSUED,
                        IssuanceStatus.CANCELLED, IssuanceEventType.CANCEL,
                        FIXTURE_V1_ANCHOR.minus(Duration.ofHours(24))),
                raw(1_005L, 6_001L, "B102000000000001", 102L, IssuanceStatus.ISSUED,
                        IssuanceStatus.EXPIRED, IssuanceEventType.EXPIRE,
                        currentKstDayStart),
                raw(1_006L, 6_002L, "B102000000000002", 102L, null,
                        IssuanceStatus.ISSUED, IssuanceEventType.ISSUE,
                        FIXTURE_V1_ANCHOR.minus(Duration.ofHours(5))),
                raw(1_007L, 6_003L, "B102000000000003", 102L, null,
                        IssuanceStatus.ISSUED, IssuanceEventType.ISSUE,
                        FIXTURE_V1_ANCHOR.minus(Duration.ofHours(1))),
                raw(1_008L, 5_004L, "A101000000000004", 101L, IssuanceStatus.ISSUED,
                        IssuanceStatus.CANCELLED, IssuanceEventType.CANCEL,
                        FIXTURE_V1_ANCHOR.minus(Duration.ofHours(1)))));
    }

    /** 테스트에서만 기존 fixture 행을 Reader 계약으로 제한해 반환합니다. */
    @Override
    public AdminIssuanceHistoryReadResult read(AdminIssuanceHistoryQuery query, Instant snapshotAt) {
        List<RawHistory> population = create(snapshotAt).histories().stream()
                .filter(row -> (query.couponId() == null || row.couponId() == query.couponId())
                        && (query.fromInclusive() == null || !row.occurredAt().isBefore(query.fromInclusive()))
                        && (query.toExclusive() == null || row.occurredAt().isBefore(query.toExclusive()))
                        && (query.eventType() == null || row.eventType() == query.eventType()))
                .sorted(Comparator.comparing(RawHistory::occurredAt).reversed()
                        .thenComparing(Comparator.comparingLong(RawHistory::historyId).reversed()))
                .toList();
        long issue = count(population, IssuanceEventType.ISSUE);
        long use = count(population, IssuanceEventType.USE);
        long cancelUse = count(population, IssuanceEventType.CANCEL_USE);
        long cancel = count(population, IssuanceEventType.CANCEL);
        long expire = count(population, IssuanceEventType.EXPIRE);
        List<RawHistory> candidates = population.stream().filter(row -> query.before() == null
                || row.occurredAt().isBefore(query.before().occurredAt())
                || (row.occurredAt().equals(query.before().occurredAt())
                && row.historyId() < query.before().historyId())).limit(query.limit() + 1L).toList();
        return new AdminIssuanceHistoryReadResult(candidates,
                new HistorySummary(issue + use + cancelUse + cancel + expire,
                        issue, use, cancelUse, cancel, expire));
    }

    /** 이벤트 유형의 fixture 행 수를 반환합니다. */
    private static long count(List<RawHistory> rows, IssuanceEventType eventType) {
        return rows.stream().filter(row -> row.eventType() == eventType).count();
    }

    /**
     * 주어진 KST 날짜의 시작을 타임존 독립적인 절대 시각으로 변환합니다.
     *
     * @param date KST 기준 날짜
     * @return 해당 KST 날짜 00:00의 Instant
     */
    private static Instant kstStartOfDay(LocalDate date) {
        return date.atStartOfDay(KST).toInstant();
    }

    /**
     * 상태 전이 규칙을 이미 만족하는 한 건의 Repository 원시 행을 만듭니다.
     *
     * @param historyId 이력 행의 식별자
     * @param issuanceId 발급건 식별자
     * @param issuanceCode 16자리 비마스킹 발급 코드
     * @param couponId 쿠폰 식별자
     * @param fromStatus 전이 전 상태
     * @param toStatus 전이 후 상태
     * @param eventType 상태 변경 사건 종류
     * @param occurredAt 상태 변경 절대 시각
     * @return 기술 중립 원시 이력 행
     */
    private static RawHistory raw(
            long historyId,
            long issuanceId,
            String issuanceCode,
            long couponId,
            IssuanceStatus fromStatus,
            IssuanceStatus toStatus,
            IssuanceEventType eventType,
            Instant occurredAt
    ) {
        return new RawHistory(
                historyId, issuanceId, issuanceCode, couponId, fromStatus, toStatus, eventType,
                null, null, occurredAt);
    }
}
