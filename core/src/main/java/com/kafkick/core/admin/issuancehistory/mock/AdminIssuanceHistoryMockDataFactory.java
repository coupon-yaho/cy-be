package com.kafkick.core.admin.issuancehistory.mock;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Objects;

import org.springframework.stereotype.Component;

import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistorySource;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistorySource.RawHistory;
import com.kafkick.core.coupon.IssuanceEventType;
import com.kafkick.core.coupon.IssuanceStatus;

/**
 * 실제 Repository 조회를 대신할 결정론적 관리자 발급 상태 변경 이력 원천을 만듭니다.
 *
 * <p>이 Factory는 요약, 마스킹, 페이지와 같은 파생 결과 없이 저장소가 반환할 원시 행만 제공합니다.</p>
 */
@Component
public class AdminIssuanceHistoryMockDataFactory {

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    /**
     * 주어진 기준 시각보다 미래에 있지 않은 원시 발급 이력 Dataset을 만듭니다.
     *
     * @param snapshotAt 한 요청에서 공유할 기준 시각
     * @return 상태 변경 종류와 필터 경계를 포함한 불변 원시 이력 원천
     */
    public AdminIssuanceHistorySource create(Instant snapshotAt) {
        Objects.requireNonNull(snapshotAt, "snapshotAt");
        LocalDate snapshotKstDate = snapshotAt.atZone(KST).toLocalDate();
        Instant previousKstDayStart = kstStartOfDay(snapshotKstDate.minusDays(1));
        Instant currentKstDayStart = kstStartOfDay(snapshotKstDate);
        // Mock은 계산 결과가 아니라 저장소가 전달할 원시 행만 기준 시각에 맞춰 만듭니다.
        return new AdminIssuanceHistorySource(List.of(
                raw(1_001L, 5_001L, "A101000000000001", 101L, null,
                        IssuanceStatus.ISSUED, IssuanceEventType.ISSUE,
                        previousKstDayStart),
                raw(1_002L, 5_001L, "A101000000000001", 101L, IssuanceStatus.ISSUED,
                        IssuanceStatus.USED, IssuanceEventType.USE,
                        snapshotAt.minus(Duration.ofHours(30))),
                raw(1_003L, 5_002L, "A101000000000002", 101L, IssuanceStatus.USED,
                        IssuanceStatus.ISSUED, IssuanceEventType.CANCEL_USE,
                        snapshotAt.minus(Duration.ofHours(28))),
                raw(1_004L, 5_003L, "A101000000000003", 101L, IssuanceStatus.ISSUED,
                        IssuanceStatus.CANCELLED, IssuanceEventType.CANCEL,
                        snapshotAt.minus(Duration.ofHours(24))),
                raw(1_005L, 6_001L, "B102000000000001", 102L, IssuanceStatus.ISSUED,
                        IssuanceStatus.EXPIRED, IssuanceEventType.EXPIRE,
                        currentKstDayStart),
                raw(1_006L, 6_002L, "B102000000000002", 102L, null,
                        IssuanceStatus.ISSUED, IssuanceEventType.ISSUE,
                        snapshotAt.minus(Duration.ofHours(5))),
                raw(1_007L, 6_003L, "B102000000000003", 102L, null,
                        IssuanceStatus.ISSUED, IssuanceEventType.ISSUE,
                        snapshotAt.minus(Duration.ofHours(1))),
                raw(1_008L, 5_004L, "A101000000000004", 101L, IssuanceStatus.ISSUED,
                        IssuanceStatus.CANCELLED, IssuanceEventType.CANCEL,
                        snapshotAt.minus(Duration.ofHours(1)))));
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
