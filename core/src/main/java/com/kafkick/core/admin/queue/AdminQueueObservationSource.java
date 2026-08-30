package com.kafkick.core.admin.queue;

import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** 관리자 화면이 기술 구현과 무관하게 쿠폰 회차별 대기열 관측값을 읽는 경계입니다. */
public interface AdminQueueObservationSource {

    /**
     * 요청한 쿠폰 회차와 같은 모집단의 대기열 관측값을 반환합니다.
     *
     * <p>응답은 요청하지 않은 ID를 포함하거나 한 couponId를 중복해 반환할 수 없으며, 요청 ID마다
     * 정확히 하나의 관측값을 포함해야 합니다. 호출자는 이 계약이 깨진 응답을 전체값으로 사용하지
     * 않습니다.</p>
     *
     * @param couponIds 중복 없는 양수 회차 ID 모집단
     * @param windowStart 입장 완료 수 집계 시작 시각
     * @param windowEnd 입장 완료 수 집계 종료 시각
     * @param snapshotAt 요청 전체가 공유하는 기준 시각
     * @return 요청 couponId를 키로 하는 불변 대기열 관측값
     */
    Map<Long, CouponRoundQueueObservation> observe(
            List<Long> couponIds,
            Instant windowStart,
            Instant windowEnd,
            Instant snapshotAt
    );

    /** 원천 구현이 공통 요청 모집단과 시간 구간을 검증하도록 합니다. */
    static List<Long> requireRequest(
            List<Long> couponIds,
            Instant windowStart,
            Instant windowEnd,
            Instant snapshotAt
    ) {
        Objects.requireNonNull(couponIds, "couponIds");
        Objects.requireNonNull(windowStart, "windowStart");
        Objects.requireNonNull(windowEnd, "windowEnd");
        Objects.requireNonNull(snapshotAt, "snapshotAt");
        if (!windowEnd.isAfter(windowStart)) {
            throw new IllegalArgumentException("대기열 관측 구간은 양수여야 합니다.");
        }
        if (windowEnd.isAfter(snapshotAt)) {
            throw new IllegalArgumentException("대기열 관측 구간은 snapshotAt 이후일 수 없습니다.");
        }
        List<Long> requestedIds = List.copyOf(couponIds);
        Set<Long> distinctIds = new LinkedHashSet<>();
        for (Long couponId : requestedIds) {
            if (couponId == null || couponId <= 0L || !distinctIds.add(couponId)) {
                throw new IllegalArgumentException("couponIds는 중복 없는 양수 회차 ID여야 합니다.");
            }
        }
        return requestedIds;
    }
}
