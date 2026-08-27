package com.kafkick.core.admin.queue.mock;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.kafkick.core.admin.queue.AdminQueueObservationSource;
import com.kafkick.core.admin.queue.CampaignQueueObservation;
import com.kafkick.core.observation.SourceStatus;

/** 프론트 연동용으로 couponId와 요청 시각에만 의존하는 결정적 대기열 원천입니다. */
public final class MockAdminQueueObservationSource implements AdminQueueObservationSource {

    /** 외부 시간이나 상태를 보관하지 않는 결정적 Mock 원천을 생성합니다. */
    public MockAdminQueueObservationSource() { }

    /**
     * 요청 ID의 나머지 패턴으로 정상·증가·감소·입장 중단 대기열을 재현합니다.
     *
     * @return 요청 구간과 snapshotAt을 그대로 반영한 불변 Mock 관측값
     */
    @Override
    public Map<Long, CampaignQueueObservation> observe(
            List<Long> couponIds,
            Instant windowStart,
            Instant windowEnd,
            Instant snapshotAt
    ) {
        List<Long> requestedIds = AdminQueueObservationSource.requireRequest(
                couponIds, windowStart, windowEnd, snapshotAt);
        Map<Long, CampaignQueueObservation> observations = new LinkedHashMap<>();
        for (Long couponId : requestedIds) {
            observations.put(couponId, observation(couponId, windowStart, windowEnd, snapshotAt));
        }
        return Map.copyOf(observations);
    }

    /** couponId별 고정 패턴을 실제 계산기에 필요한 원천값으로 변환합니다. */
    private static CampaignQueueObservation observation(
            long couponId,
            Instant windowStart,
            Instant windowEnd,
            Instant snapshotAt
    ) {
        long pattern = Math.floorMod(couponId, 4L);
        if (pattern == 0L) {
            return admitted(couponId, 12L, 12L, 6L, windowStart, windowEnd, snapshotAt);
        }
        if (pattern == 1L) {
            // 입장 수보다 유입이 큰 패턴은 기존 계산기의 증가 추세와 ETA를 보여 줍니다.
            return admitted(couponId, 30L, 18L, 4L, windowStart, windowEnd, snapshotAt);
        }
        if (pattern == 2L) {
            return admitted(couponId, 8L, 20L, 14L, windowStart, windowEnd, snapshotAt);
        }
        // 대기자가 있는 무입장은 중단 시작 시각을 함께 제공해야 중단 조치 계산이 가능합니다.
        return new CampaignQueueObservation(
                couponId, 24L, 24L, 0L, windowStart, windowEnd,
                windowStart.minusSeconds(121L), windowStart.minusSeconds(120L),
                SourceStatus.VALID, snapshotAt);
    }

    /** 입장이 있는 패턴의 마지막 입장 시각을 요청 구간 안에 고정합니다. */
    private static CampaignQueueObservation admitted(
            long couponId,
            long currentWaitingCount,
            long previousWaitingCount,
            long admittedCount,
            Instant windowStart,
            Instant windowEnd,
            Instant snapshotAt
    ) {
        return new CampaignQueueObservation(
                couponId, currentWaitingCount, previousWaitingCount, admittedCount,
                windowStart, windowEnd, windowEnd.minusNanos(1L), null,
                SourceStatus.VALID, snapshotAt);
    }
}
