package com.kafkick.core.admin.queue;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.kafkick.core.observation.SourceStatus;

/** 실제 대기열 원천이 연결되기 전 값을 만들지 않고 PENDING만 반환하는 기본 구현입니다. */
public final class PendingAdminQueueObservationSource implements AdminQueueObservationSource {

    /** 값 없는 기본 대기열 원천을 생성합니다. */
    public PendingAdminQueueObservationSource() { }

    /** 요청한 각 캠페인에 값 없는 PENDING 관측값을 반환합니다. */
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
            // 0은 실제 빈 대기열이라는 뜻이므로 미연결 원천에는 만들지 않습니다.
            observations.put(couponId, new CampaignQueueObservation(
                    couponId, null, null, null, null, null,
                    null, null, SourceStatus.PENDING, null));
        }
        return Map.copyOf(observations);
    }
}
