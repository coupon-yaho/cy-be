package com.kafkick.api.admin.observability;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.kafkick.core.consistency.ConsistencyFinalObservation;
import com.kafkick.core.consistency.ConsistencyFinalReader;
import com.kafkick.core.observation.SourceStatus;

/** 관측 DB Reader가 배선되지 않았을 때 FINAL을 계산 대기 상태로 보존합니다. */
public final class PendingConsistencyFinalReader implements ConsistencyFinalReader {

    /** 요청한 양수 ID를 첫 입력 순서로 축약해 모두 PENDING으로 반환합니다. */
    @Override
    public Map<Long, ConsistencyFinalObservation> findLatestByCouponIds(List<Long> couponIds) {
        if (couponIds == null) {
            throw new IllegalArgumentException("FINAL 조회 캠페인 ID 목록이 필요합니다.");
        }
        LinkedHashMap<Long, ConsistencyFinalObservation> observations = new LinkedHashMap<>();
        for (Long couponId : couponIds) {
            if (couponId == null || couponId <= 0L) {
                throw new IllegalArgumentException("FINAL 조회 캠페인 ID는 양수여야 합니다: " + couponId);
            }
            observations.putIfAbsent(
                    couponId, new ConsistencyFinalObservation(SourceStatus.PENDING, null));
        }
        return Collections.unmodifiableMap(observations);
    }
}
