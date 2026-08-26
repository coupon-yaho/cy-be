package com.kafkick.core.consistency;

import java.util.List;
import java.util.Map;

/**
 * 캠페인별 최신 FINAL 정합성 결과를 읽는 기술 중립 포트입니다.
 *
 * <p>Benchmark 회차가 없으면 N_A, 회차는 있지만 완료 FINAL이 없으면 PENDING,
 * 완료 FINAL이 있으면 VALID, 조회나 결과 복구가 불가능하면 UNAVAILABLE을 반환합니다.</p>
 */
public interface ConsistencyFinalReader {

    /** 요청한 캠페인 전체의 최신 FINAL 상태를 한 번에 반환합니다. */
    Map<Long, ConsistencyFinalObservation> findLatestByCouponIds(List<Long> couponIds);

    /** 단건 조회도 일괄 조회와 같은 최신 선택·상태 판정 계약을 사용합니다. */
    default ConsistencyFinalObservation findLatestByCouponId(long couponId) {
        return findLatestByCouponIds(List.of(couponId)).get(couponId);
    }
}
