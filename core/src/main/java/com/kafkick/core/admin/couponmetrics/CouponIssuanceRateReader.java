package com.kafkick.core.admin.couponmetrics;

import java.time.Instant;
import java.util.List;

import com.kafkick.core.admin.MetricsWindow;

/** Prometheus 같은 외부 관측계에서 쿠폰 회차별 초당 발급률 표본을 읽습니다. */
public interface CouponIssuanceRateReader {

    /**
     * 한 요청의 기준 시각과 구간에 해당하는 발급률 관측 결과를 읽습니다.
     *
     * @param couponId 조회할 양수 쿠폰 ID
     * @param window 조회할 상세 지표 구간
     * @param snapshotAt Service가 요청 전체에 공유한 기준 시각
     * @return 상태와 표본 및 실제 관측 시각을 함께 가진 발급률 원천
     */
    CouponMetricsSource.Observation<List<CouponMetricsSource.IssuanceRateSample>> read(
            long couponId,
            MetricsWindow window,
            Instant snapshotAt
    );
}
