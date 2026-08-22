package com.kafkick.core.admin.couponmetrics;

import java.time.Instant;
import java.util.Objects;

import org.springframework.stereotype.Service;

import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.admin.couponmetrics.mock.AdminCouponMetricsMockDataFactory;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

/**
 * 관리자 캠페인 상세 지표의 기준 시각·원천 조회·순수 계산 흐름을 조립합니다.
 *
 * <p>현재는 Mock Factory를 사용하지만 결과 계산은 기술 중립 Source와 Calculator에 위임하므로,
 * 실제 Repository와 관측 Provider가 준비되면 원천 조회 구현만 교체할 수 있습니다.</p>
 */
@Service
public class AdminCouponMetricsService {

    private final TimeProvider timeProvider;
    private final AdminCouponMetricsMockDataFactory mockDataFactory;
    private final CouponMetricsCalculator calculator;

    /**
     * 한 요청의 시간과 상세 원천 및 계산을 담당할 협력 객체를 주입받습니다.
     *
     * @param timeProvider 요청 전체에서 한 번 사용할 기준 시각 공급자
     * @param mockDataFactory Overview와 같은 모집단의 상세 원천 Factory
     * @param calculator 원천값을 요청 구간의 상세 지표로 변환하는 순수 계산기
     */
    public AdminCouponMetricsService(
            TimeProvider timeProvider,
            AdminCouponMetricsMockDataFactory mockDataFactory,
            CouponMetricsCalculator calculator
    ) {
        this.timeProvider = Objects.requireNonNull(timeProvider, "timeProvider");
        this.mockDataFactory = Objects.requireNonNull(mockDataFactory, "mockDataFactory");
        this.calculator = Objects.requireNonNull(calculator, "calculator");
    }

    /**
     * 현재 시점의 한 캠페인 상세 지표를 요청 구간에 맞춰 반환합니다.
     *
     * @param couponId 조회할 쿠폰 ID
     * @param window 발급률과 상태 전이율 계산 구간
     * @return 계산 완료된 캠페인 상세 지표
     * @throws BusinessException Overview 모집단에 couponId가 없을 경우 COMMON-002
     */
    public CouponMetricsSnapshot getCouponMetrics(long couponId, MetricsWindow window) {
        Objects.requireNonNull(window, "window");
        // 한 응답의 모든 원천과 계산 경계가 같도록 현재 시각을 최초 한 번만 읽습니다.
        Instant snapshotAt = timeProvider.instant();
        CouponMetricsSource source = mockDataFactory.find(snapshotAt, couponId)
                .orElseThrow(() -> new BusinessException(
                        CommonErrorCode.NOT_FOUND, "상세 지표 캠페인을 찾을 수 없습니다: " + couponId));
        return calculator.calculate(source, window, snapshotAt);
    }
}
