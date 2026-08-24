package com.kafkick.api.admin.dashboard;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;

import java.time.ZoneId;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.admin.dashboard.dto.AnalyticsQuery;
import com.kafkick.api.admin.dashboard.dto.AdminAnalyticsResponse;
import com.kafkick.api.admin.dashboard.dto.AdminOverviewResponse;
import com.kafkick.api.admin.dashboard.dto.CouponMetricsResponse;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.api.caller.Caller;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.admin.analytics.AdminAnalyticsQuery;
import com.kafkick.core.admin.analytics.AdminAnalyticsResult;
import com.kafkick.core.admin.analytics.AdminAnalyticsService;
import com.kafkick.core.admin.couponmetrics.AdminCouponMetricsService;
import com.kafkick.core.admin.overview.AdminOverviewResult;
import com.kafkick.core.admin.overview.AdminOverviewService;
import com.kafkick.core.support.exception.BusinessException;

/**
 * 운영 담당자가 사용하는 관리자 현황·쿠폰 지표·분석 API의 HTTP 경계입니다.
 *
 * <p>운영현황 계산·조립은 구체 {@link AdminOverviewService}에 위임하고, 반환된
 * {@link AdminOverviewResult}는 Controller에서 HTTP DTO로 변환합니다. 쿠폰 지표와 브랜드 분석도 각각의
 * Core Service가 계산하며 Controller는 HTTP 입력·출력 변환만 담당합니다.</p>
 */
@RestController
@RequestMapping("/api/v1/admin")
public class AdminDashboardController {

    private static final ZoneId ANALYTICS_ZONE = ZoneId.of("Asia/Seoul");

    private final AdminOverviewService adminOverviewService;
    private final AdminCouponMetricsService adminCouponMetricsService;
    private final AdminAnalyticsService adminAnalyticsService;

    /**
     * 운영현황·캠페인 상세·브랜드 분석 요청을 각 구체 Service에 위임하도록 구성합니다.
     *
     * @param adminOverviewService 관리자 첫 화면 응답을 조립하는 구체 Service
     * @param adminCouponMetricsService 캠페인 상세 지표를 계산하는 구체 Service
     * @param adminAnalyticsService 브랜드 분석 조회와 계산을 조립하는 구체 Service
     */
    public AdminDashboardController(
            AdminOverviewService adminOverviewService,
            AdminCouponMetricsService adminCouponMetricsService,
            AdminAnalyticsService adminAnalyticsService
    ) {
        this.adminOverviewService = adminOverviewService;
        this.adminCouponMetricsService = adminCouponMetricsService;
        this.adminAnalyticsService = adminAnalyticsService;
    }

    /**
     * 관리자 운영 현황 화면의 상단 위험 요약과 캠페인별 운영 상태를 조회합니다.
     *
     * <p>응답 계약에는 조치 필요·오픈 임박·대기 기준 초과·소진 위험 KPI와 조치 목록,
     * 전체 발급률·대기열·응답 지연·캠페인 상태 집계가 포함됩니다. 또한 캠페인별 발급 흐름,
     * 대기 상태, 전체 고객 결과 집계, 재고·소진 예상을 제공합니다.</p>
     *
     * <p>회원 발급 문의, 발급 상태 변경 이력, 고객 알림 발송 현황은 각각
     * 별도의 관리자 API에서 조회하므로 이 응답에 중복해서 포함하지 않습니다.</p>
     *
     * <p>O1 발급 흐름·O3 고객 결과·응답 지연은 실제 관측 원천에 연결됐습니다. 캠페인·O2·O4·FINAL은
     * 현재 Mock 경계를 유지하고, 전체 발급률은 세션 최고값 경계가 없어 {@code PENDING}으로
     * 제공합니다. 후속 원천 연결 뒤에도 같은 응답 계약을 유지합니다.</p>
     *
     * @param caller 기존 호출자 체인에서 검증한 관리자 회원
     * @return Service 계산 결과를 HTTP DTO로 변환한 성공 응답 봉투
     */
    @GetMapping("/overview")
    public ResponseEnvelope<AdminOverviewResponse> overview(Caller caller) {
        AdminOverviewResult result = adminOverviewService.getOverview();

        return ResponseEnvelope.success(
                AdminOverviewResponse.from(
                        result.snapshot(), result.overallStatus())
        );
    }

    /**
     * 특정 쿠폰 회차의 재고·발급 진행률·대기열·보유 상태 지표를 지정된 관측 구간으로 조회합니다.
     *
     * <p>{@code couponId}는 캠페인 회차 식별자이며 양수만 허용하는 필수 쿼리 파라미터입니다. {@code window}는
     * {@code 1m}, {@code 5m}, {@code 15m} 중 하나입니다. 현재는 Overview와 같은 Mock 모집단의
     * 원천 수량을 Core Service가 계산하며, 후속 실제 원천 연결 뒤에도 같은 응답 계약을 유지합니다.</p>
     *
     * @param couponId 조회할 쿠폰 캠페인 회차 식별자
     * @param window 지표 집계 구간
     * @param caller 기존 호출자 체인에서 검증한 관리자 회원
     * @return 계산한 쿠폰 운영 지표 성공 응답 봉투
     * @throws BusinessException Overview 모집단에 쿠폰 ID가 없는 경우
     */
    @GetMapping("/coupon-metrics")
    public ResponseEnvelope<CouponMetricsResponse> couponMetrics(
            @RequestParam @Positive(message = "couponId는 양수여야 합니다.") Long couponId,
            @RequestParam MetricsWindow window,
            Caller caller) {
        return ResponseEnvelope.success(CouponMetricsResponse.from(
                adminCouponMetricsService.getCouponMetrics(couponId, window)));
    }

    /**
     * 기간별 브랜드 추이, 시간대 히트맵, 발급 현재 상태 분포를 조회합니다.
     *
     * <p>{@code from}과 {@code to}는 필수이며 역전될 수 없고 최대 조회 범위는 1년입니다.
     * {@code brandId}와 {@code couponId}는 선택 필터로 함께 사용할 수 있습니다. 날짜·시간 해석은
     * 관리자 화면 계약인 Asia/Seoul을 사용하며, Core Service가 존재와 소속 관계를 확인합니다.</p>
     *
     * @param query 조회 기간과 선택적인 브랜드·쿠폰 필터
     * @param caller 기존 호출자 체인에서 검증한 관리자 회원
     * @return 분석별 상태와 계산값을 포함한 관리자 분석 성공 응답 봉투
     * @throws BusinessException 요청한 브랜드·캠페인이 없거나 소속 관계가 다른 경우
     */
    @GetMapping("/analytics")
    public ResponseEnvelope<AdminAnalyticsResponse> analytics(
            @Valid @ModelAttribute AnalyticsQuery query, Caller caller) {
        AdminAnalyticsQuery coreQuery = new AdminAnalyticsQuery(
                query.from(), query.to(), query.brandId(), query.couponId(), ANALYTICS_ZONE);
        AdminAnalyticsResult result = adminAnalyticsService.getAnalytics(coreQuery);
        return ResponseEnvelope.success(AdminAnalyticsResponse.from(result));
    }
}
