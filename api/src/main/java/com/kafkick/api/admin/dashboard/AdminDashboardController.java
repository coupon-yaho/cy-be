package com.kafkick.api.admin.dashboard;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.kafkick.api.admin.dashboard.dto.AnalyticsQuery;
import com.kafkick.api.admin.dashboard.dto.AdminAnalyticsResponse;
import com.kafkick.api.admin.dashboard.dto.AdminOverviewResponse;
import com.kafkick.api.admin.dashboard.dto.CouponMetricsResponse;
import com.kafkick.api.admin.support.AdminApiErrorCode;
import com.kafkick.api.support.ResponseEnvelope;
import com.kafkick.api.caller.Caller;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.admin.MetricsWindow;

/**
 * 운영 담당자가 사용하는 관리자 현황·쿠폰 지표·분석 API의 HTTP 계약을 선구축합니다.
 *
 * <p>이 단계에서는 요청 형식과 응답 DTO만 고정하고 Service, Provider, Repository를 호출하지 않습니다.
 * 실제 DB 집계와 B 소유 관제 데이터가 연결되기 전까지 모든 유효 요청은 {@code 501 / ADMIN-001}을 반환합니다.</p>
 */
@RestController
@RequestMapping("/api/v1/admin")
public class AdminDashboardController {

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
     * <p>A-03에서는 {@code AdminOverviewProvider}, 조회 조건, 내부 Snapshot과 HTTP 응답 계약만
     * 선구축합니다. 실제 DB·Redis·관제 데이터 조립과 Provider 주입은 A-06에서 연결하며,
     * 연결 전까지 이 API는 {@code 501 / ADMIN-001}을 반환합니다.</p>
     *
     * @param caller 기존 호출자 체인에서 검증한 관리자 회원
     * @return 후속 A-06 구현에서 사용할 운영 현황 응답 봉투
     * @throws BusinessException 운영 현황 실제 조회 구현이 아직 연결되지 않은 경우
     */
    @GetMapping("/overview")
    public ResponseEnvelope<AdminOverviewResponse> overview(Caller caller) {
        throw new BusinessException(AdminApiErrorCode.NOT_IMPLEMENTED);
    }

    /**
     * 특정 쿠폰 회차의 재고·발급 진행률·대기열·보유 상태 지표를 지정된 관측 구간으로 조회합니다.
     *
     * <p>{@code couponId}는 캠페인 회차 식별자이며 양수만 허용합니다. {@code window}는
     * {@code 1m}, {@code 5m}, {@code 15m} 중 하나입니다. 실제 DB 집계와 관제 Provider가 연결되기 전까지
     * 유효 요청에도 {@link AdminApiErrorCode#NOT_IMPLEMENTED}를 발생시킵니다.</p>
     *
     * @param couponId 조회할 쿠폰 캠페인 회차 식별자
     * @param window 지표 집계 구간
     * @param caller 기존 호출자 체인에서 검증한 관리자 회원
     * @return 후속 구현에서 사용할 쿠폰 운영 지표 응답 봉투
     * @throws BusinessException 쿠폰 지표 조회 구현이 아직 연결되지 않은 경우
     */
    @GetMapping("/coupons/{couponId}/metrics")
    public ResponseEnvelope<CouponMetricsResponse> couponMetrics(
            @PathVariable @Positive(message = "couponId는 양수여야 합니다.") Long couponId,
            @RequestParam MetricsWindow window,
            Caller caller) {
        throw new BusinessException(AdminApiErrorCode.NOT_IMPLEMENTED);
    }

    /**
     * 기간별 브랜드 추이, 시간대 히트맵, 발급 퍼널 분석 데이터를 조회합니다.
     *
     * <p>{@code from}과 {@code to}는 필수이며 역전될 수 없고 최대 조회 범위는 1년입니다.
     * {@code brandId}와 {@code couponId}는 선택 필터로 함께 사용할 수 있습니다. 쿠폰의 브랜드 소속 확인과
     * 실제 집계 조회는 후속 분석 Use Case에서 연결하며 현재는 {@code 501 / ADMIN-001}을 반환합니다.</p>
     *
     * @param query 조회 기간과 선택적인 브랜드·쿠폰 필터
     * @param caller 기존 호출자 체인에서 검증한 관리자 회원
     * @return 후속 구현에서 사용할 관리자 분석 응답 봉투
     * @throws BusinessException 분석 조회 구현이 아직 연결되지 않은 경우
     */
    @GetMapping("/analytics")
    public ResponseEnvelope<AdminAnalyticsResponse> analytics(
            @Valid @ModelAttribute AnalyticsQuery query, Caller caller) {
        throw new BusinessException(AdminApiErrorCode.NOT_IMPLEMENTED);
    }
}
