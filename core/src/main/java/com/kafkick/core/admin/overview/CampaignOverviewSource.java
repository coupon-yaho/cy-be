package com.kafkick.core.admin.overview;

import java.time.Instant;
import java.util.Objects;

import com.kafkick.core.admin.campaignsource.PreparationObservation;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.SourceStatus;

/**
 * 관리자 운영현황 계산에 필요한 캠페인 단위 원천값입니다.
 *
 * <p>현재는 Mock Factory가 생성하지만, 캠페인 저장소가 연결된 이후에는 조회 결과를 이 계약으로
 * 변환해 동일한 계산기에 전달합니다. 재고를 아직 수집할 수 없는 캠페인은 수량과 관측 시각을
 * {@code null}로 보존하며, 관측하지 않은 값을 0으로 대신하지 않습니다.</p>
 *
 * @param couponId 캠페인을 식별하는 쿠폰 ID
 * @param campaignName 관리자 화면에 표시할 캠페인명
 * @param brandName 관리자 화면에 표시할 브랜드명
 * @param status 조회 시점의 캠페인 운영 상태
 * @param opensAt 캠페인 오픈 예정 시각
 * @param closesAt 캠페인 종료 예정 시각; 종료 시각이 없으면 {@code null}
 * @param engineVersion 재고 원천 선택 기준인 발급 엔진 버전
 * @param totalQuantity 발급 가능한 전체 수량; 아직 수집하지 못했으면 {@code null}
 * @param activeCount 현재 활성 발급 수량; 아직 수집하지 못했으면 {@code null}
 * @param stockObservedAt 재고 수량을 관측한 시각; 재고를 수집하지 못했으면 {@code null}
 * @param stockStatus 기술 원천과 분리한 재고 관측 상태
 * @param preparation 필수 준비 항목의 완료 여부와 관측 상태
 */
public record CampaignOverviewSource(
        Long couponId,
        String campaignName,
        String brandName,
        CouponRoundStatus status,
        Instant opensAt,
        Instant closesAt,
        EngineVersion engineVersion,
        Long totalQuantity,
        Long activeCount,
        Instant stockObservedAt,
        SourceStatus stockStatus,
        PreparationObservation preparation
) {

    /** 캠페인 기본 정보와 재고 상태·수량·관측 시각의 canonical 조합을 강제합니다. */
    public CampaignOverviewSource {
        Objects.requireNonNull(couponId, "couponId");
        Objects.requireNonNull(campaignName, "campaignName");
        Objects.requireNonNull(brandName, "brandName");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(opensAt, "opensAt");
        Objects.requireNonNull(engineVersion, "engineVersion");
        Objects.requireNonNull(stockStatus, "stockStatus");
        Objects.requireNonNull(preparation, "preparation");
        if (stockStatus.carriesValue()) {
            if (totalQuantity == null || activeCount == null || stockObservedAt == null) {
                throw new IllegalArgumentException("값 있는 재고 상태는 수량과 observedAt이 모두 필요합니다.");
            }
            if (totalQuantity <= 0L || activeCount < 0L || activeCount > totalQuantity) {
                throw new IllegalArgumentException("재고 수량 관계가 유효하지 않습니다.");
            }
        } else if (totalQuantity != null || activeCount != null || stockObservedAt != null) {
            throw new IllegalArgumentException("값 없는 재고 상태는 수량과 observedAt이 모두 null이어야 합니다.");
        }
    }
}
