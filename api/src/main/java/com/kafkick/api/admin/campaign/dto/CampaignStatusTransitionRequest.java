package com.kafkick.api.admin.campaign.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import com.kafkick.core.coupon.domain.CouponRoundStatus;

/**
 * 일반 수정과 분리된 캠페인 운영 상태 전환 요청입니다.
 *
 * @param targetStatus 전환하려는 목표 상태
 * @param reason 감사 로그에 남길 운영 사유
 */
public record CampaignStatusTransitionRequest(
        @NotNull CouponRoundStatus targetStatus,
        @NotBlank @Size(max = 200) String reason) {
}
