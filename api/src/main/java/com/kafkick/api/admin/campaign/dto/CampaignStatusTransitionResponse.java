package com.kafkick.api.admin.campaign.dto;

import java.time.Instant;

import com.kafkick.core.coupon.CouponStatus;

/**
 * 감사 대상인 캠페인 상태 전환의 결과입니다.
 *
 * @param campaignId 전환된 캠페인 식별자
 * @param status 전환 후 상태
 * @param updatedBy 명령을 수행한 관리자 회원 식별자
 * @param updatedAt 상태 전환이 확정된 시각
 */
public record CampaignStatusTransitionResponse(
        Long campaignId, CouponStatus status, Long updatedBy, Instant updatedAt) {
}
