package com.kafkick.api.admin.campaign.dto;

import java.time.DayOfWeek;
import java.time.LocalTime;
import java.util.List;

import com.kafkick.core.admin.CouponPolicyType;

/** 쿠폰 정책과 반복 규칙을 함께 제공하는 템플릿 목록 응답입니다. */
public record TemplateListResponse(List<TemplateSummary> items, String nextBeforeCursor, boolean hasOlder) {
    /** 템플릿 조회에 필요한 정책·스케줄 필드입니다. */
    public record TemplateSummary(Long id, Long brandId, String name, CouponPolicyType policyType, Integer nthWeek,
                                  DayOfWeek dayOfWeek, LocalTime startTime, Integer durationHours,
                                  int stockPerOccurrence, int eligibleGradesMask, boolean active) { }
}
