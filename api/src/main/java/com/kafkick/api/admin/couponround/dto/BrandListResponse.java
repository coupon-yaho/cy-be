package com.kafkick.api.admin.couponround.dto;

import java.util.List;

import com.kafkick.core.admin.BrandCategory;

/** 브랜드 선택 목록 응답입니다. */
public record BrandListResponse(List<BrandSummary> items, String nextBeforeCursor, boolean hasOlder) {
    /**
     * 관리자 선택 목록에 노출할 브랜드의 최소 필드입니다.
     *
     * @param id 브랜드 식별자
     * @param name 관리자 화면 표시명
     * @param category 도메인에서 관리하는 카테고리 코드
     * @param active 쿠폰 회차에서 선택 가능한 활성 상태인지 여부
     */
    public record BrandSummary(Long id, String name, BrandCategory category, boolean active) { }
}
