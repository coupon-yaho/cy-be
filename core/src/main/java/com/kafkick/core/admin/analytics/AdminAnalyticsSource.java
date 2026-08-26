package com.kafkick.core.admin.analytics;

/** 관리자 브랜드 분석의 실제 집계 조회 경계입니다. */
@FunctionalInterface
public interface AdminAnalyticsSource {

    /** 요청 기간과 선택 필터가 적용된 집계 원천과 검증 메타데이터를 반환합니다. */
    AdminAnalyticsDataset load(AdminAnalyticsQuery query);
}
