package com.kafkick.api.admin.analytics;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kafkick.api.admin.support.config.AdminAnalyticsProperties;
import com.kafkick.core.admin.analytics.AdminAnalyticsCalculator;
import com.kafkick.core.admin.analytics.AdminAnalyticsFreshnessPolicy;
import com.kafkick.core.admin.analytics.AdminAnalyticsPendingSource;
import com.kafkick.core.admin.analytics.AdminAnalyticsService;
import com.kafkick.core.admin.analytics.AdminAnalyticsSource;
import com.kafkick.core.support.TimeProvider;

/** 관리자 브랜드 분석의 기술 중립 Core 구성요소를 API 실행 환경에 조립합니다. */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(AdminAnalyticsProperties.class)
public class AdminAnalyticsConfig {

    /** 실제 Source가 없으면 가짜 집계 없이 값 없는 Pending Source를 선택합니다. */
    @Bean
    @ConditionalOnMissingBean(AdminAnalyticsSource.class)
    public AdminAnalyticsSource adminAnalyticsSource() {
        // 집계 Source가 도착하기 전에는 가짜 통계를 만들지 않고 아직 집계되지 않았음을 명시합니다.
        return new AdminAnalyticsPendingSource();
    }

    /** 실제 Source에 적용할 Freshness 임계값을 등록합니다. */
    @Bean
    @ConditionalOnMissingBean(AdminAnalyticsFreshnessPolicy.class)
    public AdminAnalyticsFreshnessPolicy adminAnalyticsFreshnessPolicy(
            AdminAnalyticsProperties properties
    ) {
        return new AdminAnalyticsFreshnessPolicy(properties.staleAfter());
    }

    /** 월별 추이·히트맵·현재 상태 분포를 계산하는 순수 계산기를 등록합니다. */
    @Bean
    @ConditionalOnMissingBean(AdminAnalyticsCalculator.class)
    public AdminAnalyticsCalculator adminAnalyticsCalculator(
            AdminAnalyticsFreshnessPolicy freshnessPolicy
    ) {
        return new AdminAnalyticsCalculator(freshnessPolicy);
    }

    /** Source 조회와 필터 검증 및 계산 수명주기를 담당하는 Core Service를 등록합니다. */
    @Bean
    @ConditionalOnMissingBean(AdminAnalyticsService.class)
    public AdminAnalyticsService adminAnalyticsService(
            AdminAnalyticsSource source,
            TimeProvider timeProvider,
            AdminAnalyticsCalculator calculator
    ) {
        return new AdminAnalyticsService(source, timeProvider, calculator);
    }
}
