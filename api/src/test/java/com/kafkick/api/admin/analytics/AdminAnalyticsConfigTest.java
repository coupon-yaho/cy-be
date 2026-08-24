package com.kafkick.api.admin.analytics;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kafkick.api.admin.support.config.AdminAnalyticsProperties;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AggregateAvailability;
import com.kafkick.core.admin.analytics.AdminAnalyticsFreshnessPolicy;
import com.kafkick.core.admin.analytics.AdminAnalyticsQuery;
import com.kafkick.core.admin.analytics.AdminAnalyticsService;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AnalyticsSourceType;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;

/** 관리자 분석 Source가 설정에 따라 Mock 또는 Pending으로 안전하게 조립되는지 검증합니다. */
class AdminAnalyticsConfigTest {

    private static final Instant NOW = Instant.parse("2026-08-23T02:00:00Z");

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(TestClockConfig.class, AdminAnalyticsConfig.class);

    /** 별도 설정 없이 개발용 Mock 원천과 Mock 전용 최신성 기준을 사용하는지 검증합니다. */
    @Test
    void defaultsToMockSource() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(AdminAnalyticsService.class)
                    .getAnalytics(query()).sourceType()).isEqualTo(AnalyticsSourceType.MOCK);
            assertThat(context.getBean(AdminAnalyticsProperties.class).staleAfter())
                    .isEqualTo(java.time.Duration.ofHours(24));
        });
    }

    /** Mock을 명시적으로 비활성화하면 실제 Source 연결 전 Pending 응답을 사용하는지 검증합니다. */
    @Test
    void disablesMockSourceExplicitly() {
        runner.withPropertyValues("admin.analytics.mock-enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(AdminAnalyticsProperties.class).staleAfter())
                            .isEqualTo(java.time.Duration.ofHours(24));
                    assertThat(context.getBean(AdminAnalyticsFreshnessPolicy.class).evaluate(
                            AggregateAvailability.AVAILABLE,
                            NOW.minusSeconds(60),
                            NOW)).isEqualTo(SourceStatus.VALID);
                    assertThat(context.getBean(AdminAnalyticsService.class)
                            .getAnalytics(query()).sourceType()).isEqualTo(AnalyticsSourceType.NONE);
                });
    }

    /** 명시적으로 제공한 최신성 기준이 0 이하이면 기동 단계에서 거부하는지 검증합니다. */
    @Test
    void rejectsNonPositiveStaleAfter() {
        runner.withPropertyValues(
                        "admin.analytics.mock-enabled=true",
                        "admin.analytics.stale-after=0s")
                .run(context -> assertThat(context).hasFailed());
    }

    /** 테스트에 공통으로 사용할 유효한 Core 조회 조건을 만듭니다. */
    private static AdminAnalyticsQuery query() {
        return new AdminAnalyticsQuery(
                java.time.LocalDate.parse("2026-01-01"),
                java.time.LocalDate.parse("2026-03-31"), null, null,
                java.time.ZoneId.of("Asia/Seoul"));
    }

    /** 분석 설정과 고정 시간을 최소 Spring Context에 제공합니다. */
    @Configuration(proxyBeanMethods = false)
    static class TestClockConfig {

        /** 모든 분석이 같은 현재 시각을 사용하도록 고정합니다. */
        @Bean
        TimeProvider timeProvider() {
            return new TimeProvider(Clock.fixed(NOW, ZoneOffset.UTC));
        }
    }
}
