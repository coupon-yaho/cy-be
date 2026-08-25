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
import com.kafkick.core.admin.analytics.AdminAnalyticsPendingSource;
import com.kafkick.core.admin.analytics.AdminAnalyticsQuery;
import com.kafkick.core.admin.analytics.AdminAnalyticsService;
import com.kafkick.core.admin.analytics.AdminAnalyticsSource;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AnalyticsSourceType;
import com.kafkick.core.admin.analytics.mock.AdminAnalyticsMockSource;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;

/** 관리자 분석 Source가 설정에 따라 Mock 또는 Pending으로 안전하게 조립되는지 검증합니다. */
class AdminAnalyticsConfigTest {

    private static final Instant NOW = Instant.parse("2026-08-23T02:00:00Z");

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(TestClockConfig.class, AdminAnalyticsConfig.class);

    /**
     * <b>[OBS-36] 별도 설정 없이는 Mock 이 아니라 Pending 이다.</b>
     *
     * <p>예전에는 이 테스트가 {@code defaultsToMockSource} 라는 이름으로 정반대를 고정하고
     * 있었다. {@code @DefaultValue("true")} 였고 이 키를 설정하는 yml 이 하나도 없어서,
     * {@code AdminAnalyticsConfig} 의 "운영 기본값에서는 가짜 통계를 만들지 않는다" 는 주석과
     * 반대로 <b>운영에서도 Mock 이 200 으로 나갔다.</b> 기본값을 뒤집고 이 테스트를 함께 뒤집는다 —
     * 한쪽만 고치면 다른 쪽이 그것을 되돌리는 근거가 된다.
     */
    @Test
    void defaultsToPendingSource() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(AdminAnalyticsSource.class);
            assertThat(context.getBean(AdminAnalyticsSource.class))
                    .isInstanceOf(AdminAnalyticsPendingSource.class);
            assertThat(context.getBean(AdminAnalyticsService.class)
                    .getAnalytics(query()).sourceType()).isEqualTo(AnalyticsSourceType.NONE);
            assertThat(context.getBean(AdminAnalyticsProperties.class).staleAfter())
                    .isEqualTo(java.time.Duration.ofHours(24));
        });
    }

    /** 로컬·시연용으로 명시적으로 켜면 개발용 Mock 원천을 쓰는지 검증합니다. */
    @Test
    void enablesMockSourceExplicitly() {
        runner.withPropertyValues("admin.analytics.mock-enabled=true")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(AdminAnalyticsSource.class);
                    assertThat(context.getBean(AdminAnalyticsSource.class))
                            .isInstanceOf(AdminAnalyticsMockSource.class);
                    assertThat(context.getBean(AdminAnalyticsService.class)
                            .getAnalytics(query()).sourceType()).isEqualTo(AnalyticsSourceType.MOCK);
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
