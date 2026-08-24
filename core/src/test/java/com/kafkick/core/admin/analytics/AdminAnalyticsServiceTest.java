package com.kafkick.core.admin.analytics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AggregateAvailability;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AggregateObservation;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AnalyticsSourceType;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.BrandRef;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.CampaignRef;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.CatalogSnapshot;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;

/** 관리자 분석 Service의 조회 횟수와 필터 검증 책임을 확인합니다. */
class AdminAnalyticsServiceTest {

    private static final Instant NOW = Instant.parse("2026-03-11T00:30:00Z");
    private static final AdminAnalyticsQuery QUERY = new AdminAnalyticsQuery(
            LocalDate.parse("2026-01-01"),
            LocalDate.parse("2026-03-31"),
            1L,
            101L,
            ZoneId.of("Asia/Seoul"));

    /** 한 요청이 같은 Dataset과 평가 시각을 공유하도록 의존성을 한 번씩만 호출하는지 검증합니다. */
    @Test
    @DisplayName("Service는 Source와 TimeProvider를 요청당 한 번만 호출한다")
    void loadsSourceAndTimeExactlyOnce() {
        RecordingSource source = new RecordingSource(availableDataset());
        RecordingTimeProvider timeProvider = new RecordingTimeProvider();
        AdminAnalyticsService service = service(source, timeProvider);

        AdminAnalyticsResult result = service.getAnalytics(QUERY);

        assertThat(result.sourceType()).isEqualTo(AnalyticsSourceType.MOCK);
        assertThat(source.callCount).isEqualTo(1);
        assertThat(source.lastQuery).isSameAs(QUERY);
        assertThat(timeProvider.callCount).isEqualTo(1);
    }

    /** 카탈로그가 확인된 경우 브랜드·캠페인 소속 불일치를 0건으로 숨기지 않는지 검증합니다. */
    @Test
    @DisplayName("Service는 브랜드와 캠페인 소속이 다르면 ANALYTICS-004를 반환한다")
    void rejectsCampaignOwnedByAnotherBrand() {
        CatalogSnapshot catalog = new CatalogSnapshot(
                AggregateAvailability.AVAILABLE,
                List.of(new BrandRef(1L, "브랜드 1"), new BrandRef(2L, "브랜드 2")),
                List.of(new CampaignRef(
                        101L, 2L, LocalDate.parse("2026-01-01"), LocalDate.parse("2026-12-31"))));
        RecordingSource source = new RecordingSource(dataset(catalog));

        assertThatThrownBy(() -> service(source, new RecordingTimeProvider()).getAnalytics(QUERY))
                .isInstanceOf(BusinessException.class)
                .satisfies(exception -> assertThat(((BusinessException) exception)
                        .getErrorCode().getCode()).isEqualTo("ANALYTICS-004"));
    }

    /** 확인된 카탈로그에 요청 브랜드가 없으면 브랜드 전용 오류로 구분하는지 검증합니다. */
    @Test
    @DisplayName("Service는 존재하지 않는 브랜드에 ANALYTICS-002를 반환한다")
    void rejectsMissingBrand() {
        AdminAnalyticsQuery query = new AdminAnalyticsQuery(
                QUERY.from(), QUERY.to(), 999L, null, QUERY.zoneId());
        RecordingSource source = new RecordingSource(availableDataset());

        assertThatThrownBy(() -> service(source, new RecordingTimeProvider()).getAnalytics(query))
                .isInstanceOf(BusinessException.class)
                .satisfies(exception -> assertThat(((BusinessException) exception)
                        .getErrorCode().getCode()).isEqualTo("ANALYTICS-002"));
    }

    /** 확인된 카탈로그에 요청 캠페인이 없으면 캠페인 전용 오류로 구분하는지 검증합니다. */
    @Test
    @DisplayName("Service는 존재하지 않는 캠페인에 ANALYTICS-003을 반환한다")
    void rejectsMissingCampaign() {
        AdminAnalyticsQuery query = new AdminAnalyticsQuery(
                QUERY.from(), QUERY.to(), null, 999L, QUERY.zoneId());
        RecordingSource source = new RecordingSource(availableDataset());

        assertThatThrownBy(() -> service(source, new RecordingTimeProvider()).getAnalytics(query))
                .isInstanceOf(BusinessException.class)
                .satisfies(exception -> assertThat(((BusinessException) exception)
                        .getErrorCode().getCode()).isEqualTo("ANALYTICS-003"));
    }

    /** 카탈로그 자체가 아직 없을 때 필터 ID를 미존재로 오판하지 않는지 검증합니다. */
    @Test
    @DisplayName("Pending Source는 필터가 있어도 404가 아니라 분석별 PENDING을 반환한다")
    void pendingCatalogDoesNotBecomeNotFound() {
        RecordingSource source = new RecordingSource(AdminAnalyticsPendingSource.pendingDataset());

        AdminAnalyticsResult result = service(source, new RecordingTimeProvider()).getAnalytics(QUERY);

        assertThat(result.sourceType()).isEqualTo(AnalyticsSourceType.NONE);
        assertThat(result.brandTrends().status()).isEqualTo(SourceStatus.PENDING);
        assertThat(result.hourlyHeatmap().status()).isEqualTo(SourceStatus.PENDING);
        assertThat(result.issuanceStatusDistribution().status()).isEqualTo(SourceStatus.PENDING);
    }

    private static AdminAnalyticsService service(
            AdminAnalyticsSource source,
            TimeProvider timeProvider
    ) {
        return new AdminAnalyticsService(
                source,
                timeProvider,
                new AdminAnalyticsCalculator(
                        new AdminAnalyticsFreshnessPolicy(Duration.ofHours(1))));
    }

    private static AdminAnalyticsDataset availableDataset() {
        return dataset(new CatalogSnapshot(
                AggregateAvailability.AVAILABLE,
                List.of(new BrandRef(1L, "브랜드 1")),
                List.of(new CampaignRef(
                        101L, 1L, LocalDate.parse("2026-01-01"), LocalDate.parse("2026-12-31")))));
    }

    private static AdminAnalyticsDataset dataset(CatalogSnapshot catalog) {
        Instant observedAt = NOW.minusSeconds(60);
        return new AdminAnalyticsDataset(
                AnalyticsSourceType.MOCK,
                catalog,
                new AggregateObservation<>(List.of(), AggregateAvailability.AVAILABLE, observedAt),
                new AggregateObservation<>(List.of(), AggregateAvailability.AVAILABLE, observedAt),
                new AggregateObservation<>(List.of(), AggregateAvailability.AVAILABLE, observedAt));
    }

    /** Source 호출 횟수와 전달 Query를 기록합니다. */
    private static final class RecordingSource implements AdminAnalyticsSource {

        private final AdminAnalyticsDataset dataset;
        private int callCount;
        private AdminAnalyticsQuery lastQuery;

        private RecordingSource(AdminAnalyticsDataset dataset) {
            this.dataset = dataset;
        }

        @Override
        public AdminAnalyticsDataset load(AdminAnalyticsQuery query) {
            callCount++;
            lastQuery = query;
            return dataset;
        }
    }

    /** 고정된 시각을 반환하며 호출 횟수를 기록합니다. */
    private static final class RecordingTimeProvider extends TimeProvider {

        private int callCount;

        private RecordingTimeProvider() {
            super(Clock.fixed(NOW, ZoneOffset.UTC));
        }

        @Override
        public Instant instant() {
            callCount++;
            return super.instant();
        }
    }
}
