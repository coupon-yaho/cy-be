package com.kafkick.core.admin.couponmetrics.mock;

import static com.kafkick.core.observation.SourceStatus.N_A;
import static com.kafkick.core.observation.SourceStatus.VALID;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.springframework.stereotype.Component;

import com.kafkick.core.admin.couponmetrics.CouponMetricsSource;
import com.kafkick.core.admin.overview.CampaignOverviewSource;
import com.kafkick.core.admin.overview.calculator.CampaignQueueCalculator.QueueInput;
import com.kafkick.core.admin.overview.mock.AdminOverviewMockDataFactory;
import com.kafkick.core.admin.overview.mock.AdminOverviewMockDataset;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.SourceStatus;

/**
 * Overview Mock과 같은 캠페인 모집단을 사용하는 상세 지표 원천 Fixture를 제공합니다.
 *
 * <p>캠페인 기본 정보·재고·대기열은 Overview Dataset에서 그대로 가져옵니다. 상세 화면에만 필요한
 * 누적 발급 Counter, 상태별 보유량과 전이 버킷은 원천 수량으로만 보강하며 파생 비율이나 속도는
 * 계산하지 않습니다.</p>
 */
@Component
public class AdminCouponMetricsMockDataFactory {

    private static final Duration SAMPLE_INTERVAL = Duration.ofMinutes(1);
    private static final int SAMPLE_INTERVAL_COUNT = 15;

    private final AdminOverviewMockDataFactory overviewFactory;

    /**
     * 상세 지표 모집단의 정본으로 사용할 Overview Mock Factory를 주입받습니다.
     *
     * @param overviewFactory 캠페인·재고·대기 원천을 제공하는 기존 Factory
     */
    public AdminCouponMetricsMockDataFactory(AdminOverviewMockDataFactory overviewFactory) {
        this.overviewFactory = Objects.requireNonNull(overviewFactory, "overviewFactory");
    }

    /**
     * 같은 기준 시각의 Overview Dataset에서 쿠폰을 찾아 상세 원천값으로 변환합니다.
     *
     * @param snapshotAt 모든 Mock 원천이 공유하는 기준 시각
     * @param couponId 조회할 쿠폰 ID
     * @return 같은 ID의 상세 지표 원천 또는 빈 Optional
     */
    public Optional<CouponMetricsSource> find(Instant snapshotAt, long couponId) {
        Objects.requireNonNull(snapshotAt, "snapshotAt");
        AdminOverviewMockDataset overview = overviewFactory.create(snapshotAt);
        return overview.campaigns().stream()
                .filter(campaign -> campaign.couponId().equals(couponId))
                .findFirst()
                .map(campaign -> detailSource(
                        campaign, queueFor(overview.queueInputs(), couponId), snapshotAt));
    }

    /** 같은 Dataset 안에서 캠페인 ID와 일치하는 유일한 대기 원천을 찾습니다. */
    private static QueueInput queueFor(List<QueueInput> queues, long couponId) {
        return queues.stream()
                .filter(queue -> queue.couponId().equals(couponId))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Overview Dataset에 대기 원천이 없습니다."));
    }

    /** Overview 원천과 상세 전용 원시 수량을 하나의 기술 중립 Source로 조립합니다. */
    private static CouponMetricsSource detailSource(
            CampaignOverviewSource campaign,
            QueueInput queue,
            Instant snapshotAt
    ) {
        if (campaign.status() != CouponRoundStatus.OPEN) {
            return notApplicable(campaign);
        }
        return new CouponMetricsSource(
                campaign.couponId(),
                new CouponMetricsSource.CampaignRuntime(campaign.status(), campaign.opensAt()),
                stock(campaign),
                observed(issuanceSamples(campaign.couponId(), snapshotAt), snapshotAt),
                queue(queue),
                holdings(campaign),
                observed(transitionBuckets(campaign.couponId(), snapshotAt), snapshotAt));
    }

    /** 예약·종료 캠페인의 상세 계산 비적용 영역을 모두 N_A로 명시합니다. */
    private static CouponMetricsSource notApplicable(CampaignOverviewSource campaign) {
        return new CouponMetricsSource(
                campaign.couponId(),
                new CouponMetricsSource.CampaignRuntime(campaign.status(), campaign.opensAt()),
                empty(N_A),
                empty(N_A),
                empty(N_A),
                empty(N_A),
                empty(N_A));
    }

    /** Overview 재고 수량·상태·관측 시각을 손실 없이 상세 원천으로 변환합니다. */
    private static CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> stock(
            CampaignOverviewSource campaign
    ) {
        if (!campaign.stockStatus().carriesValue()) {
            return empty(campaign.stockStatus());
        }
        return new CouponMetricsSource.Observation<>(
                new CouponMetricsSource.StockCounts(
                        campaign.totalQuantity(), campaign.activeCount()),
                campaign.stockStatus(),
                campaign.stockObservedAt());
    }

    /** Overview 대기·입장 수량과 실제 관측 구간을 상세 대기 원천으로 변환합니다. */
    private static CouponMetricsSource.Observation<CouponMetricsSource.QueueCounts> queue(
            QueueInput queue
    ) {
        if (!queue.sourceStatus().carriesValue()) {
            return empty(queue.sourceStatus());
        }
        return new CouponMetricsSource.Observation<>(
                new CouponMetricsSource.QueueCounts(
                        queue.currentWaitingCount(), queue.admittedCount(),
                        queue.windowStart(), queue.windowEnd()),
                queue.sourceStatus(),
                queue.observedAt());
    }

    /** 활성 발급 수량과 합이 맞는 발급·사용 보유량 및 별도 종료 상태 수량을 만듭니다. */
    private static CouponMetricsSource.Observation<CouponMetricsSource.IssuanceStatusCounts> holdings(
            CampaignOverviewSource campaign
    ) {
        if (!campaign.stockStatus().carriesValue()) {
            return empty(campaign.stockStatus());
        }
        long used = switch (campaign.couponId().intValue()) {
            case 101 -> 2_070L;
            case 102 -> 1_330L;
            case 103 -> 155L;
            default -> 0L;
        };
        long issued = Math.subtractExact(campaign.activeCount(), used);
        CouponMetricsSource.IssuanceStatusCounts counts = new CouponMetricsSource.IssuanceStatusCounts(
                issued, used, campaign.couponId() % 17L, campaign.couponId() % 11L);
        return new CouponMetricsSource.Observation<>(
                counts, campaign.stockStatus(), campaign.stockObservedAt());
    }

    /** 15분 전 기준점부터 현재까지의 불변 누적 완료 Counter 표본을 만듭니다. */
    private static List<CouponMetricsSource.IssuanceCounterSample> issuanceSamples(
            long couponId,
            Instant snapshotAt
    ) {
        long[] completedPerMinute = completedPerMinute(couponId);
        List<CouponMetricsSource.IssuanceCounterSample> samples = new ArrayList<>();
        long cumulative = couponId * 1_000L;
        Instant firstObservedAt = snapshotAt.minus(Duration.ofMinutes(SAMPLE_INTERVAL_COUNT));
        samples.add(new CouponMetricsSource.IssuanceCounterSample(firstObservedAt, cumulative));
        for (int index = 0; index < completedPerMinute.length; index++) {
            cumulative = Math.addExact(cumulative, completedPerMinute[index]);
            samples.add(new CouponMetricsSource.IssuanceCounterSample(
                    firstObservedAt.plus(SAMPLE_INTERVAL.multipliedBy(index + 1L)), cumulative));
        }
        return List.copyOf(samples);
    }

    /** 캠페인별 화면 시나리오에 사용할 15개의 원시 분당 완료 건수를 제공합니다. */
    private static long[] completedPerMinute(long couponId) {
        return switch ((int) couponId) {
            case 101 -> new long[] { 240, 300, 360, 420, 480, 540, 600, 660, 720, 780, 840, 900, 960, 720, 600 };
            case 102 -> new long[] { 180, 210, 240, 270, 300, 330, 360, 390, 420, 450, 480, 510, 540, 570, 600 };
            case 103 -> new long[] { 60, 72, 84, 96, 108, 120, 132, 144, 156, 168, 180, 192, 204, 216, 228 };
            default -> throw new IllegalArgumentException("OPEN 상세 Mock 수량이 없는 couponId입니다: " + couponId);
        };
    }

    /** 요청 구간별 집계가 가능하도록 15개의 1분 상태 전이 원시 버킷을 만듭니다. */
    private static List<CouponMetricsSource.TransitionBucket> transitionBuckets(
            long couponId,
            Instant snapshotAt
    ) {
        List<CouponMetricsSource.TransitionBucket> buckets = new ArrayList<>();
        Instant firstStart = snapshotAt.minus(Duration.ofMinutes(SAMPLE_INTERVAL_COUNT));
        long scale = couponId == 101L ? 3L : couponId == 102L ? 2L : 1L;
        for (int index = 0; index < SAMPLE_INTERVAL_COUNT; index++) {
            Instant start = firstStart.plus(SAMPLE_INTERVAL.multipliedBy(index));
            buckets.add(new CouponMetricsSource.TransitionBucket(
                    start,
                    start.plus(SAMPLE_INTERVAL),
                    scale * (index + 1L),
                    scale * (index % 3L),
                    scale * (index % 2L),
                    scale * (index % 4L)));
        }
        return List.copyOf(buckets);
    }

    /** 값을 실제로 관측한 정상 Mock Observation을 생성합니다. */
    private static <T> CouponMetricsSource.Observation<T> observed(T value, Instant observedAt) {
        return new CouponMetricsSource.Observation<>(value, VALID, observedAt);
    }

    /** 값과 관측 시각이 없는 비적용·미관측 Observation을 생성합니다. */
    private static <T> CouponMetricsSource.Observation<T> empty(SourceStatus status) {
        return new CouponMetricsSource.Observation<>(null, status, null);
    }
}
