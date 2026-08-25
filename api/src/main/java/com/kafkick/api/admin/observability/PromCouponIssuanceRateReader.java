package com.kafkick.api.admin.observability;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.admin.couponmetrics.CouponIssuanceRateReader;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSource;
import com.kafkick.core.observation.SourceStatus;

/** Prometheus의 캠페인 성공 발급 rate matrix를 Core 발급률 관측으로 변환합니다. */
public class PromCouponIssuanceRateReader implements CouponIssuanceRateReader {

    private final PromRangeQuery rangeQuery;
    private final PromTimeQuery timeQuery;
    private final PrometheusSeriesProperties seriesProperties;
    private final Duration staleAfter;

    /**
     * series 전용 range 경계와 instant freshness 경계로 발급률 Reader를 만듭니다.
     *
     * @param rangeQuery 5초 평가점 matrix를 읽는 range 경계
     * @param timeQuery 실제 scrape 시각을 읽는 instant 경계
     * @param seriesProperties range 평가 간격과 상한 설정
     * @param staleAfter 마지막 실제 scrape 이후 stale로 볼 기간
     */
    public PromCouponIssuanceRateReader(
            PromRangeQuery rangeQuery,
            PromTimeQuery timeQuery,
            PrometheusSeriesProperties seriesProperties,
            Duration staleAfter
    ) {
        this.rangeQuery = Objects.requireNonNull(rangeQuery, "rangeQuery");
        this.timeQuery = Objects.requireNonNull(timeQuery, "timeQuery");
        this.seriesProperties = Objects.requireNonNull(seriesProperties, "seriesProperties");
        this.staleAfter = requirePositive(staleAfter, "staleAfter");
    }

    /**
     * 요청 기준 시각에 고정한 range rate와 실제 scrape freshness를 함께 읽습니다.
     *
     * @param couponId 조회할 양수 쿠폰 ID
     * @param window 화면이 요청한 1·5·15분 구간
     * @param snapshotAt Service가 요청 전체에서 공유한 기준 시각
     * @return Prometheus 결과를 상태와 표본으로 보존한 기술 중립 관측값
     */
    @Override
    public CouponMetricsSource.Observation<List<CouponMetricsSource.IssuanceRateSample>> read(
            long couponId,
            MetricsWindow window,
            Instant snapshotAt
    ) {
        Objects.requireNonNull(window, "window");
        Objects.requireNonNull(snapshotAt, "snapshotAt");
        try {
            Duration rateWindow = seriesProperties.step();
            List<PromRangeSeries> series = rangeQuery.query(
                    CouponMetricsPrometheusContract.successRate(couponId, rateWindow),
                    snapshotAt.minus(window.duration()), snapshotAt, rateWindow);
            if (series.isEmpty()) {
                return empty(SourceStatus.PENDING);
            }
            if (series.size() != 1) {
                throw new PromQueryException("성공 발급률 range 결과는 단일 시계열이어야 합니다.");
            }
            List<CouponMetricsSource.IssuanceRateSample> samples = rateSamples(
                    series.getFirst().points(), snapshotAt.minus(window.duration()), snapshotAt);
            if (samples.isEmpty()) {
                throw new PromQueryException("성공 발급률 range 결과에 평가점이 없습니다.");
            }
            Instant observedAt = singleEpoch(timeQuery.query(
                    CouponMetricsPrometheusContract.successFreshnessEpoch(couponId), snapshotAt), snapshotAt);
            if (!isComplete(samples, window, snapshotAt)) {
                // 확보한 표본은 반환하되 전체 창을 덮지 못한 사실을 WARMING_UP으로 보존합니다.
                return observed(samples, SourceStatus.WARMING_UP, observedAt);
            }
            if (Duration.between(observedAt, snapshotAt).compareTo(staleAfter) > 0) {
                return observed(samples, SourceStatus.STALE, observedAt);
            }
            if (samples.stream().allMatch(sample -> sample.perSecond() == 0.0)) {
                return observed(samples, SourceStatus.NO_TRAFFIC, observedAt);
            }
            return observed(samples, SourceStatus.VALID, observedAt);
        } catch (RuntimeException failure) {
            return empty(SourceStatus.UNAVAILABLE);
        }
    }

    /** matrix 점을 요청 구간의 시간순 유한·비음수 Core rate 표본으로 바꿉니다. */
    private static List<CouponMetricsSource.IssuanceRateSample> rateSamples(
            List<PromRangePoint> points,
            Instant windowStart,
            Instant snapshotAt
    ) {
        List<CouponMetricsSource.IssuanceRateSample> samples = points.stream()
                .map(point -> rateSample(point, windowStart, snapshotAt))
                .toList();
        for (int index = 1; index < samples.size(); index++) {
            if (!samples.get(index).observedAt().isAfter(samples.get(index - 1).observedAt())) {
                throw new PromQueryException("성공 발급률 평가점 시각은 오름차순이어야 합니다.");
            }
        }
        return samples;
    }

    /** 한 range 점이 요청 구간 안의 유한·비음수 rate인지 확인합니다. */
    private static CouponMetricsSource.IssuanceRateSample rateSample(
            PromRangePoint point,
            Instant windowStart,
            Instant snapshotAt
    ) {
        if (!point.hasNumericValue() || point.value() < 0.0
                || point.observedAt().isBefore(windowStart) || point.observedAt().isAfter(snapshotAt)) {
            throw new PromQueryException("유효한 요청 구간 발급률 평가점이 아닙니다.");
        }
        return new CouponMetricsSource.IssuanceRateSample(point.observedAt(), point.value());
    }

    /** freshness 질의의 유일한 epoch 초 표본을 실제 관측 시각으로 변환합니다. */
    private static Instant singleEpoch(List<PromSample> samples, Instant snapshotAt) {
        if (samples.size() != 1 || !samples.getFirst().hasNumericValue()) {
            throw new PromQueryException("성공 발급 freshness 결과는 유한한 단일 표본이어야 합니다.");
        }
        double epochSeconds = samples.getFirst().value();
        if (epochSeconds < 0.0 || epochSeconds > 4_102_444_800.0) {
            throw new PromQueryException("성공 발급 freshness epoch이 유효하지 않습니다.");
        }
        Instant observedAt = Instant.ofEpochMilli(Math.round(epochSeconds * 1_000.0));
        if (observedAt.isAfter(snapshotAt)) {
            throw new PromQueryException("성공 발급 freshness가 snapshotAt 이후입니다.");
        }
        return observedAt;
    }

    /** 모든 5초 평가점이 요청 구간의 시작과 끝을 빠짐없이 덮는지 확인합니다. */
    private boolean isComplete(
            List<CouponMetricsSource.IssuanceRateSample> samples,
            MetricsWindow window,
            Instant snapshotAt
    ) {
        long expectedPoints = window.duration().toSeconds() / seriesProperties.step().toSeconds() + 1L;
        if (samples.size() != expectedPoints
                || !samples.getFirst().observedAt().equals(snapshotAt.minus(window.duration()))
                || !samples.getLast().observedAt().equals(snapshotAt)) {
            return false;
        }
        for (int index = 1; index < samples.size(); index++) {
            Instant expectedAt = samples.get(index - 1).observedAt().plus(seriesProperties.step());
            if (!samples.get(index).observedAt().equals(expectedAt)) {
                return false;
            }
        }
        return true;
    }

    /** 값을 실을 수 없는 관측 상태를 생성합니다. */
    private static CouponMetricsSource.Observation<List<CouponMetricsSource.IssuanceRateSample>> empty(
            SourceStatus status
    ) {
        return new CouponMetricsSource.Observation<>(null, status, null);
    }

    /** rate 표본과 실제 scrape 시각을 함께 보존합니다. */
    private static CouponMetricsSource.Observation<List<CouponMetricsSource.IssuanceRateSample>> observed(
            List<CouponMetricsSource.IssuanceRateSample> samples,
            SourceStatus status,
            Instant observedAt
    ) {
        return new CouponMetricsSource.Observation<>(samples, status, observedAt);
    }

    /** 양수 Duration 설정만 허용합니다. */
    private static Duration requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + "은 양수여야 합니다.");
        }
        return value;
    }
}
