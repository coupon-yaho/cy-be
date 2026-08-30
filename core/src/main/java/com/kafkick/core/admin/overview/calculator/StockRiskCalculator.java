package com.kafkick.core.admin.overview.calculator;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.springframework.stereotype.Component;

import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.admin.overview.OverviewCalculationPolicy;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.SourceStatus;

/** 권위 재고와 O1 발급률을 결합해 O4 소진 예상과 전체 위험을 계산하는 순수 계산기입니다. */
@Component
public class StockRiskCalculator {

    /** 상태 없는 O4 순수 계산기를 생성합니다. */
    public StockRiskCalculator() { }

    /**
     * 쿠폰 회차별 V1 잔량·비율·ETA와 같은 모집단의 전체 소진 위험을 계산합니다.
     *
     * <p>발급률 0, STALE, WARMING_UP, 값 없음은 ETA null이며 재고 수치는 보존합니다. 적용 쿠폰 회차
     * 하나라도 재고 또는 O1이 미수집이면 전역 위험은 부분 합계가 아닌 UNAVAILABLE입니다.
     * 소진 임박만으로는 조치 후보를 만들지 않습니다.</p>
     *
     * @param policy 소진 임박 기준 시간
     * @param inputs V1 수량과 couponId별 O1 관측값
     * @return 쿠폰 회차별 O4와 전체 소진 위험 관측값
     */
    public StockRiskCalculation calculate(OverviewCalculationPolicy policy, List<StockInput> inputs) {
        Objects.requireNonNull(policy, "policy");
        Objects.requireNonNull(inputs, "inputs");
        Map<Long, AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.StockForecast>> forecasts =
                new LinkedHashMap<>();
        List<SourceStatus> applicableStatuses = new ArrayList<>();
        long riskCount = 0L;
        Duration nearest = null;
        Instant observedAt = null;
        for (StockInput input : inputs) {
            Objects.requireNonNull(input, "inputs에는 null을 포함할 수 없습니다.");
            AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.StockForecast> forecast = calculateOne(input);
            if (forecasts.put(input.couponId(), forecast) != null) {
                throw new IllegalArgumentException("couponId는 중복될 수 없습니다.");
            }
            SourceStatus issuanceStatus = input.issuanceFlow() == null
                    ? SourceStatus.UNAVAILABLE : input.issuanceFlow().status();
            // 예약·종료처럼 재고 위험이 적용되지 않는 쿠폰 회차는 전역 모집단에서도 제외합니다.
            if (input.stockStatus() == SourceStatus.N_A || issuanceStatus == SourceStatus.N_A) {
                continue;
            }
            applicableStatuses.add(aggregateStatus(List.of(input.stockStatus(), issuanceStatus)));
            // 재고와 O1 중 하나라도 값이 없으면 해당 행을 전역 위험 숫자에 부분 반영하지 않습니다.
            if (!input.stockStatus().carriesValue() || !issuanceStatus.carriesValue()) {
                continue;
            }
            // 결합 결과의 최신성은 두 원천 중 더 오래된 관측 시각을 기준으로 보수적으로 잡습니다.
            Instant rowObservedAt = input.issuanceFlow().observedAt().isBefore(input.observedAt())
                    ? input.issuanceFlow().observedAt() : input.observedAt();
            observedAt = observedAt == null || rowObservedAt.isBefore(observedAt)
                    ? rowObservedAt : observedAt;
            Duration eta = forecast.value().estimatedDepletion();
            // 계산 가능한 ETA가 임계시간 이내인 쿠폰 회차만 상단 소진 위험에 집계합니다.
            if (eta != null && eta.compareTo(policy.stockDepletionThreshold()) <= 0) {
                riskCount++;
                if (nearest == null || eta.compareTo(nearest) < 0) {
                    nearest = eta;
                }
            }
        }
        SourceStatus aggregateStatus = aggregateStatus(applicableStatuses);
        AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.StockRiskSummary> risk = !aggregateStatus.carriesValue()
                ? new AdminOverviewSnapshot.Observation<>(null, aggregateStatus, null)
                : new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.StockRiskSummary(riskCount, nearest),
                        aggregateStatus, observedAt);
        return new StockRiskCalculation(forecasts, risk);
    }

    /** 권위 재고 수량이 유효할 때 실제 잔량을 만들고, 적격 O1만 ETA 계산에 사용합니다. */
    private static AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.StockForecast> calculateOne(StockInput input) {
        if (!input.stockStatus().carriesValue()) {
            return new AdminOverviewSnapshot.Observation<>(null, input.stockStatus(), null);
        }
        validateQuantity(input.totalQuantity(), input.activeCount());
        // Resolver가 V1 DB active와 V2 Redis 잔량을 모두 같은 '발급 수량' 의미로 정규화했습니다.
        long remaining = input.totalQuantity() - input.activeCount();
        Duration eta = depletionEta(remaining, input.issuanceFlow());
        return new AdminOverviewSnapshot.Observation<>(new AdminOverviewSnapshot.StockForecast(remaining,
                input.totalQuantity(), remaining / (double) input.totalQuantity(), eta),
                input.stockStatus(), input.observedAt());
    }

    /** O1 VALID·NO_TRAFFIC 외에는 최신성 없는 값을 소진 예측에 사용하지 않습니다. */
    private static Duration depletionEta(long remaining,
            AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow> issuanceFlow) {
        if (issuanceFlow == null || issuanceFlow.value() == null
                || (issuanceFlow.status() != SourceStatus.VALID
                && issuanceFlow.status() != SourceStatus.NO_TRAFFIC)) {
            return null;
        }
        if (!Double.isFinite(issuanceFlow.value().currentPerMinute())) {
            throw new IllegalArgumentException("O1 currentPerMinute은 유한해야 합니다.");
        }
        if (issuanceFlow.value().currentPerMinute() < 0.0) {
            throw new IllegalArgumentException("O1 currentPerMinute은 음수일 수 없습니다.");
        }
        if (issuanceFlow.status() == SourceStatus.NO_TRAFFIC
                && issuanceFlow.value().currentPerMinute() != 0.0) {
            throw new IllegalArgumentException("NO_TRAFFIC O1 rate는 0이어야 합니다.");
        }
        if (issuanceFlow.value().currentPerMinute() == 0.0) {
            return null;
        }
        // 현재 분당 발급 속도가 유지된다고 가정해 잔량이 소진될 때까지의 초를 계산합니다.
        double seconds = remaining / issuanceFlow.value().currentPerMinute() * 60.0;
        if (!Double.isFinite(seconds) || seconds < 0.0 || seconds >= 0x1.0p63) {
            throw new IllegalArgumentException("ETA 초 값이 유한한 Duration 범위를 벗어났습니다.");
        }
        return Duration.ofSeconds((long) Math.ceil(seconds));
    }

    /** 적용 원천의 불완전 상태를 전역 위험에서 숨기지 않는 합성 순서입니다. */
    private static SourceStatus aggregateStatus(List<SourceStatus> statuses) {
        if (statuses.isEmpty()) return SourceStatus.N_A;
        if (statuses.contains(SourceStatus.UNAVAILABLE)) return SourceStatus.UNAVAILABLE;
        if (statuses.contains(SourceStatus.PENDING)) return SourceStatus.PENDING;
        if (statuses.contains(SourceStatus.STALE)) return SourceStatus.STALE;
        if (statuses.contains(SourceStatus.WARMING_UP)) return SourceStatus.WARMING_UP;
        return statuses.stream().allMatch(status -> status == SourceStatus.NO_TRAFFIC)
                ? SourceStatus.NO_TRAFFIC : SourceStatus.VALID;
    }

    /** V1의 총수량 0·음수와 activeCount 역전은 실제 재고 0으로 바꾸지 않습니다. */
    private static void validateQuantity(Long totalQuantity, Long activeCount) {
        if (totalQuantity == null || activeCount == null || totalQuantity <= 0L
                || activeCount < 0L || activeCount > totalQuantity) {
            throw new IllegalArgumentException("V1 재고 수량 범위가 유효하지 않습니다.");
        }
    }

    /**
     * O4 재고 입력입니다.
     *
     * @param couponId 쿠폰 회차 식별자
     * @param engineVersion 재고 엔진; 이번 계산은 V1만 수량 계산함
     * @param totalQuantity V1 전체 발급 가능 수량
     * @param activeCount V1에서 이미 활성화·발급된 수량
     * @param stockStatus 재고 원천 상태
     * @param observedAt 값이 있는 재고 원천의 실제 관측 시각
     * @param issuanceFlow 같은 couponId의 O1 결과; ETA 계산에만 사용
     */
    public record StockInput(Long couponId, EngineVersion engineVersion, Long totalQuantity, Long activeCount,
                             SourceStatus stockStatus, Instant observedAt,
                             AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow> issuanceFlow) {
        /** 식별자·엔진·상태와 상태별 관측 시각 관계를 검증합니다. */
        public StockInput {
            Objects.requireNonNull(couponId, "couponId");
            Objects.requireNonNull(engineVersion, "engineVersion");
            Objects.requireNonNull(stockStatus, "stockStatus");
            if (stockStatus.carriesValue() != (observedAt != null)) {
                throw new IllegalArgumentException("재고 상태와 observedAt 조합이 맞지 않습니다.");
            }
        }
    }

    /**
     * O4 계산 결과입니다.
     *
     * @param stockForecasts couponId별 재고 관측값
     * @param stockRisk 모든 적용 쿠폰 회차가 수집된 경우에만 계산한 전체 위험
     */
    public record StockRiskCalculation(
            Map<Long, AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.StockForecast>> stockForecasts,
            AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.StockRiskSummary> stockRisk) {
        /** 결과 Map을 불변 복사하고 전체 위험 관측값 존재를 보장합니다. */
        public StockRiskCalculation {
            stockForecasts = Map.copyOf(stockForecasts);
            Objects.requireNonNull(stockRisk, "stockRisk");
        }
    }
}
