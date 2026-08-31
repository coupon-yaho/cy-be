package com.kafkick.core.admin.stock;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.kafkick.core.admin.couponroundsource.AdminCouponRoundCatalog;
import com.kafkick.core.admin.couponroundsource.AdminCouponRoundDetailData;
import com.kafkick.core.admin.couponroundsource.DetailAvailability;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSource;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.SourceStatus;

/** 회차 엔진에 따라 DB 또는 Redis를 선택해 관리자 권위 재고를 확정합니다. */
public final class AdminStockResolver {
    private final V2AdminStockReader v2Reader;

    /** V2 재고 포트를 주입받아 버전별 재고 원천 선택기를 만듭니다. */
    public AdminStockResolver(V2AdminStockReader v2Reader) {
        this.v2Reader = Objects.requireNonNull(v2Reader, "v2Reader");
    }

    /** 목록의 V1 DB 재고는 보존하고 V2 DB 미러는 Redis 정본 관측으로 교체합니다. */
    public AdminCouponRoundCatalog resolve(AdminCouponRoundCatalog catalog, Instant observedAt) {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(observedAt, "observedAt");
        if (catalog.status() != SourceStatus.VALID) {
            return catalog;
        }
        List<V2AdminStockReader.Request> requests = catalog.couponRounds().stream()
                .filter(couponRound -> couponRound.engineVersion() == EngineVersion.V2)
                .filter(couponRound -> couponRound.stock().status().carriesValue())
                .map(couponRound -> new V2AdminStockReader.Request(
                        couponRound.couponId(), couponRound.status(), couponRound.stock().value().totalQuantity()))
                .toList();
        Map<Long, CouponMetricsSource.Observation<AdminStockSnapshot>> redis =
                requests.isEmpty() ? Map.of() : v2Reader.read(requests, observedAt);
        ArrayList<AdminCouponRoundCatalog.CouponRoundData> resolved = new ArrayList<>();
        for (AdminCouponRoundCatalog.CouponRoundData couponRound : catalog.couponRounds()) {
            CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> stock = couponRound.stock();
            if (couponRound.engineVersion() == EngineVersion.V2) {
                // V2에서 DB active_count는 미러일 뿐이므로 Redis 실패를 DB 값으로 숨기지 않습니다.
                stock = toLegacyCounts(redis.get(couponRound.couponId()));
            }
            resolved.add(new AdminCouponRoundCatalog.CouponRoundData(
                    couponRound.couponId(), couponRound.couponName(), couponRound.brandName(), couponRound.engineVersion(),
                    couponRound.status(), couponRound.opensAt(), couponRound.closesAt(), stock, couponRound.preparation()));
        }
        return new AdminCouponRoundCatalog(catalog.status(), catalog.observedAt(), resolved);
    }

    /** 상세의 V1 DB 재고는 보존하고 V2 DB 미러만 Redis 정본으로 교체합니다. */
    public AdminCouponRoundDetailData resolve(AdminCouponRoundDetailData detail, Instant observedAt) {
        Objects.requireNonNull(detail, "detail");
        Objects.requireNonNull(observedAt, "observedAt");
        if (detail.availability() != DetailAvailability.AVAILABLE
                || detail.value().engineVersion() != EngineVersion.V2) {
            return detail;
        }
        AdminCouponRoundDetailData.DetailValue value = detail.value();
        CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> stock;
        if (!value.stock().status().carriesValue()) {
            stock = new CouponMetricsSource.Observation<>(null, SourceStatus.UNAVAILABLE, null);
        } else {
            V2AdminStockReader.Request request = new V2AdminStockReader.Request(
                    value.couponId(), value.couponRound().status(), value.stock().value().totalQuantity());
            stock = toLegacyCounts(v2Reader.read(List.of(request), observedAt).get(value.couponId()));
        }
        return new AdminCouponRoundDetailData(DetailAvailability.AVAILABLE,
                new AdminCouponRoundDetailData.DetailValue(
                        value.couponId(), value.couponName(), value.brandName(), value.engineVersion(),
                        value.couponRound(), stock, value.holdingCounts(), value.transitions()));
    }

    /** 기존 계산기 입력의 activeCount 자리에 권위 재고로부터 도출한 발급 수량을 싣습니다. */
    private static CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> toLegacyCounts(
            CouponMetricsSource.Observation<AdminStockSnapshot> observation) {
        if (observation == null || !observation.status().carriesValue()) {
            SourceStatus status = observation == null ? SourceStatus.UNAVAILABLE : observation.status();
            return new CouponMetricsSource.Observation<>(null, status, null);
        }
        AdminStockSnapshot value = observation.value();
        return new CouponMetricsSource.Observation<>(
                new CouponMetricsSource.StockCounts(value.totalQuantity(), value.issuedQuantity()),
                observation.status(), observation.observedAt());
    }

    /** Redis 구현이 없는 실행 환경에서 V2만 명시적으로 조회 불가로 만드는 포트입니다. */
    public static V2AdminStockReader unavailableV2Reader() {
        return (requests, observedAt) -> {
            LinkedHashMap<Long, CouponMetricsSource.Observation<AdminStockSnapshot>> result =
                    new LinkedHashMap<>();
            for (V2AdminStockReader.Request request : requests) {
                result.put(request.couponId(),
                        new CouponMetricsSource.Observation<>(null, SourceStatus.UNAVAILABLE, null));
            }
            return Map.copyOf(result);
        };
    }
}
