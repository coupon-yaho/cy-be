package com.kafkick.core.admin.preparation;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.kafkick.core.admin.campaignsource.AdminCampaignCatalog;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.SourceStatus;

/** DB 캠페인 모집단에서 V2 Redis 준비 조회 대상과 결과 모집단을 확정합니다. */
public final class AdminPreparationResolver {

    private final V2AdminPreparationReader v2Reader;

    /** V2 준비 상태 조회 포트를 주입받습니다. */
    public AdminPreparationResolver(V2AdminPreparationReader v2Reader) {
        this.v2Reader = Objects.requireNonNull(v2Reader, "v2Reader");
    }

    /**
     * DB 준비가 완료된 V2 예약 회차만 Redis로 조회하고 모든 캠페인 ID의 적용 여부를 반환합니다.
     * Reader 실패나 응답 모집단 위반은 요청한 V2 회차만 UNAVAILABLE로 격리합니다.
     */
    public Map<Long, V2PreparationSource> resolve(
            AdminCampaignCatalog catalog,
            Instant observedAt
    ) {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(observedAt, "observedAt");
        if (catalog.status() != SourceStatus.VALID) {
            return Map.of();
        }

        LinkedHashMap<Long, V2PreparationSource> resolved = new LinkedHashMap<>();
        ArrayList<V2AdminPreparationReader.Request> requests = new ArrayList<>();
        for (AdminCampaignCatalog.CampaignData campaign : catalog.campaigns()) {
            resolved.put(campaign.couponId(), V2PreparationSource.notApplicable());
            if (!requiresV2Preparation(campaign)) {
                continue;
            }
            try {
                requests.add(toRequest(campaign));
            } catch (RuntimeException exception) {
                // DB 원천이 스스로 준비 완료라 했지만 요청 계약을 못 만들면 정상값으로 추측하지 않습니다.
                resolved.put(campaign.couponId(), V2PreparationSource.unavailable());
            }
        }
        if (requests.isEmpty()) {
            return immutableCopy(resolved);
        }

        Set<Long> requestedIds = requestedIds(requests);
        if (requestedIds.size() != requests.size()) {
            // Map 응답은 중복 ID를 표현할 수 없으므로 Reader를 호출하기 전에 모집단을 거부합니다.
            putUnavailable(resolved, requestedIds);
            return immutableCopy(resolved);
        }

        Map<Long, V2PreparationSource> response;
        try {
            response = v2Reader.read(List.copyOf(requests), observedAt);
        } catch (RuntimeException exception) {
            putUnavailable(resolved, requestedIds);
            return immutableCopy(resolved);
        }
        if (!hasExactPopulation(response, requestedIds)) {
            putUnavailable(resolved, requestedIds);
            return immutableCopy(resolved);
        }
        for (Long requestedId : requestedIds) {
            resolved.put(requestedId, response.get(requestedId));
        }
        return immutableCopy(resolved);
    }

    /** DB 정본 비교값을 모두 가진 V2 예약 회차인지 확인합니다. */
    private static boolean requiresV2Preparation(AdminCampaignCatalog.CampaignData campaign) {
        return campaign.engineVersion() == EngineVersion.V2
                && campaign.status() == CouponRoundStatus.SCHEDULED
                && campaign.preparation().status().carriesValue()
                && Boolean.TRUE.equals(campaign.preparation().campaignConfigurationReady())
                && Boolean.TRUE.equals(campaign.preparation().databaseStockReady())
                && campaign.stock().status().carriesValue()
                && campaign.stock().value() != null;
    }

    /** DB 캠페인 값을 Redis Reader의 비교 요청으로 변환합니다. */
    private static V2AdminPreparationReader.Request toRequest(
            AdminCampaignCatalog.CampaignData campaign
    ) {
        return new V2AdminPreparationReader.Request(
                campaign.couponId(),
                campaign.status(),
                campaign.opensAt(),
                campaign.closesAt(),
                campaign.preparation().eligibleGradesMask(),
                campaign.stock().value().totalQuantity(),
                campaign.stock().value().totalQuantity() - campaign.stock().value().activeCount());
    }

    /** 요청 순서를 보존한 ID 집합을 생성합니다. */
    private static Set<Long> requestedIds(List<V2AdminPreparationReader.Request> requests) {
        LinkedHashSet<Long> ids = new LinkedHashSet<>();
        for (V2AdminPreparationReader.Request request : requests) {
            ids.add(request.couponId());
        }
        return Collections.unmodifiableSet(ids);
    }

    /** Reader 응답이 null 값 없이 요청 ID 집합과 정확히 일치하는지 확인합니다. */
    private static boolean hasExactPopulation(
            Map<Long, V2PreparationSource> response,
            Set<Long> requestedIds
    ) {
        return response != null
                && response.keySet().equals(requestedIds)
                && response.values().stream().allMatch(Objects::nonNull);
    }

    /** 지정한 V2 요청 ID만 조회 불가 상태로 교체합니다. */
    private static void putUnavailable(
            LinkedHashMap<Long, V2PreparationSource> resolved,
            Set<Long> requestedIds
    ) {
        for (Long requestedId : requestedIds) {
            resolved.put(requestedId, V2PreparationSource.unavailable());
        }
    }

    /** 캠페인 순서를 유지하면서 결과 Map을 불변으로 반환합니다. */
    private static Map<Long, V2PreparationSource> immutableCopy(
            LinkedHashMap<Long, V2PreparationSource> source
    ) {
        return Collections.unmodifiableMap(new LinkedHashMap<>(source));
    }

    /** Redis 준비 Reader Bean이 없는 실행 환경에서 V2 요청만 조회 불가로 만드는 포트입니다. */
    public static V2AdminPreparationReader unavailableV2Reader() {
        return (requests, observedAt) -> {
            Objects.requireNonNull(requests, "requests");
            Objects.requireNonNull(observedAt, "observedAt");
            LinkedHashMap<Long, V2PreparationSource> result = new LinkedHashMap<>();
            for (V2AdminPreparationReader.Request request : List.copyOf(requests)) {
                V2PreparationSource previous = result.put(
                        request.couponId(), V2PreparationSource.unavailable());
                if (previous != null) {
                    throw new IllegalArgumentException(
                            "중복된 V2 준비 요청입니다: " + request.couponId());
                }
            }
            return immutableCopy(result);
        };
    }
}
