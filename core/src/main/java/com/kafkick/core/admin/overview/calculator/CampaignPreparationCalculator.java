package com.kafkick.core.admin.overview.calculator;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.kafkick.core.admin.CouponPolicyType;
import com.kafkick.core.admin.campaignsource.PreparationItem;
import com.kafkick.core.admin.campaignsource.PreparationObservation;
import com.kafkick.core.admin.campaignsource.PreparationSource;
import com.kafkick.core.admin.preparation.V2PreparationSource;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.runtimeconfig.RuntimeConfigSnapshot;

/** DB 회차 엔진과 V2 Redis 준비 원천을 결합하는 순수 계산기입니다. */
@Component
public class CampaignPreparationCalculator {

    private static final Set<CouponPolicyType> ISSUANCE_POLICIES =
            Set.of(CouponPolicyType.PERCENT_CAPPED, CouponPolicyType.FIXED_AMOUNT);

    private static final Map<EngineVersion, Set<CouponPolicyType>> SUPPORTED_ISSUANCE_POLICIES = Map.of(
            EngineVersion.V1, ISSUANCE_POLICIES,
            EngineVersion.V2, ISSUANCE_POLICIES);

    /** 상태가 없는 순수 계산기로 생성합니다. */
    public CampaignPreparationCalculator() { }

    /**
     * DB 설정·재고·회차 엔진과 V2 Redis 준비 상태를 결합해 확정 실패 항목만 반환합니다.
     *
     * @param source DB에서 판정한 캠페인 설정·재고 준비 원천
     * @param engineVersion DB 회차에 저장된 발급 엔진 버전
     * @param v2Source V2 예약 회차의 Redis 준비 원천; V1에는 적용되지 않음
     * @return 모든 항목을 판정했을 때만 완료 여부와 확정 실패 목록을 가진 관측값
     */
    public PreparationObservation calculate(
            PreparationSource source,
            EngineVersion engineVersion,
            V2PreparationSource v2Source
    ) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(engineVersion, "engineVersion");
        Objects.requireNonNull(v2Source, "v2Source");
        if (!source.status().carriesValue()) {
            // DB 원천이 값을 주지 않았으므로 실패 목록을 임의로 만들지 않습니다.
            return new PreparationObservation(null, List.of(), source.status(), null);
        }

        List<PreparationItem> failedItems = new ArrayList<>();
        if (!source.campaignConfigurationReady()) {
            failedItems.add(PreparationItem.CAMPAIGN_CONFIGURATION);
        }
        if (!source.databaseStockReady()) {
            failedItems.add(PreparationItem.DATABASE_STOCK);
        }
        Set<CouponPolicyType> supportedPolicies = SUPPORTED_ISSUANCE_POLICIES
                .getOrDefault(engineVersion, Set.of());
        if (source.policyType() == null || !supportedPolicies.contains(source.policyType())) {
            // 실제 엔진·정책 조합이 구현된 경우에만 발급 경로를 준비 완료로 봅니다.
            failedItems.add(PreparationItem.ISSUANCE_PATH);
        }
        if (engineVersion != EngineVersion.V2 || !failedItems.isEmpty()) {
            // DB에서 이미 실패를 확정했거나 V2가 아니면 존재하지 않는 Redis 판정을 요구하지 않습니다.
            return observed(source, failedItems, source.observedAt());
        }
        if (!v2Source.status().carriesValue()) {
            // 아직 워밍업 전이거나 Redis를 읽지 못한 상태를 확정 실패 false로 바꾸지 않습니다.
            return new PreparationObservation(null, List.of(), v2Source.status(), null);
        }
        if (!v2Source.warmupReady()) {
            failedItems.add(PreparationItem.REDIS_WARMUP);
        }
        if (!v2Source.gateReady()) {
            failedItems.add(PreparationItem.REDIS_GATE);
        }
        Instant observedAt = source.observedAt().isBefore(v2Source.observedAt())
                ? source.observedAt() : v2Source.observedAt();
        return observed(source, failedItems, observedAt);
    }

    /**
     * Redis 준비 Resolver 연결 전 호출부의 기존 판정 의미를 보존합니다.
     *
     * @deprecated CY-780 호출부는 DB 회차 엔진과 실제 V2 준비 원천을 전달해야 합니다.
     */
    @Deprecated
    public PreparationObservation calculate(
            PreparationSource source,
            RuntimeConfigSnapshot runtimeConfig
    ) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(runtimeConfig, "runtimeConfig");
        if (!source.status().carriesValue()) {
            return new PreparationObservation(null, List.of(), source.status(), null);
        }
        if (!runtimeConfig.status().carriesValue()) {
            return new PreparationObservation(null, List.of(), runtimeConfig.status(), null);
        }
        // 이 호환 경로는 Redis 준비를 관측하지 않던 기존 호출 의미만 유지하며 CY-780에서 제거됩니다.
        V2PreparationSource compatibilitySource = new V2PreparationSource(
                true, true, SourceStatus.VALID, source.observedAt());
        return calculate(source, runtimeConfig.engineVersion(), compatibilitySource);
    }

    /** 확정 실패 목록과 DB 원천 상태를 값 보유 준비 관측으로 변환합니다. */
    private static PreparationObservation observed(
            PreparationSource source,
            List<PreparationItem> failedItems,
            Instant observedAt
    ) {
        return new PreparationObservation(
                failedItems.isEmpty(), failedItems, source.status(), observedAt);
    }
}
