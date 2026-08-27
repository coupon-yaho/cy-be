package com.kafkick.core.admin.overview.calculator;

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
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.runtimeconfig.RuntimeConfigSnapshot;

/** DB 준비 원천과 요청당 한 번 읽은 Runtime 설정을 결합하는 순수 계산기입니다. */
@Component
public class CampaignPreparationCalculator {

    private static final Map<EngineVersion, Set<CouponPolicyType>> SUPPORTED_ISSUANCE_POLICIES = Map.of(
            EngineVersion.V1,
            Set.of(CouponPolicyType.PERCENT_CAPPED, CouponPolicyType.FIXED_AMOUNT));

    /** 상태가 없는 순수 계산기로 생성합니다. */
    public CampaignPreparationCalculator() { }

    /**
     * DB 설정·재고와 실제 발급 설정을 결합해 확정 실패 항목만 반환합니다.
     *
     * @param source DB에서 판정한 캠페인 설정·재고 준비 원천
     * @param runtimeConfig 같은 Overview 요청에서 한 번 읽은 Runtime 설정
     * @return 모든 항목을 판정했을 때만 완료 여부와 확정 실패 목록을 가진 관측값
     */
    public PreparationObservation calculate(
            PreparationSource source,
            RuntimeConfigSnapshot runtimeConfig
    ) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(runtimeConfig, "runtimeConfig");
        if (!source.status().carriesValue()) {
            // DB 원천이 값을 주지 않았으므로 실패 목록을 임의로 만들지 않습니다.
            return new PreparationObservation(null, List.of(), source.status(), null);
        }
        if (!runtimeConfig.status().carriesValue()) {
            // 엔진 설정을 확인하지 못하면 나머지 DB 판정도 최종 완료 여부로 확정하지 않습니다.
            return new PreparationObservation(null, List.of(), runtimeConfig.status(), null);
        }

        List<PreparationItem> failedItems = new ArrayList<>();
        if (!source.campaignConfigurationReady()) {
            failedItems.add(PreparationItem.CAMPAIGN_CONFIGURATION);
        }
        if (!source.databaseStockReady()) {
            failedItems.add(PreparationItem.DATABASE_STOCK);
        }
        Set<CouponPolicyType> supportedPolicies = SUPPORTED_ISSUANCE_POLICIES
                .getOrDefault(runtimeConfig.engineVersion(), Set.of());
        if (source.policyType() == null || !supportedPolicies.contains(source.policyType())) {
            // 실제 엔진·정책 조합이 구현된 경우에만 발급 경로를 준비 완료로 봅니다.
            failedItems.add(PreparationItem.ISSUANCE_PATH);
        }
        return new PreparationObservation(
                failedItems.isEmpty(), failedItems, source.status(), source.observedAt());
    }
}
