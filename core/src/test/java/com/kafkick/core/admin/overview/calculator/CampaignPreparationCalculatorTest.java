package com.kafkick.core.admin.overview.calculator;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.campaignsource.PreparationItem;
import com.kafkick.core.admin.campaignsource.PreparationObservation;
import com.kafkick.core.admin.campaignsource.PreparationSource;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.runtimeconfig.RuntimeConfigSnapshot;

/** DB 준비 원천과 Runtime 설정을 결합하는 캠페인 준비 계산 규칙을 검증합니다. */
class CampaignPreparationCalculatorTest {

    private static final Instant OBSERVED_AT = Instant.parse("2026-08-27T00:00:00Z");

    private final CampaignPreparationCalculator calculator = new CampaignPreparationCalculator();

    /** DB 원천 값이 없으면 확정 실패 목록을 만들지 않는지 검증합니다. */
    @Test
    @DisplayName("DB 준비 원천이 PENDING이면 실패 목록 없이 PENDING을 보존한다")
    void preservesPendingSourceWithoutFailedItems() {
        PreparationObservation result = calculator.calculate(
                new PreparationSource(null, null, SourceStatus.PENDING, null), validRuntime(EngineVersion.V1));

        assertThat(result).isEqualTo(new PreparationObservation(
                null, List.of(), SourceStatus.PENDING, null));
    }

    /** Runtime 설정을 읽지 못한 경우 DB 실패를 최종 실패로 확정하지 않는지 검증합니다. */
    @Test
    @DisplayName("Runtime 설정이 PENDING이면 DB 판정과 무관하게 실패 목록을 비운다")
    void preservesUnknownWhenRuntimeConfigIsPending() {
        PreparationObservation result = calculator.calculate(
                new PreparationSource(false, false, SourceStatus.VALID, OBSERVED_AT),
                runtime(EngineVersion.V1, SourceStatus.PENDING));

        assertThat(result).isEqualTo(new PreparationObservation(
                null, List.of(), SourceStatus.PENDING, null));
    }

    /** DB 두 항목과 지원되지 않는 발급 경로를 각각 구분해 노출하는지 검증합니다. */
    @Test
    @DisplayName("DB 설정·재고와 V2 발급 경로의 확정 실패 항목을 분리한다")
    void listsConfirmedFailuresSeparately() {
        PreparationObservation result = calculator.calculate(
                new PreparationSource(false, false, SourceStatus.VALID, OBSERVED_AT), validRuntime(EngineVersion.V2));

        assertThat(result).isEqualTo(new PreparationObservation(
                false,
                List.of(
                        PreparationItem.CAMPAIGN_CONFIGURATION,
                        PreparationItem.DATABASE_STOCK,
                        PreparationItem.ISSUANCE_PATH),
                SourceStatus.VALID,
                OBSERVED_AT));
    }

    /** 현재 실제 발급 구현이 V1일 때만 네 항목 전체를 준비 완료로 보는지 검증합니다. */
    @Test
    @DisplayName("정상 DB 원천과 V1 설정은 준비 완료이며 실패 목록이 비어 있다")
    void completesOnlyWithSupportedV1Path() {
        PreparationObservation result = calculator.calculate(
                new PreparationSource(true, true, SourceStatus.VALID, OBSERVED_AT), validRuntime(EngineVersion.V1));

        assertThat(result).isEqualTo(new PreparationObservation(
                true, List.of(), SourceStatus.VALID, OBSERVED_AT));
    }

    /** 값 보유 여부만 다른 Runtime Snapshot을 같은 형식으로 생성합니다. */
    private static RuntimeConfigSnapshot validRuntime(EngineVersion engineVersion) {
        return runtime(engineVersion, SourceStatus.VALID);
    }

    /** 계산기에 전달할 Runtime 설정 상태를 명시적으로 생성합니다. */
    private static RuntimeConfigSnapshot runtime(EngineVersion engineVersion, SourceStatus status) {
        return new RuntimeConfigSnapshot(
                engineVersion, ReleaseStage.V1, QueueMode.OFF, 1L, OBSERVED_AT, "test", status);
    }
}
