package com.kafkick.core.admin.overview.calculator;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.kafkick.core.admin.CouponPolicyType;
import com.kafkick.core.admin.campaignsource.PreparationItem;
import com.kafkick.core.admin.campaignsource.PreparationObservation;
import com.kafkick.core.admin.campaignsource.PreparationSource;
import com.kafkick.core.admin.preparation.V2PreparationSource;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.SourceStatus;

/** DB 회차 엔진과 V2 Redis 원천을 결합하는 캠페인 준비 계산 규칙을 검증합니다. */
class CampaignPreparationCalculatorTest {

    private static final Instant DB_AT = Instant.parse("2026-08-27T00:00:00Z");
    private static final Instant REDIS_AT = Instant.parse("2026-08-26T23:59:59Z");

    private final CampaignPreparationCalculator calculator = new CampaignPreparationCalculator();

    /** DB 원천 값이 없으면 Redis 값으로 완료 여부를 새로 만들지 않는지 검증합니다. */
    @Test
    @DisplayName("DB 준비 원천이 PENDING이면 실패 목록 없이 PENDING을 보존한다")
    void preservesPendingDatabaseSourceWithoutFailedItems() {
        PreparationObservation result = calculator.calculate(
                new PreparationSource(null, null, null, null, SourceStatus.PENDING, null),
                EngineVersion.V2,
                validV2(true, true));

        assertThat(result).isEqualTo(new PreparationObservation(
                null, List.of(), SourceStatus.PENDING, null));
    }

    /** DB에서 확정한 설정·재고 실패가 Redis 미판정 상태로 덮이지 않는지 검증합니다. */
    @Test
    @DisplayName("V2 DB 설정·재고 실패는 Redis를 판정하지 못해도 확정 실패로 보존한다")
    void preservesConfirmedDatabaseFailuresForV2() {
        PreparationObservation result = calculator.calculate(
                database(false, false, CouponPolicyType.FIXED_AMOUNT),
                EngineVersion.V2,
                V2PreparationSource.notApplicable());

        assertThat(result).isEqualTo(new PreparationObservation(
                false,
                List.of(
                        PreparationItem.CAMPAIGN_CONFIGURATION,
                        PreparationItem.DATABASE_STOCK),
                SourceStatus.VALID,
                DB_AT));
    }

    /** V2 두 Redis 축의 false가 서로 사라지지 않고 확정 실패 목록으로 합쳐지는지 검증합니다. */
    @Test
    @DisplayName("V2 Redis 워밍업·게이트 실패를 각각 확정 실패 항목으로 합친다")
    void mergesV2RedisFailures() {
        PreparationObservation result = calculator.calculate(
                readyDatabase(CouponPolicyType.FIXED_AMOUNT),
                EngineVersion.V2,
                validV2(false, false));

        assertThat(result.completed()).isFalse();
        assertThat(result.failedItems()).containsExactly(
                PreparationItem.REDIS_WARMUP, PreparationItem.REDIS_GATE);
        assertThat(result.status()).isEqualTo(SourceStatus.VALID);
        assertThat(result.observedAt()).isEqualTo(REDIS_AT);
    }

    /** V1 회차가 존재하지 않는 V2 원천의 장애에 종속되는 회귀를 방지합니다. */
    @Test
    @DisplayName("V1은 V2 준비 원천 장애와 무관하게 DB와 정책으로 판정한다")
    void v1IgnoresV2PreparationSource() {
        PreparationObservation result = calculator.calculate(
                readyDatabase(CouponPolicyType.FIXED_AMOUNT),
                EngineVersion.V1,
                V2PreparationSource.unavailable());

        assertThat(result).isEqualTo(new PreparationObservation(
                true, List.of(), SourceStatus.VALID, DB_AT));
    }

    /** 워밍업 전 상태를 확정 실패로 바꿔 거짓 경고를 만드는 것을 방지합니다. */
    @Test
    @DisplayName("DB 준비가 끝난 V2의 Redis PENDING은 false로 바뀌지 않는다")
    void preservesPendingV2Readiness() {
        PreparationObservation result = calculator.calculate(
                readyDatabase(CouponPolicyType.FIXED_AMOUNT),
                EngineVersion.V2,
                new V2PreparationSource(null, null, SourceStatus.PENDING, null));

        assertThat(result).isEqualTo(new PreparationObservation(
                null, List.of(), SourceStatus.PENDING, null));
    }

    /** Redis 통신 장애를 준비 실패로 확정하지 않고 조회 불가로 보존하는지 검증합니다. */
    @Test
    @DisplayName("DB 준비가 끝난 V2의 Redis UNAVAILABLE은 미판정으로 보존한다")
    void preservesUnavailableV2Readiness() {
        PreparationObservation result = calculator.calculate(
                readyDatabase(CouponPolicyType.FIXED_AMOUNT),
                EngineVersion.V2,
                V2PreparationSource.unavailable());

        assertThat(result).isEqualTo(new PreparationObservation(
                null, List.of(), SourceStatus.UNAVAILABLE, null));
    }

    /** 구현된 할인 정책 두 종류가 엔진별 발급 경로 실패로 오인되지 않는지 검증합니다. */
    @ParameterizedTest
    @EnumSource(value = CouponPolicyType.class, names = { "PERCENT_CAPPED", "FIXED_AMOUNT" })
    @DisplayName("V1과 V2는 구현된 할인 정책의 발급 경로를 준비 완료로 판정한다")
    void completesSupportedIssuancePolicies(CouponPolicyType policyType) {
        PreparationObservation v1 = calculator.calculate(
                readyDatabase(policyType), EngineVersion.V1, V2PreparationSource.notApplicable());
        PreparationObservation v2 = calculator.calculate(
                readyDatabase(policyType), EngineVersion.V2, validV2(true, true));

        assertThat(v1.completed()).isTrue();
        assertThat(v1.failedItems()).isEmpty();
        assertThat(v2.completed()).isTrue();
        assertThat(v2.failedItems()).isEmpty();
    }

    /** DB에 저장됐지만 발급 경로가 구현되지 않은 정책을 엔진별 실패로 노출하는지 검증합니다. */
    @ParameterizedTest
    @EnumSource(value = EngineVersion.class, names = { "V1", "V2" })
    @DisplayName("DATA_GRANT 정책은 V1과 V2 발급 경로 실패다")
    void dataGrantPolicyFailsIssuancePath(EngineVersion engineVersion) {
        PreparationObservation result = calculator.calculate(
                readyDatabase(CouponPolicyType.DATA_GRANT),
                engineVersion,
                engineVersion == EngineVersion.V2
                        ? validV2(true, true) : V2PreparationSource.notApplicable());

        assertThat(result.failedItems()).containsExactly(PreparationItem.ISSUANCE_PATH);
        assertThat(result.completed()).isFalse();
    }

    /** 알 수 없는 정책은 DB 설정 실패와 발급 경로 실패를 함께 보존하는지 검증합니다. */
    @Test
    @DisplayName("알 수 없는 정책은 캠페인 설정과 발급 경로 실패다")
    void unknownPolicyFailsConfigurationAndIssuancePath() {
        PreparationObservation result = calculator.calculate(
                database(false, true, null),
                EngineVersion.V1,
                V2PreparationSource.notApplicable());

        assertThat(result).isEqualTo(new PreparationObservation(
                false,
                List.of(PreparationItem.CAMPAIGN_CONFIGURATION, PreparationItem.ISSUANCE_PATH),
                SourceStatus.VALID,
                DB_AT));
    }

    /** 두 원천 중 더 오래된 시각을 최종 판정의 보수적인 관측 시각으로 사용하는지 검증합니다. */
    @Test
    @DisplayName("V2 준비 완료 시 DB와 Redis 중 이른 관측 시각을 사용한다")
    void usesEarlierObservationTimeForV2() {
        PreparationObservation result = calculator.calculate(
                readyDatabase(CouponPolicyType.FIXED_AMOUNT),
                EngineVersion.V2,
                validV2(true, true));

        assertThat(result.observedAt()).isEqualTo(REDIS_AT);
    }

    /** 설정·재고가 모두 준비된 DB 원천을 생성합니다. */
    private static PreparationSource readyDatabase(CouponPolicyType policyType) {
        return database(true, true, policyType);
    }

    /** 지정한 DB 판정과 정책을 값 보유 원천으로 생성합니다. */
    private static PreparationSource database(
            boolean configurationReady,
            boolean databaseStockReady,
            CouponPolicyType policyType
    ) {
        return new PreparationSource(
                configurationReady, databaseStockReady, policyType, 3, SourceStatus.VALID, DB_AT);
    }

    /** 지정한 두 Redis 판정을 값 보유 V2 원천으로 생성합니다. */
    private static V2PreparationSource validV2(boolean warmupReady, boolean gateReady) {
        return new V2PreparationSource(
                warmupReady, gateReady, SourceStatus.VALID, REDIS_AT);
    }
}
