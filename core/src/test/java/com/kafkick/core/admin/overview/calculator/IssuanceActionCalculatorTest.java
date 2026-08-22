package com.kafkick.core.admin.overview.calculator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;

/** O1 발급 중단 조치 후보가 최신 정상 관측에서만 만들어지는지 검증합니다. */
class IssuanceActionCalculatorTest {

    private static final Instant OBSERVED_AT = Instant.parse("2026-08-22T03:00:00Z");

    /** 발급 중단의 연속 시간은 조치 최초 감지 시각과 지속 시간으로 그대로 보존돼야 합니다. */
    @Test
    @DisplayName("VALID STOPPED O1은 stateDuration으로 계산한 발급 중단 조치 후보를 만든다")
    void createsStoppedIssuanceActionWithDetectedAtFromStateDuration() {
        IssuanceActionCalculator calculator = new IssuanceActionCalculator();

        List<AdminOverviewSnapshot.OperationActionItem> result = calculator.calculate(Map.of(
                17L, observation(SourceStatus.VALID, AdminOverviewSnapshot.IssuanceFlowState.STOPPED,
                        Duration.ofMinutes(12))));

        assertThat(result).containsExactly(new AdminOverviewSnapshot.OperationActionItem(
                17L, null, null, Severity.CRITICAL,
                AdminOverviewSnapshot.CustomerImpact.LIMITED,
                "쿠폰 발급이 중단되었습니다.", OBSERVED_AT.minus(Duration.ofMinutes(12)),
                Duration.ofMinutes(12), new AdminOverviewSnapshot.RecommendedAction(
                        AdminOverviewSnapshot.ActionCode.ISSUANCE_STOPPED,
                        "발급 흐름 확인", AdminOverviewSnapshot.TargetScreen.ISSUANCE_INQUIRY)));
    }

    /** 최초 중단 시각을 알 수 없는 관측을 임의의 현재 시각으로 바꾸는 회귀를 막습니다. */
    @Test
    @DisplayName("VALID STOPPED O1의 stateDuration이 없으면 감지 시각과 지속 시간도 null이다")
    void preservesUnknownDetectedAtWhenStoppedDurationIsUnknown() {
        List<AdminOverviewSnapshot.OperationActionItem> result = new IssuanceActionCalculator().calculate(Map.of(
                17L, observation(SourceStatus.VALID, AdminOverviewSnapshot.IssuanceFlowState.STOPPED, null)));

        assertThat(result).singleElement().satisfies(action -> {
            assertThat(action.detectedAt()).isNull();
            assertThat(action.duration()).isNull();
        });
    }

    /** 0 duration은 현재 관측 시각을 감지 시각으로 유지하고 음수 duration은 모델 경계에서 거부해야 합니다. */
    @Test
    @DisplayName("O1 stateDuration은 0 또는 null을 허용하고 음수는 거부한다")
    void validatesIssuanceStateDurationAtTheModelBoundary() {
        List<AdminOverviewSnapshot.OperationActionItem> result = new IssuanceActionCalculator().calculate(Map.of(
                17L, observation(SourceStatus.VALID, AdminOverviewSnapshot.IssuanceFlowState.STOPPED,
                        Duration.ZERO)));

        assertThat(result).singleElement().satisfies(action -> {
            assertThat(action.detectedAt()).isEqualTo(OBSERVED_AT);
            assertThat(action.duration()).isZero();
        });
        assertThatThrownBy(() -> observation(SourceStatus.VALID,
                AdminOverviewSnapshot.IssuanceFlowState.STOPPED, Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 감소·정상과 오래됐거나 값 없는 O1 원천이 발급 중단 조치로 잘못 승격되는 회귀를 막습니다. */
    @Test
    @DisplayName("STOPPED 외 상태와 VALID 외 원천 상태는 발급 중단 조치를 만들지 않는다")
    void excludesNonStoppedAndNonValidIssuanceObservations() {
        IssuanceActionCalculator calculator = new IssuanceActionCalculator();

        for (AdminOverviewSnapshot.IssuanceFlowState state : List.of(
                AdminOverviewSnapshot.IssuanceFlowState.NORMAL,
                AdminOverviewSnapshot.IssuanceFlowState.DECREASING)) {
            assertThat(calculator.calculate(Map.of(17L, observation(SourceStatus.VALID, state,
                    Duration.ofMinutes(12))))).isEmpty();
        }
        for (SourceStatus status : List.of(SourceStatus.NO_TRAFFIC, SourceStatus.STALE,
                SourceStatus.WARMING_UP, SourceStatus.PENDING, SourceStatus.UNAVAILABLE, SourceStatus.N_A)) {
            assertThat(calculator.calculate(Map.of(17L, observation(status,
                    AdminOverviewSnapshot.IssuanceFlowState.STOPPED, Duration.ofMinutes(12))))).isEmpty();
        }
    }

    /** couponId별 한 O1 관측이 Action 집계에 중복 후보를 공급하지 않아야 합니다. */
    @Test
    @DisplayName("각 couponId의 STOPPED O1은 조치 후보를 한 건만 만든다")
    void createsAtMostOneActionPerCoupon() {
        List<AdminOverviewSnapshot.OperationActionItem> result = new IssuanceActionCalculator().calculate(Map.of(
                17L, observation(SourceStatus.VALID, AdminOverviewSnapshot.IssuanceFlowState.STOPPED,
                        Duration.ofMinutes(12)),
                18L, observation(SourceStatus.VALID, AdminOverviewSnapshot.IssuanceFlowState.STOPPED,
                        Duration.ofMinutes(6))));

        assertThat(result).extracting(AdminOverviewSnapshot.OperationActionItem::couponId)
                .containsExactlyInAnyOrder(17L, 18L);
    }

    /** O1 Observation의 상태별 값 보유 계약을 지키는 고정 입력을 만듭니다. */
    private static AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow> observation(
            SourceStatus status,
            AdminOverviewSnapshot.IssuanceFlowState state,
            Duration duration
    ) {
        if (!status.carriesValue()) {
            return new AdminOverviewSnapshot.Observation<>(null, status, null);
        }
        return new AdminOverviewSnapshot.Observation<>(new AdminOverviewSnapshot.IssuanceFlow(
                0.0, OBSERVED_AT.minus(Duration.ofMinutes(1)), OBSERVED_AT, List.of(), state, duration),
                status, OBSERVED_AT);
    }
}
