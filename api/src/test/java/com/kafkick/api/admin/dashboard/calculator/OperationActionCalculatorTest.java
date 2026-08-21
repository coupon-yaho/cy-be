package com.kafkick.api.admin.dashboard.calculator;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.observation.Severity;

/** 조치 후보 집계가 Service 흐름과 분리된 순수 계산 경계에서 유지되는지 검증합니다. */
class OperationActionCalculatorTest {

    private static final Instant NOW = Instant.parse("2026-08-21T03:00:00Z");

    /** 동일 캠페인의 여러 후보가 KPI를 중복 증가시키는 회귀를 방지합니다. */
    @Test
    @DisplayName("동일 캠페인의 조치 후보는 최고 심각도 한 건으로 집계한다")
    void selectsHighestSeverityPerCampaign() {
        OperationActionCalculator calculator = new OperationActionCalculator();
        List<AdminOverviewSnapshot.OperationActionItem> decisions = List.of(
                action(17L, Severity.WARN),
                action(17L, Severity.CRITICAL),
                action(18L, Severity.WARN),
                action(19L, Severity.NONE)
        );

        OperationActionCalculator.ActionCalculation result = calculator.calculate(decisions);

        assertThat(result.required().totalCount()).isEqualTo(2);
        assertThat(result.required().urgentCount()).isEqualTo(1);
        assertThat(result.required().warningCount()).isEqualTo(1);
        assertThat(result.items().topItems())
                .extracting(AdminOverviewSnapshot.OperationActionItem::couponId)
                .containsExactly(17L, 18L);
    }

    /** 화면 노출 제한이 전체 조치 규모까지 잘라내는 회귀를 방지합니다. */
    @Test
    @DisplayName("조치 대상이 20건을 넘어도 KPI는 유지하고 목록만 상위 20건으로 제한한다")
    void limitsOnlyDisplayedItemsToTwenty() {
        List<AdminOverviewSnapshot.OperationActionItem> decisions = IntStream.rangeClosed(1, 21)
                .mapToObj(couponId -> action(
                        couponId,
                        couponId == 21 ? Severity.CRITICAL : Severity.WARN,
                        NOW.plusSeconds(couponId)))
                .toList();
        OperationActionCalculator calculator = new OperationActionCalculator();

        OperationActionCalculator.ActionCalculation result = calculator.calculate(decisions);

        assertThat(result.required().totalCount()).isEqualTo(21);
        assertThat(result.required().urgentCount()).isEqualTo(1);
        assertThat(result.required().warningCount()).isEqualTo(20);
        assertThat(result.items().totalCount()).isEqualTo(21);
        assertThat(result.items().topItems()).hasSize(20);
        assertThat(result.items().topItems().getFirst().couponId()).isEqualTo(21L);
    }

    /** 기존 비교 키가 같은 후보의 입력 순서에 따라 화면 내용이 달라지는 회귀를 방지합니다. */
    @Test
    @DisplayName("동일 우선순위 후보는 입력 순서와 무관하게 고객 영향이 큰 판정을 선택한다")
    void selectsSameRepresentativeRegardlessOfInputOrder() {
        OperationActionCalculator calculator = new OperationActionCalculator();
        AdminOverviewSnapshot.OperationActionItem limited = action(
                17L,
                Severity.WARN,
                NOW,
                AdminOverviewSnapshot.CustomerImpact.LIMITED,
                "일부 고객에게 영향이 있습니다.");
        AdminOverviewSnapshot.OperationActionItem widespread = action(
                17L,
                Severity.WARN,
                NOW,
                AdminOverviewSnapshot.CustomerImpact.WIDESPREAD,
                "전체 고객에게 영향이 있습니다.");

        OperationActionCalculator.ActionCalculation forward =
                calculator.calculate(List.of(limited, widespread));
        OperationActionCalculator.ActionCalculation reversed =
                calculator.calculate(List.of(widespread, limited));

        assertThat(forward.items().topItems()).containsExactly(widespread);
        assertThat(reversed.items().topItems()).containsExactly(widespread);
    }

    /** 테스트에 필요한 캠페인 식별자와 심각도만 달리해 실제 조치 후보를 생성합니다. */
    private static AdminOverviewSnapshot.OperationActionItem action(
            long couponId,
            Severity severity
    ) {
        return action(couponId, severity, NOW);
    }

    /** 감지 시각 정렬을 검증할 수 있도록 실제 조치 후보를 생성합니다. */
    private static AdminOverviewSnapshot.OperationActionItem action(
            long couponId,
            Severity severity,
            Instant detectedAt
    ) {
        return action(
                couponId,
                severity,
                detectedAt,
                AdminOverviewSnapshot.CustomerImpact.NONE,
                "테스트 고객 영향");
    }

    /** 동률 후보의 고객 영향과 표시 내용까지 달리해 대표 선택의 결정성을 검증합니다. */
    private static AdminOverviewSnapshot.OperationActionItem action(
            long couponId,
            Severity severity,
            Instant detectedAt,
            AdminOverviewSnapshot.CustomerImpact customerImpact,
            String customerImpactText
    ) {
        return new AdminOverviewSnapshot.OperationActionItem(
                couponId,
                "캠페인 " + couponId,
                NOW,
                severity,
                customerImpact,
                customerImpactText,
                detectedAt,
                null,
                new AdminOverviewSnapshot.RecommendedAction(
                        AdminOverviewSnapshot.ActionCode.DATA_UNAVAILABLE,
                        "운영 상태 확인",
                        AdminOverviewSnapshot.TargetScreen.OVERVIEW)
        );
    }
}
