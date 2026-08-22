package com.kafkick.core.admin.overview.calculator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.observation.SourceStatus;

/** O3 이미 분류된 결과의 합산·고정 순서·0건 표현 규칙을 검증합니다. */
class CustomerOutcomeCalculatorTest {

    private static final Instant END = Instant.parse("2026-08-22T03:00:00Z");

    /** 같은 유형은 먼저 합산하고, 결과 7종은 입력 순서와 무관하게 고정 순서로 반환해야 합니다. */
    @Test
    void aggregatesDuplicatesAndReturnsAllTypesInFixedOrder() {
        CustomerOutcomeCalculator.OutcomeCalculation result = new CustomerOutcomeCalculator().calculate(
                new CustomerOutcomeCalculator.OutcomeInput(
                        END.minus(Duration.ofMinutes(5)), END,
                        List.of(new CustomerOutcomeCalculator.OutcomeCount(
                                        AdminOverviewSnapshot.CustomerOutcomeType.SYSTEM_FAILURE, 1L),
                                new CustomerOutcomeCalculator.OutcomeCount(
                                        AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 2L),
                                new CustomerOutcomeCalculator.OutcomeCount(
                                        AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 3L)),
                        SourceStatus.VALID, END));

        AdminOverviewSnapshot.CustomerOutcomeSummary summary = result.customerOutcomes().value();
        assertThat(summary.totalCount()).isEqualTo(6L);
        assertThat(summary.outcomes()).extracting(AdminOverviewSnapshot.CustomerOutcome::type)
                .containsExactly(AdminOverviewSnapshot.CustomerOutcomeType.values());
        assertThat(summary.outcomes().getFirst()).isEqualTo(new AdminOverviewSnapshot.CustomerOutcome(
                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 5L, 0.8333333333333334, null));
    }

    /** 정상 관측에서 실제 결과가 0건이면 빈 목록과 NO_TRAFFIC을 반환합니다. */
    @Test
    void returnsNoTrafficWithEmptyOutcomesForZeroTotal() {
        CustomerOutcomeCalculator.OutcomeCalculation result = new CustomerOutcomeCalculator().calculate(
                new CustomerOutcomeCalculator.OutcomeInput(
                        END.minus(Duration.ofMinutes(5)), END, List.of(), SourceStatus.VALID, END));

        assertThat(result.customerOutcomes().status()).isEqualTo(SourceStatus.NO_TRAFFIC);
        assertThat(result.customerOutcomes().value().totalCount()).isZero();
        assertThat(result.customerOutcomes().value().outcomes()).isEmpty();
    }

    /** 음수 count와 역전된 관측 구간을 묵시적으로 보정하지 않아야 합니다. */
    @Test
    void rejectsNegativeCountAndInvalidWindow() {
        CustomerOutcomeCalculator calculator = new CustomerOutcomeCalculator();
        assertThatThrownBy(() -> calculator.calculate(new CustomerOutcomeCalculator.OutcomeInput(
                END, END.minusSeconds(1), List.of(), SourceStatus.VALID, END)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> calculator.calculate(new CustomerOutcomeCalculator.OutcomeInput(
                END.minusSeconds(1), END,
                List.of(new CustomerOutcomeCalculator.OutcomeCount(
                        AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, -1L)), SourceStatus.VALID, END)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 0건은 VALID 원천에서만 NO_TRAFFIC으로 변환하고 STALE·WARMING_UP 상태는 보존합니다. */
    @Test
    void preservesCarryingSourceStatusForZeroCountsAndAllowsNonCarryingWithoutRawValues() {
        CustomerOutcomeCalculator calculator = new CustomerOutcomeCalculator();
        CustomerOutcomeCalculator.OutcomeCalculation stale = calculator.calculate(
                new CustomerOutcomeCalculator.OutcomeInput(
                        END.minus(Duration.ofMinutes(1)), END, List.of(), SourceStatus.STALE, END));
        CustomerOutcomeCalculator.OutcomeCalculation unavailable = calculator.calculate(
                new CustomerOutcomeCalculator.OutcomeInput(null, null, null, SourceStatus.UNAVAILABLE, null));

        assertThat(stale.customerOutcomes().status()).isEqualTo(SourceStatus.STALE);
        assertThat(unavailable.customerOutcomes().status()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /** 명시적 NO_TRAFFIC에 결과가 있거나 windowEnd가 관측 시각 뒤면 계약 오류입니다. */
    @Test
    void rejectsNoTrafficWithOutcomesAndWindowAfterObservation() {
        assertThatThrownBy(() -> new CustomerOutcomeCalculator.OutcomeInput(
                END.minusSeconds(1), END, List.of(new CustomerOutcomeCalculator.OutcomeCount(
                        AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 1L)), SourceStatus.NO_TRAFFIC, END))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CustomerOutcomeCalculator.OutcomeInput(
                END.minusSeconds(1), END.plusSeconds(1), List.of(), SourceStatus.VALID, END))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 같은 유형 병합과 전체 합계는 long 범위를 넘으면 조용히 감싸면 안 됩니다. */
    @Test
    void rejectsDuplicateAndTotalOverflowAndPreservesWarmingUpZero() {
        CustomerOutcomeCalculator calculator = new CustomerOutcomeCalculator();
        assertThatThrownBy(() -> calculator.calculate(new CustomerOutcomeCalculator.OutcomeInput(
                END.minusSeconds(1), END, List.of(
                        new CustomerOutcomeCalculator.OutcomeCount(AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, Long.MAX_VALUE),
                        new CustomerOutcomeCalculator.OutcomeCount(AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 1L)),
                SourceStatus.VALID, END))).isInstanceOf(ArithmeticException.class);
        assertThat(calculator.calculate(new CustomerOutcomeCalculator.OutcomeInput(
                END.minusSeconds(1), END, List.of(), SourceStatus.WARMING_UP, END)).customerOutcomes().status())
                .isEqualTo(SourceStatus.WARMING_UP);
    }
}
