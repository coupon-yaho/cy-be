package com.kafkick.core.admin.overview.calculator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.observation.SourceStatus;

/** O3 이미 분류된 결과의 합산·고정 순서·0건 표현 규칙을 검증합니다. */
class CustomerOutcomeCalculatorTest {

    private static final Instant END = Instant.parse("2026-08-22T03:00:00Z");

    /** 같은 유형의 estimated fraction을 합산하고 7종은 입력 순서와 무관하게 고정합니다. */
    @Test
    void aggregatesDuplicatesAndReturnsAllTypesInFixedOrder() {
        CustomerOutcomeCalculator.OutcomeCalculation result = new CustomerOutcomeCalculator().calculate(
                new CustomerOutcomeCalculator.OutcomeInput(
                        END.minus(Duration.ofMinutes(5)), END,
                        List.of(new CustomerOutcomeCalculator.OutcomeCount(
                                        AdminOverviewSnapshot.CustomerOutcomeType.SYSTEM_FAILURE, 0.25d),
                                new CustomerOutcomeCalculator.OutcomeCount(
                                        AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 0.1d),
                                new CustomerOutcomeCalculator.OutcomeCount(
                                        AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 0.2d)),
                        SourceStatus.VALID, END));

        AdminOverviewSnapshot.CustomerOutcomeSummary summary = result.customerOutcomes().value();
        assertThat(summary.totalCount()).isEqualTo(0.55d);
        assertThat(summary.outcomes()).extracting(AdminOverviewSnapshot.CustomerOutcome::type)
                .containsExactly(AdminOverviewSnapshot.CustomerOutcomeType.values());
        assertThat(summary.outcomes().getFirst()).isEqualTo(new AdminOverviewSnapshot.CustomerOutcome(
                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED,
                0.30000000000000004d, 0.5454545454545455d, null));
    }

    /** 양의 fractional increase는 0으로 축소되지 않고 실제 활동과 100% 비율을 만듭니다. */
    @Test
    void preservesPositiveFractionAsTrafficWithoutFalseZero() {
        CustomerOutcomeCalculator.OutcomeCalculation result = new CustomerOutcomeCalculator().calculate(
                new CustomerOutcomeCalculator.OutcomeInput(
                        END.minus(Duration.ofMinutes(5)), END,
                        List.of(new CustomerOutcomeCalculator.OutcomeCount(
                                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 0.1d)),
                        SourceStatus.VALID, END));

        AdminOverviewSnapshot.CustomerOutcomeSummary summary = result.customerOutcomes().value();
        assertThat(result.customerOutcomes().status()).isEqualTo(SourceStatus.VALID);
        assertThat(summary.totalCount()).isEqualTo(0.1d);
        assertThat(summary.outcomes().getFirst().count()).isEqualTo(0.1d);
        assertThat(summary.outcomes().getFirst().ratio()).isOne();
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

    /** 음수·비유한 count와 역전된 관측 구간을 묵시적으로 보정하지 않아야 합니다. */
    @Test
    void rejectsNegativeCountAndInvalidWindow() {
        CustomerOutcomeCalculator calculator = new CustomerOutcomeCalculator();
        assertThatThrownBy(() -> calculator.calculate(new CustomerOutcomeCalculator.OutcomeInput(
                END, END.minusSeconds(1), List.of(), SourceStatus.VALID, END)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> calculator.calculate(new CustomerOutcomeCalculator.OutcomeInput(
                END.minusSeconds(1), END,
                List.of(new CustomerOutcomeCalculator.OutcomeCount(
                        AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, -0.1d)), SourceStatus.VALID, END)))
                .isInstanceOf(IllegalArgumentException.class);
        for (double invalid : List.of(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)) {
            assertThatThrownBy(() -> new CustomerOutcomeCalculator.OutcomeCount(
                    AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, invalid))
                    .isInstanceOf(IllegalArgumentException.class);
        }
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

    /** 명시적 NO_TRAFFIC에 실제 결과 count가 있으면 계약 오류입니다. */
    @Test
    void rejectsNoTrafficWithOutcomes() {
        assertThatThrownBy(() -> new CustomerOutcomeCalculator.OutcomeInput(
                END.minusSeconds(1), END, List.of(new CustomerOutcomeCalculator.OutcomeCount(
                        AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 1L)), SourceStatus.NO_TRAFFIC, END))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 합산 overflow는 공개 Infinity가 될 수 없고 원본 목록 변경도 입력을 바꿀 수 없습니다. */
    @Test
    void rejectsNonFiniteSumsCopiesInputAndPreservesWarmingUpZero() {
        CustomerOutcomeCalculator calculator = new CustomerOutcomeCalculator();
        assertThatThrownBy(() -> calculator.calculate(new CustomerOutcomeCalculator.OutcomeInput(
                END.minusSeconds(1), END, List.of(
                        new CustomerOutcomeCalculator.OutcomeCount(
                                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, Double.MAX_VALUE),
                        new CustomerOutcomeCalculator.OutcomeCount(
                                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, Double.MAX_VALUE)),
                SourceStatus.VALID, END))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> calculator.calculate(new CustomerOutcomeCalculator.OutcomeInput(
                END.minusSeconds(1), END, List.of(
                        new CustomerOutcomeCalculator.OutcomeCount(
                                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, Double.MAX_VALUE),
                        new CustomerOutcomeCalculator.OutcomeCount(
                                AdminOverviewSnapshot.CustomerOutcomeType.QUEUED, Double.MAX_VALUE)),
                SourceStatus.VALID, END))).isInstanceOf(IllegalArgumentException.class);
        List<CustomerOutcomeCalculator.OutcomeCount> mutable = new ArrayList<>(List.of(
                new CustomerOutcomeCalculator.OutcomeCount(
                        AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 0.1d)));
        CustomerOutcomeCalculator.OutcomeInput copied = new CustomerOutcomeCalculator.OutcomeInput(
                END.minusSeconds(1), END, mutable, SourceStatus.VALID, END);
        mutable.clear();

        assertThat(copied.counts()).hasSize(1);
        assertThatThrownBy(() -> copied.counts().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThat(calculator.calculate(new CustomerOutcomeCalculator.OutcomeInput(
                END.minusSeconds(1), END, List.of(), SourceStatus.WARMING_UP, END)).customerOutcomes().status())
                .isEqualTo(SourceStatus.WARMING_UP);
    }
}
