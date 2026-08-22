package com.kafkick.core.admin.overview.calculator;

import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Objects;

import org.springframework.stereotype.Component;

import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.observation.SourceStatus;

/** 이미 분류된 O3 고객 결과를 합산해 고정 순서의 count·ratio로 만드는 순수 계산기입니다. */
@Component
public class CustomerOutcomeCalculator {

    /** 상태 없는 O3 순수 계산기를 생성합니다. */
    public CustomerOutcomeCalculator() { }

    /**
     * 같은 유형 count를 먼저 합산하고 전체 분모로 비율을 계산합니다.
     *
     * <p>정상 원천의 총합 0은 실제 무트래픽이므로 NO_TRAFFIC과 빈 목록으로 표현합니다. 미수집은
     * 0건으로 바꾸지 않고 상태와 null 값을 전파하며 ReasonCode 매핑은 이 계산기의 책임이 아닙니다.</p>
     *
     * @param input 관측 구간·이미 분류된 유형별 count·원천 상태
     * @return O3 고객 결과 관측값
     */
    public OutcomeCalculation calculate(OutcomeInput input) {
        Objects.requireNonNull(input, "input");
        // 미수집·수집 전 상태를 고객 결과 0건으로 오인하지 않도록 null 값을 유지합니다.
        if (!input.sourceStatus().carriesValue()) {
            return new OutcomeCalculation(new AdminOverviewSnapshot.Observation<>(
                    null, input.sourceStatus(), null));
        }
        if (!input.windowEnd().isAfter(input.windowStart())) {
            throw new IllegalArgumentException("관측 구간은 양수여야 합니다.");
        }
        EnumMap<AdminOverviewSnapshot.CustomerOutcomeType, Long> counts =
                new EnumMap<>(AdminOverviewSnapshot.CustomerOutcomeType.class);
        for (OutcomeCount count : input.counts()) {
            Objects.requireNonNull(count, "counts에는 null을 포함할 수 없습니다.");
            // 같은 결과 유형이 여러 원천에서 들어와도 화면에는 유형별 한 행으로 합산합니다.
            counts.merge(count.type(), count.count(), Math::addExact);
        }
        long total = counts.values().stream().mapToLong(Long::longValue).reduce(0L, Math::addExact);
        if (total == 0L) {
            // 정상 수집 결과가 실제 0건일 때만 빈 결과와 NO_TRAFFIC을 함께 반환합니다.
            SourceStatus status = input.sourceStatus() == SourceStatus.VALID
                    ? SourceStatus.NO_TRAFFIC : input.sourceStatus();
            return new OutcomeCalculation(new AdminOverviewSnapshot.Observation<>(
                    new AdminOverviewSnapshot.CustomerOutcomeSummary(
                            input.windowStart(), input.windowEnd(), 0L, List.of()),
                    status, input.observedAt()));
        }
        // enum 선언 순서로 모든 유형을 출력해 입력 순서와 무관한 고정 응답을 만듭니다.
        List<AdminOverviewSnapshot.CustomerOutcome> outcomes =
                java.util.Arrays.stream(AdminOverviewSnapshot.CustomerOutcomeType.values())
                        .map(type -> new AdminOverviewSnapshot.CustomerOutcome(type,
                                counts.getOrDefault(type, 0L),
                                counts.getOrDefault(type, 0L) / (double) total, null))
                        .toList();
        return new OutcomeCalculation(new AdminOverviewSnapshot.Observation<>(
                new AdminOverviewSnapshot.CustomerOutcomeSummary(
                        input.windowStart(), input.windowEnd(), total, outcomes),
                input.sourceStatus(), input.observedAt()));
    }

    /**
     * O3 관측 입력입니다.
     *
     * @param windowStart 결과 count의 관측 구간 시작 시각
     * @param windowEnd 결과 count의 관측 구간 종료 시각
     * @param counts 이미 CustomerOutcomeType으로 분류된 count 목록; 동일 type은 합산됨
     * @param sourceStatus 원천 상태
     * @param observedAt 값이 있는 상태의 실제 관측 시각
     */
    public record OutcomeInput(Instant windowStart, Instant windowEnd, List<OutcomeCount> counts,
                               SourceStatus sourceStatus, Instant observedAt) {
        /** 필수 시간·목록·상태와 값 있는 상태의 관측 시각을 검증합니다. */
        public OutcomeInput {
            Objects.requireNonNull(sourceStatus, "sourceStatus");
            if (sourceStatus.carriesValue() != (observedAt != null)) {
                throw new IllegalArgumentException("원천 상태와 observedAt 조합이 맞지 않습니다.");
            }
            if (sourceStatus.carriesValue()) {
                Objects.requireNonNull(windowStart, "windowStart");
                Objects.requireNonNull(windowEnd, "windowEnd");
                Objects.requireNonNull(counts, "counts");
                if (!windowEnd.isAfter(windowStart) || windowEnd.isAfter(observedAt)) {
                    throw new IllegalArgumentException("관측 구간은 양수이고 observedAt을 넘을 수 없습니다.");
                }
                if (sourceStatus == SourceStatus.NO_TRAFFIC
                        && counts.stream().mapToLong(OutcomeCount::count).anyMatch(count -> count != 0L)) {
                    throw new IllegalArgumentException("NO_TRAFFIC 결과 count는 0이어야 합니다.");
                }
                counts = List.copyOf(counts);
            }
        }
    }

    /**
     * 이미 분류된 O3 결과 한 유형의 count입니다.
     *
     * @param type 화면 계약의 고객 결과 유형
     * @param count 실제 결과 count; 0은 해당 유형이 발생하지 않음
     */
    public record OutcomeCount(AdminOverviewSnapshot.CustomerOutcomeType type, long count) {
        /** 유형 null과 음수 count를 거부합니다. */
        public OutcomeCount {
            Objects.requireNonNull(type, "type");
            if (count < 0L) {
                throw new IllegalArgumentException("count는 음수일 수 없습니다.");
            }
        }
    }

    /**
     * O3 계산 결과입니다.
     *
     * @param customerOutcomes 전체 count·ratio·고정 순서를 보존한 관측값
     */
    public record OutcomeCalculation(
            AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.CustomerOutcomeSummary> customerOutcomes) {
        /** 결과 관측값이 null이 아님을 보장합니다. */
        public OutcomeCalculation {
            Objects.requireNonNull(customerOutcomes, "customerOutcomes");
        }
    }
}
