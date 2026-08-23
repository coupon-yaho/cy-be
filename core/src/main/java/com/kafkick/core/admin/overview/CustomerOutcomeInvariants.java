package com.kafkick.core.admin.overview;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

/** Core와 HTTP O3 요약이 공유하는 집계 구간·합계·비율 불변식을 검증합니다. */
public final class CustomerOutcomeInvariants {

    /** 인스턴스화를 막습니다. */
    private CustomerOutcomeInvariants() { }

    /**
     * 표현 타입과 무관하게 O3 고객 결과 요약의 동일한 업무 규칙을 검증합니다.
     *
     * @param windowStart 집계 시작 시각
     * @param windowEnd 집계 종료 시각
     * @param totalCount 결과 전체 합계
     * @param outcomes Core 또는 HTTP 결과 목록
     * @param typeExtractor 중복을 판정할 결과 유형 추출기
     * @param countExtractor 결과 건수 추출기
     * @param ratioExtractor 전체 대비 비율 추출기
     * @param <T> 결과 표현 타입
     * @param <K> 결과 유형 키 타입
     */
    public static <T, K> void validate(
            Instant windowStart,
            Instant windowEnd,
            double totalCount,
            List<T> outcomes,
            Function<? super T, ? extends K> typeExtractor,
            ToDoubleFunction<? super T> countExtractor,
            ToDoubleFunction<? super T> ratioExtractor
    ) {
        Objects.requireNonNull(windowStart, "windowStart");
        Objects.requireNonNull(windowEnd, "windowEnd");
        Objects.requireNonNull(outcomes, "outcomes");
        Objects.requireNonNull(typeExtractor, "typeExtractor");
        Objects.requireNonNull(countExtractor, "countExtractor");
        Objects.requireNonNull(ratioExtractor, "ratioExtractor");
        if (!windowEnd.isAfter(windowStart)) {
            throw new IllegalArgumentException("O3 집계 구간은 양수여야 합니다.");
        }
        if (!Double.isFinite(totalCount) || totalCount < 0d) {
            throw new IllegalArgumentException("totalCount는 유한한 비음수여야 합니다.");
        }
        if ((totalCount == 0d) != outcomes.isEmpty()) {
            throw new IllegalArgumentException("totalCount가 0일 때만 outcomes가 비어야 합니다.");
        }
        if (totalCount == 0d) {
            return;
        }

        Set<K> types = new HashSet<>();
        double sum = 0d;
        for (T outcome : outcomes) {
            T requiredOutcome = Objects.requireNonNull(outcome, "outcome");
            K type = Objects.requireNonNull(typeExtractor.apply(requiredOutcome), "outcome.type");
            if (!types.add(type)) {
                throw new IllegalArgumentException("O3 outcome type은 중복될 수 없습니다.");
            }
            double count = countExtractor.applyAsDouble(requiredOutcome);
            sum += count;
            if (!Double.isFinite(sum)) {
                throw new IllegalArgumentException("O3 outcome count 합계는 유한해야 합니다.");
            }
            if (!equalWithinAccumulationUlps(
                    ratioExtractor.applyAsDouble(requiredOutcome), count / totalCount, 1)) {
                throw new IllegalArgumentException("O3 outcome ratio가 count/totalCount와 맞지 않습니다.");
            }
        }
        if (!equalWithinAccumulationUlps(sum, totalCount, outcomes.size())) {
            throw new IllegalArgumentException("O3 outcome count 합이 totalCount와 맞지 않습니다.");
        }
    }

    /** 각 합산 항마다 최대 1 ULP의 반올림 차이만 허용합니다. */
    private static boolean equalWithinAccumulationUlps(double left, double right, int termCount) {
        if (left == right) {
            return true;
        }
        double tolerance = Math.max(Math.ulp(left), Math.ulp(right)) * termCount;
        return Math.abs(left - right) <= tolerance;
    }
}
