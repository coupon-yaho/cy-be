// 관리자 쿠폰 템플릿의 핵심 정책과 생성 규칙을 관리하는 도메인 모델입니다.
package com.kafkick.core.coupon;

import java.time.LocalTime;
import java.util.Set;

public record CouponTemplate(
        Long id,
        Long brandId,
        String name,
        CouponPolicyType policyType,
        Integer discountRate,
        Integer maxDiscountAmount,
        Integer discountAmount,
        int minOrderAmount,
        int validDays,
        int nthWeek,
        CouponDayOfWeek dayOfWeek,
        LocalTime startTime,
        int durationHours,
        int stockPerOccurrence,
        Set<MembershipGrade> eligibleGrades,
        boolean active
) {

    public CouponTemplate {
        validateId(id);
        validateBrandId(brandId);
        validateName(name);
        validatePolicy(
                policyType,
                discountRate,
                maxDiscountAmount,
                discountAmount
        );
        validateSchedule(
                validDays,
                nthWeek,
                dayOfWeek,
                startTime,
                durationHours
        );
        validateStock(stockPerOccurrence);
        validateMinOrderAmount(minOrderAmount);
        validateEligibleGrades(eligibleGrades);

        name = name.trim();
        eligibleGrades = Set.copyOf(eligibleGrades);
    }

    public static CouponTemplate create(
            Long brandId,
            String name,
            CouponPolicyType policyType,
            Integer discountRate,
            Integer maxDiscountAmount,
            Integer discountAmount,
            int minOrderAmount,
            int validDays,
            int nthWeek,
            CouponDayOfWeek dayOfWeek,
            LocalTime startTime,
            int durationHours,
            int stockPerOccurrence,
            Set<MembershipGrade> eligibleGrades
    ) {
        return new CouponTemplate(
                null,
                brandId,
                name,
                policyType,
                discountRate,
                maxDiscountAmount,
                discountAmount,
                minOrderAmount,
                validDays,
                nthWeek,
                dayOfWeek,
                startTime,
                durationHours,
                stockPerOccurrence,
                eligibleGrades,
                true
        );
    }

    public static CouponTemplate restore(
            Long id,
            Long brandId,
            String name,
            CouponPolicyType policyType,
            Integer discountRate,
            Integer maxDiscountAmount,
            Integer discountAmount,
            int minOrderAmount,
            int validDays,
            int nthWeek,
            CouponDayOfWeek dayOfWeek,
            LocalTime startTime,
            int durationHours,
            int stockPerOccurrence,
            Set<MembershipGrade> eligibleGrades,
            boolean active
    ) {
        return new CouponTemplate(
                id,
                brandId,
                name,
                policyType,
                discountRate,
                maxDiscountAmount,
                discountAmount,
                minOrderAmount,
                validDays,
                nthWeek,
                dayOfWeek,
                startTime,
                durationHours,
                stockPerOccurrence,
                eligibleGrades,
                active
        );
    }

    public int eligibleGradesMask() {
        return MembershipGrade.toMask(eligibleGrades);
    }

    private static void validateId(Long id) {
        if (id != null && id <= 0) {
            throw new IllegalArgumentException(
                    "쿠폰 ID는 0보다 커야 합니다."
            );
        }
    }

    private static void validateBrandId(Long brandId) {
        if (brandId == null || brandId <= 0) {
            throw new IllegalArgumentException(
                    "브랜드 ID는 0보다 커야 합니다."
            );
        }
    }

    private static void validateName(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException(
                    "쿠폰 이름은 필수입니다."
            );
        }

        if (name.trim().length() > 100) {
            throw new IllegalArgumentException(
                    "쿠폰 이름은 100자 이하여야 합니다."
            );
        }
    }

    private static void validatePolicy(
            CouponPolicyType policyType,
            Integer discountRate,
            Integer maxDiscountAmount,
            Integer discountAmount
    ) {
        if (policyType == null) {
            throw new IllegalArgumentException(
                    "할인 정책은 필수입니다."
            );
        }

        switch (policyType) {
            case PERCENT_CAPPED -> validatePercentPolicy(
                    discountRate,
                    maxDiscountAmount,
                    discountAmount
            );
            case FIXED_AMOUNT -> validateFixedAmountPolicy(
                    discountRate,
                    maxDiscountAmount,
                    discountAmount
            );
        }
    }

    private static void validatePercentPolicy(
            Integer discountRate,
            Integer maxDiscountAmount,
            Integer discountAmount
    ) {
        if (discountRate == null
                || discountRate < 1
                || discountRate > 100) {
            throw new IllegalArgumentException(
                    "퍼센트 할인율은 1에서 100 사이여야 합니다."
            );
        }

        if (maxDiscountAmount == null
                || maxDiscountAmount <= 0) {
            throw new IllegalArgumentException(
                    "퍼센트 할인은 최대 할인 금액이 필요합니다."
            );
        }

        if (discountAmount != null) {
            throw new IllegalArgumentException(
                    "퍼센트 할인에는 정액 할인 금액을 입력할 수 없습니다."
            );
        }
    }

    private static void validateFixedAmountPolicy(
            Integer discountRate,
            Integer maxDiscountAmount,
            Integer discountAmount
    ) {
        if (discountAmount == null || discountAmount <= 0) {
            throw new IllegalArgumentException(
                    "정액 할인 금액은 0보다 커야 합니다."
            );
        }

        if (discountRate != null || maxDiscountAmount != null) {
            throw new IllegalArgumentException(
                    "정액 할인에는 할인율이나 최대 할인 금액을 입력할 수 없습니다."
            );
        }
    }

    private static void validateSchedule(
            int validDays,
            int nthWeek,
            CouponDayOfWeek dayOfWeek,
            LocalTime startTime,
            int durationHours
    ) {
        if (validDays <= 0) {
            throw new IllegalArgumentException(
                    "쿠폰 유효기간은 0보다 커야 합니다."
            );
        }

        if (nthWeek < 1 || nthWeek > 4) {
            throw new IllegalArgumentException(
                    "쿠폰 발행 주차는 1에서 4 사이여야 합니다."
            );
        }

        if (dayOfWeek == null) {
            throw new IllegalArgumentException(
                    "쿠폰 발행 요일은 필수입니다."
            );
        }

        if (startTime == null) {
            throw new IllegalArgumentException(
                    "쿠폰 시작 시간은 필수입니다."
            );
        }

        if (durationHours <= 0) {
            throw new IllegalArgumentException(
                    "쿠폰 진행 시간은 0보다 커야 합니다."
            );
        }
    }

    private static void validateStock(int stockPerOccurrence) {
        if (stockPerOccurrence <= 0) {
            throw new IllegalArgumentException(
                    "쿠폰 재고는 0보다 커야 합니다."
            );
        }
    }

    private static void validateMinOrderAmount(
            int minOrderAmount
    ) {
        if (minOrderAmount < 0) {
            throw new IllegalArgumentException(
                    "최소 주문 금액은 0 이상이어야 합니다."
            );
        }
    }

    private static void validateEligibleGrades(
            Set<MembershipGrade> eligibleGrades
    ) {
        if (eligibleGrades == null || eligibleGrades.isEmpty()) {
            throw new IllegalArgumentException(
                    "참여 가능한 멤버십 등급이 필요합니다."
            );
        }
    }
}