package com.kafkick.core.coupon.domain;

import java.time.LocalTime;
import java.util.Set;
import java.util.Collections;
import java.util.EnumSet;

public record CouponTemplate(
        Long id,
        Long brandId,
        String name,
        CouponPolicyType policyType,
        Integer discountRate,
        Integer maxDiscountAmount,
        Integer discountAmount,
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
        policyType = validatePolicy(
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
        validateEligibleGrades(eligibleGrades);

        name = name.trim();
        eligibleGrades = Collections.unmodifiableSet(
                EnumSet.copyOf(eligibleGrades)
        );
    }

    public static CouponTemplate create(
            Long brandId,
            String name,
            CouponPolicyType policyType,
            Integer discountRate,
            Integer maxDiscountAmount,
            Integer discountAmount,
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
            int validDays,
            int nthWeek,
            CouponDayOfWeek dayOfWeek,
            LocalTime startTime,
            int durationHours,
            int stockPerOccurrence,
            Set<MembershipGrade> eligibleGrades,
            boolean active
    ) {
        validateRestoredId(id);

        return new CouponTemplate(
                id,
                brandId,
                name,
                policyType,
                discountRate,
                maxDiscountAmount,
                discountAmount,
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

    public CouponTemplate update(
            Long brandId,
            String name,
            CouponPolicyType policyType,
            Integer discountRate,
            Integer maxDiscountAmount,
            Integer discountAmount,
            int validDays,
            int nthWeek,
            CouponDayOfWeek dayOfWeek,
            LocalTime startTime,
            int durationHours,
            int stockPerOccurrence,
            Set<MembershipGrade> eligibleGrades
    ) {
        return new CouponTemplate(
                id,
                brandId,
                name,
                policyType,
                discountRate,
                maxDiscountAmount,
                discountAmount,
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

    public CouponTemplate changeActivation(boolean active) {
        return new CouponTemplate(
                id,
                brandId,
                name,
                policyType,
                discountRate,
                maxDiscountAmount,
                discountAmount,
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

    private static CouponPolicyType validatePolicy(
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

        return switch (policyType) {
            case PERCENT_CAPPED -> {
                validatePercentPolicy(
                        discountRate,
                        maxDiscountAmount,
                        discountAmount
                );
                yield policyType;
            }

            case FIXED_AMOUNT -> {
                validateFixedAmountPolicy(
                        discountRate,
                        maxDiscountAmount,
                        discountAmount
                );
                yield policyType;
            }
        };
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

        if (startTime.getNano() != 0) {
            throw new IllegalArgumentException(
                    "쿠폰 시작 시간은 초 단위까지만 입력할 수 있습니다."
            );
        }

        if (durationHours < 1 || durationHours > 24) {
            throw new IllegalArgumentException(
                    "쿠폰 진행 시간은 1에서 24 사이여야 합니다."
            );
        }

        if (!startTime.plusHours(durationHours).isAfter(startTime)) {
            throw new IllegalArgumentException(
                    "쿠폰 진행 시간은 시작일 자정을 넘을 수 없습니다."
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

    private static void validateEligibleGrades(
            Set<MembershipGrade> eligibleGrades
    ) {
        if (eligibleGrades == null || eligibleGrades.isEmpty()) {
            throw new IllegalArgumentException(
                    "참여 가능한 멤버십 등급이 필요합니다."
            );
        }
    }

    private static void validateRestoredId(Long id) {
        if (id == null || id <= 0) {
            throw new IllegalArgumentException(
                    "복원할 쿠폰 템플릿 ID는 0보다 커야 합니다."
            );
        }
    }
}
