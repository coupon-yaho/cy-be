// 템플릿 생성 시점의 정책을 고정해 보관하는 단일 쿠폰 회차 도메인입니다.
package com.kafkick.core.coupon.domain;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

public record CouponRound(
        Long id,
        Long templateId,
        Long brandId,
        String name,
        CouponPolicyType policyType,
        Integer discountRate,
        Integer maxDiscountAmount,
        Integer discountAmount,
        int validDays,
        Set<MembershipGrade> eligibleGrades,
        Instant openAt,
        Instant closeAt,
        CouponRoundStatus status,
        Instant createdAt
) {

    public CouponRound {
        validateId(id);
        validatePositiveId(templateId, "쿠폰 템플릿 ID");
        validatePositiveId(brandId, "브랜드 ID");
        validateName(name);
        policyType = validatePolicy(
                policyType,
                discountRate,
                maxDiscountAmount,
                discountAmount
        );
        if (validDays <= 0) {
            throw new IllegalArgumentException(
                    "쿠폰 유효기간은 0보다 커야 합니다."
            );
        }
        if (eligibleGrades == null || eligibleGrades.isEmpty()) {
            throw new IllegalArgumentException(
                    "참여 가능한 멤버십 등급이 필요합니다."
            );
        }
        if (openAt == null || closeAt == null || !closeAt.isAfter(openAt)) {
            throw new IllegalArgumentException(
                    "쿠폰 회차 종료 시각은 시작 시각보다 늦어야 합니다."
            );
        }
        if (status == null) {
            throw new IllegalArgumentException(
                    "쿠폰 회차 상태는 필수입니다."
            );
        }
        if (createdAt == null) {
            throw new IllegalArgumentException(
                    "쿠폰 회차 생성 시각은 필수입니다."
            );
        }

        name = name.trim();
        eligibleGrades = Collections.unmodifiableSet(
                EnumSet.copyOf(eligibleGrades)
        );
    }

    public static CouponRound schedule(
            CouponTemplate template,
            Instant openAt,
            Instant createdAt
    ) {
        if (template == null || template.id() == null) {
            throw new IllegalArgumentException(
                    "저장된 쿠폰 템플릿이 필요합니다."
            );
        }
        if (!template.active()) {
            throw new IllegalArgumentException(
                    "비활성 쿠폰 템플릿으로 회차를 만들 수 없습니다."
            );
        }

        return new CouponRound(
                null,
                template.id(),
                template.brandId(),
                template.name(),
                template.policyType(),
                template.discountRate(),
                template.maxDiscountAmount(),
                template.discountAmount(),
                template.validDays(),
                template.eligibleGrades(),
                openAt,
                openAt.plus(template.durationHours(), ChronoUnit.HOURS),
                CouponRoundStatus.SCHEDULED,
                createdAt
        );
    }

    public static CouponRound restore(
            Long id,
            Long templateId,
            Long brandId,
            String name,
            CouponPolicyType policyType,
            Integer discountRate,
            Integer maxDiscountAmount,
            Integer discountAmount,
            int validDays,
            Set<MembershipGrade> eligibleGrades,
            Instant openAt,
            Instant closeAt,
            CouponRoundStatus status,
            Instant createdAt
    ) {
        if (id == null || id <= 0) {
            throw new IllegalArgumentException(
                    "복원할 쿠폰 회차 ID는 0보다 커야 합니다."
            );
        }

        return new CouponRound(
                id,
                templateId,
                brandId,
                name,
                policyType,
                discountRate,
                maxDiscountAmount,
                discountAmount,
                validDays,
                eligibleGrades,
                openAt,
                closeAt,
                status,
                createdAt
        );
    }

    public int eligibleGradesMask() {
        return MembershipGrade.toMask(eligibleGrades);
    }

    private static void validateId(Long id) {
        if (id != null && id <= 0) {
            throw new IllegalArgumentException(
                    "쿠폰 회차 ID는 0보다 커야 합니다."
            );
        }
    }

    private static void validatePositiveId(Long id, String fieldName) {
        if (id == null || id <= 0) {
            throw new IllegalArgumentException(
                    fieldName + "는 0보다 커야 합니다."
            );
        }
    }

    private static void validateName(String name) {
        if (name == null || name.isBlank() || name.trim().length() > 100) {
            throw new IllegalArgumentException(
                    "쿠폰 회차 이름은 1자 이상 100자 이하여야 합니다."
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
            throw new IllegalArgumentException("할인 정책은 필수입니다.");
        }

        return switch (policyType) {
            case PERCENT_CAPPED -> {
                if (discountRate == null
                        || discountRate < 1
                        || discountRate > 100
                        || maxDiscountAmount == null
                        || maxDiscountAmount <= 0
                        || discountAmount != null) {
                    throw new IllegalArgumentException(
                            "정률 할인 스냅샷 값이 올바르지 않습니다."
                    );
                }
                yield policyType;
            }
            case FIXED_AMOUNT -> {
                if (discountAmount == null
                        || discountAmount <= 0
                        || discountRate != null
                        || maxDiscountAmount != null) {
                    throw new IllegalArgumentException(
                            "정액 할인 스냅샷 값이 올바르지 않습니다."
                    );
                }
                yield policyType;
            }
        };
    }
}
